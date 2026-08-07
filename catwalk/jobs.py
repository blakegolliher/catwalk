"""Bounded, cancellable background rollup jobs with progress reporting."""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field

_DONE_RETENTION_SECS = 300.0


class JobQueueFull(RuntimeError):
    """Raised instead of allowing an unbounded executor backlog."""


class JobCancelled(RuntimeError):
    status_code = 409


@dataclass
class Job:
    id: str
    key: tuple
    status: str = "queued"  # queued | running | done | error | cancelled
    rows_scanned: int = 0
    submitted: float = field(default_factory=time.monotonic)
    started: float | None = None
    finished: float | None = None
    result: dict | None = None
    error: str | None = None
    error_status: int = 502
    event: threading.Event = field(default_factory=threading.Event, repr=False)
    cancel_requested: threading.Event = field(default_factory=threading.Event, repr=False)
    future: Future | None = field(default=None, repr=False)
    watchers: set[str] = field(default_factory=set, repr=False)

    def snapshot(self) -> dict:
        now = self.finished or time.monotonic()
        running_s = now - self.started if self.started is not None else 0.0
        queued_s = (self.started or now) - self.submitted
        return {
            "job_id": self.id,
            "status": self.status,
            "rows_scanned": self.rows_scanned,
            "queued_s": round(queued_s, 1),
            "elapsed_s": round(running_s, 1),
            "error": self.error,
        }


class JobManager:
    def __init__(self, max_workers: int = 2, max_queue: int = 16):
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="rollup")
        self._jobs: dict[str, Job] = {}
        self._by_key: dict[tuple, Job] = {}
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(max_workers + max_queue)
        self._closed = False

    def submit(self, key: tuple, fn) -> tuple[Job, str]:
        """Start ``fn(progress_cb)`` or join an active job with the same key.

        Returns ``(job, watcher)``. Deduplicated jobs are shared between
        clients, so each caller gets its own watcher token; the job is only
        cancelled once every watcher has detached via :meth:`cancel`.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("job manager is closed")
            self._prune_locked()
            watcher = uuid.uuid4().hex[:12]
            existing = self._by_key.get(key)
            if (
                existing
                and existing.status in ("queued", "running")
                # A doomed job (last watcher just detached) is not joinable:
                # fall through and start a fresh computation instead.
                and not existing.cancel_requested.is_set()
            ):
                existing.watchers.add(watcher)
                return existing, watcher
            if not self._slots.acquire(blocking=False):
                raise JobQueueFull("rollup queue is full; retry after active scans finish")

            job = Job(id=uuid.uuid4().hex[:12], key=key, watchers={watcher})
            self._jobs[job.id] = job
            self._by_key[key] = job

            def progress(rows):
                if job.cancel_requested.is_set():
                    raise JobCancelled("rollup cancelled")
                job.rows_scanned = rows

            def run():
                job.started = time.monotonic()
                job.status = "running"
                final_status = "done"
                try:
                    if job.cancel_requested.is_set():
                        raise JobCancelled("rollup cancelled")
                    result = fn(progress)
                    if job.cancel_requested.is_set():
                        raise JobCancelled("rollup cancelled")
                    job.result = result
                except JobCancelled as e:
                    job.error = str(e)
                    job.error_status = e.status_code
                    final_status = "cancelled"
                except Exception as e:
                    job.error = str(e)
                    job.error_status = getattr(e, "status_code", 502)
                    final_status = "error"
                finally:
                    job.finished = time.monotonic()
                    job.status = final_status
                    job.event.set()

            try:
                future = self.executor.submit(run)
            except Exception:
                self._jobs.pop(job.id, None)
                self._by_key.pop(key, None)
                self._slots.release()
                raise
            job.future = future
            future.add_done_callback(lambda _future: self._future_done(job))
            return job, watcher

    def _future_done(self, job: Job):
        if job.future is not None and job.future.cancelled() and not job.event.is_set():
            job.error = "rollup cancelled"
            job.error_status = 409
            job.finished = time.monotonic()
            job.status = "cancelled"
            job.event.set()
        self._slots.release()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            self._prune_locked()
            return self._jobs.get(job_id)

    def cancel(self, job_id: str, watcher: str | None = None) -> Job | None:
        """Detach ``watcher`` from a job; cancel it once no watchers remain.

        ``watcher=None`` force-cancels regardless of other watchers (used by
        :meth:`close`); HTTP callers must always pass their own token so one
        client leaving cannot kill a job other clients still poll.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if job.status not in ("queued", "running"):
                return job
            if watcher is not None:
                job.watchers.discard(watcher)
                if job.watchers:
                    return job
            job.cancel_requested.set()
            future = job.future
        if future is not None:
            future.cancel()
        return job

    def wait(self, job: Job, timeout: float) -> bool:
        """Block up to timeout for completion; True if the job finished."""
        return job.event.wait(timeout)

    def close(self, wait: bool = True):
        with self._lock:
            self._closed = True
            active = [job for job in self._jobs.values() if job.status in ("queued", "running")]
        for job in active:
            self.cancel(job.id)
        self.executor.shutdown(wait=wait, cancel_futures=True)

    def _prune_locked(self):
        now = time.monotonic()
        stale = [
            j
            for j in self._jobs.values()
            if j.finished is not None and now - j.finished > _DONE_RETENTION_SECS
        ]
        for job in stale:
            self._jobs.pop(job.id, None)
            if self._by_key.get(job.key) is job:
                self._by_key.pop(job.key, None)
