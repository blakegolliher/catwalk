"""Rollup job backpressure, state transitions, cancellation, and shutdown."""

import threading
import time

import pytest

from catwalk.jobs import JobManager, JobQueueFullError


def test_queue_is_bounded_and_queued_job_can_be_cancelled():
    gate = threading.Event()
    started = threading.Event()
    manager = JobManager(max_workers=1, max_queue=1)

    def blocking(_progress):
        started.set()
        gate.wait(5)
        return {"ok": True}

    first, _ = manager.submit(("first",), blocking)
    assert started.wait(1)
    second, _ = manager.submit(("second",), lambda _progress: {"ok": True})
    assert second.status == "queued"
    with pytest.raises(JobQueueFullError):
        manager.submit(("third",), lambda _progress: {})

    manager.cancel(second.id)
    assert second.event.wait(1)
    assert second.status == "cancelled"
    gate.set()
    assert first.event.wait(1)
    manager.close()


def test_running_job_cancels_through_progress_callback():
    started = threading.Event()
    manager = JobManager(max_workers=1, max_queue=0)

    def work(progress):
        started.set()
        rows = 0
        while True:
            rows += 1
            progress(rows)
            time.sleep(0.001)

    job, _ = manager.submit(("running",), work)
    assert started.wait(1)
    manager.cancel(job.id)
    assert job.event.wait(1)
    assert job.status == "cancelled"
    manager.close()


def test_active_jobs_are_deduplicated():
    gate = threading.Event()
    manager = JobManager(max_workers=1, max_queue=0)
    first, w1 = manager.submit(("same",), lambda _progress: gate.wait(1))
    second, w2 = manager.submit(("same",), lambda _progress: None)
    assert second is first
    assert w1 != w2
    gate.set()
    assert first.event.wait(1)
    manager.close()


def test_shared_job_survives_until_last_watcher_detaches():
    gate = threading.Event()
    started = threading.Event()
    manager = JobManager(max_workers=1, max_queue=0)

    def work(progress):
        started.set()
        while not gate.wait(0.001):
            progress(1)
        return {"ok": True}

    job, w1 = manager.submit(("shared",), work)
    same, w2 = manager.submit(("shared",), lambda _progress: None)
    assert same is job
    assert started.wait(1)

    # First client leaves: the job must keep running for the second one.
    manager.cancel(job.id, watcher=w1)
    assert not job.cancel_requested.is_set()
    assert job.status == "running"

    # Last client leaves: now the job really cancels.
    manager.cancel(job.id, watcher=w2)
    assert job.event.wait(1)
    assert job.status == "cancelled"
    manager.close()


def test_submit_does_not_join_a_job_whose_cancel_is_in_flight():
    started = threading.Event()
    release = threading.Event()
    manager = JobManager(max_workers=2, max_queue=2)

    def slow_doomed(_progress):
        started.set()
        release.wait(5)
        return {"doomed": True}

    doomed, w = manager.submit(("key",), slow_doomed)
    assert started.wait(1)
    manager.cancel(doomed.id, watcher=w)
    assert doomed.cancel_requested.is_set()

    # cancel_requested is set but the worker hasn't flipped status yet:
    # a new client must get a fresh computation, not the doomed job.
    fresh, _ = manager.submit(("key",), lambda _progress: {"fresh": True})
    assert fresh is not doomed
    release.set()
    assert fresh.event.wait(1)
    assert fresh.status == "done"
    assert fresh.result == {"fresh": True}
    manager.close()


def test_close_force_cancels_despite_watchers():
    gate = threading.Event()
    started = threading.Event()
    manager = JobManager(max_workers=1, max_queue=0)

    def work(progress):
        started.set()
        while not gate.wait(0.001):
            progress(1)
        return {"ok": True}

    job, _ = manager.submit(("open",), work)
    manager.submit(("open",), lambda _progress: None)  # second watcher
    assert started.wait(1)
    manager.close()
    assert job.event.wait(1)
    assert job.status == "cancelled"
