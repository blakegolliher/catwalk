"""Rollup job backpressure, state transitions, cancellation, and shutdown."""

import threading
import time

import pytest

from catwalk.jobs import JobManager, JobQueueFull


def test_queue_is_bounded_and_queued_job_can_be_cancelled():
    gate = threading.Event()
    started = threading.Event()
    manager = JobManager(max_workers=1, max_queue=1)

    def blocking(_progress):
        started.set()
        gate.wait(5)
        return {"ok": True}

    first = manager.submit(("first",), blocking)
    assert started.wait(1)
    second = manager.submit(("second",), lambda _progress: {"ok": True})
    assert second.status == "queued"
    with pytest.raises(JobQueueFull):
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

    job = manager.submit(("running",), work)
    assert started.wait(1)
    manager.cancel(job.id)
    assert job.event.wait(1)
    assert job.status == "cancelled"
    manager.close()


def test_active_jobs_are_deduplicated():
    gate = threading.Event()
    manager = JobManager(max_workers=1, max_queue=0)
    first = manager.submit(("same",), lambda _progress: gate.wait(1))
    assert manager.submit(("same",), lambda _progress: None) is first
    gate.set()
    assert first.event.wait(1)
    manager.close()
