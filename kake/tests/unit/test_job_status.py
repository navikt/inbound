from multiprocessing import Manager
from time import sleep
from unittest import TestCase

from kake.core.job_management import JobClerk, WorkerManager


def dummy_job():
    pass


def dummy_job_result():
    return {"awsm": "22k"}


def long_running_job():
    sleep(5)


def failing_job():
    raise Exception("feil")


class RunOnceWorkerManager(WorkerManager):
    def run(self):
        worker, job = self._assign_work()
        self._wait_for_worker_to_finnish(worker, job)


class NotWaitForWorkerManager(WorkerManager):
    def run(self):
        worker, job = self._assign_work()


class TestJobStatus(TestCase):
    def test_job_status_is_in_queue_when_adding_new_job(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job)
        assert job_status.status == "in_queue"

    def test_job_status_is_running_when_job_has_started(self):
        clerk = JobClerk(worker_manager=NotWaitForWorkerManager)
        job_status = clerk.run_job(long_running_job)
        clerk.worker_manager_process.run()
        new_status = clerk.get_job_status(job_status.job_id)
        assert new_status.status == "running"

    def test_job_status_is_done_after_job_is_done(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert new_status.status == "done"

    def test_job_status_is_error_if_job_fails(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(failing_job)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert new_status.status == "error"

    def test_job_result_is_error_message_if_job_fails(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(failing_job)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert new_status.job_result == {"error_message": "feil"}

    def test_job_status_created_at_and_updated_at_is_equal_when_adding_new_job(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job)
        assert job_status.created_at == job_status.updated_at

    def test_job_status_updated_at_is_updated_when_status_changes(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert job_status.updated_at < new_status.updated_at

    def test_job_status_created_at_is_not_updated_when_status_changes(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert job_status.created_at == new_status.created_at

    def test_job_status_job_result_is_updated_when_job_is_finnished(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_status = clerk.run_job(dummy_job_result)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        assert new_status.job_result == dummy_job_result()

    def test_job_status_id(self):
        clerk = JobClerk(worker_manager=RunOnceWorkerManager)
        job_id = "bla"
        job_status = clerk.run_job(dummy_job_result, job_id=job_id)
        clerk.worker_manager_process.join()
        new_status = clerk.get_job_status(job_status.job_id)
        result = new_status.job_id
        expected = job_id
        assert result == expected
