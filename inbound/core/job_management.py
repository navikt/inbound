import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Manager, Process, Queue
from typing import Callable, Optional, Type


@dataclass
class JobStatus:
    job_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    job_result: Optional[dict] = None


@dataclass
class Job:
    id: str
    job: Callable[[], Optional[dict]]

    def run(self, job_statuses: dict[str, JobStatus]):
        secrets.set_env_variables_from_secrets()
        try:
            job_result = self.job()
            status = "done"
        except Exception as e:
            job_result = {"error_message": str(e)}
            status = "error"
        job_status = job_statuses.get(self.id)
        job_status.job_result = job_result
        job_status.status = status
        job_status.updated_at = datetime.now()
        job_statuses[self.id] = job_status


class WorkerManager(Process):
    def __init__(self, job_queue: Queue, job_statuses: dict[str, JobStatus]):
        super(WorkerManager, self).__init__()
        self.job_queue = job_queue
        self.job_statuses = job_statuses

    def run(self):
        while True:
            worker, job = self._assign_work()
            self._wait_for_worker_to_finnish(worker, job)

    def _assign_work(self):
        job: Job = self.job_queue.get()
        worker_process = Process(
            target=job.run, kwargs=dict(job_statuses=self.job_statuses)
        )
        worker_process.start()
        self._update_job_status(job, "running")
        return worker_process, job

    def _wait_for_worker_to_finnish(self, worker: Process, job: Job):
        worker.join()
        if worker.exitcode != 0:
            self._update_job_status(job, "error")

    def _update_job_status(self, job: Job, new_status: str):
        job_status = self.job_statuses.get(job.id)
        job_status.status = new_status
        job_status.updated_at = datetime.now()
        self.job_statuses[job.id] = job_status


class JobClerk:
    def __init__(self, worker_manager: Type[WorkerManager] = WorkerManager):
        self.job_manager = Manager()
        self.job_queue = self.job_manager.Queue()
        self.job_statuses = self.job_manager.dict()
        self.worker_manager_process = worker_manager(
            job_queue=self.job_queue, job_statuses=self.job_statuses
        )
        self.worker_manager_process.start()

    def run_job(
        self, job: Callable[[], Optional[dict]], job_id: str = None
    ) -> JobStatus:
        if job_id is None:
            job_id = self.id_generator()
        created_at = datetime.now()
        job_status = JobStatus(
            job_id=job_id,
            status="in_queue",
            created_at=created_at,
            updated_at=created_at,
        )
        task = Job(id=job_id, job=job)
        self.job_statuses[job_id] = job_status
        self.job_queue.put(task)
        return job_status

    def get_job_status(self, job_id: str) -> JobStatus:
        status = self.job_statuses.get(job_id)
        return status

    def id_generator(self) -> str:
        return str(uuid.uuid1())
