from datetime import datetime
from dataclasses import dataclass
import logging
import os
from threading import Thread, Event
from typing import Any, Callable, Tuple

from constants import LOGGER_NAME, JOBS_FOLDER, DELAYED_JOBS_FOLDER
from job import Job

logger = logging.getLogger(LOGGER_NAME)


class JobThread(Thread):
    def __init__(self, target: Callable, args: Tuple[Any]):
        self.target = target
        self.args = args
        super().__init__()

        self._stop_event = Event()

    def run(self):
        self.target(*self.args)

    def stop(self):
        self._stop_event.set()


@dataclass
class PendingJob:
    job: Job
    thread: JobThread
    started_at: datetime


@dataclass
class SuccessJob:
    job: Job
    started_at: datetime
    finished_at: datetime


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self.__pool = {inx: None for inx, i in enumerate(range(pool_size))}
        self.__success_jobs = []
        self.__tries = {}
        self.__init_file_struct()
        self.__is_running = True

    def __del(self):
        self.__lock_file.close()

    def __init_file_struct(self):
        if not os.path.exists(JOBS_FOLDER):
            os.mkdir(JOBS_FOLDER)
        if not os.path.exists(DELAYED_JOBS_FOLDER):
            os.mkdir(DELAYED_JOBS_FOLDER)
        self.__lock_file = open('.lock', 'wb')

    def __execute_task(self, job: Job, thread_key: int):
        if job.tries > 0:
            if job.id in self.__tries and self.__tries[job.id] > job.tries:
                logger.info('Remove task %s cause max tries exceeded (%s)',
                            job.id,
                            self.__tries[job.id],
                            )
                os.remove(job.path)
                self.__pool[thread_key] = None
                return

        try:
            job.run()

            self.__success_jobs.append(SuccessJob(
                job=job,
                started_at=self.__pool[thread_key].started_at,
                finished_at=datetime.now()
            ))

            logger.info('Remove task %s', job.id)
            os.remove(job.path)
            self.__pool[thread_key] = None
        except Exception as e:
            logger.error('Task %s in thread %s had error: %s',
                         job.id,
                         thread_key,
                         e,
                         )
            if job.id in self.__tries:
                self.__tries[job.id] += 1
            else:
                self.__tries[job.id] = 1

    def __put(self, job: Job):
        """Ищем для задачи свободный поток и отправляем ее в него на выполнение.
        """
        logger.debug('Thread pool current state %s', self.__pool)
        while True:
            for thread_key in self.__pool.keys():
                if self.__pool[thread_key] is None:
                    logger.info('Put task %s to %s thread', job.id, thread_key)
                    t = JobThread(target=self.__execute_task, args=(job, thread_key, ))
                    self.__pool[thread_key] = PendingJob(
                        job=job,
                        thread=t,
                        started_at=datetime.now(),
                    )
                    t.start()
                    return

    def __check_delayed(self, job: Job):
        if datetime.now().timestamp() > job.start_at:
            self.__put(job)

    def is_running(self):
        return self.__is_running

    @staticmethod
    def load_job(file_name: str) -> Job:
        with open(file_name, 'rb') as job_data:
            return Job.deserialize(job_data.read())

    def schedule(self):
        while True:
            job = (yield)
            if job is not None:
                if job.is_delayed:
                    job_file_name = f'{DELAYED_JOBS_FOLDER}/{job.id}_{job.start_at}'
                else:
                    job_file_name = f'{JOBS_FOLDER}/{job.id}'
                with open(job_file_name, 'wb') as job_file:
                    job_file.write(job.serialize())

    def _run(self):
        self.__is_running = True
        while True:
            if not self.__is_running:
                break
            logger.info('Pending jobs: %s', self.__pool)
            logger.info('Success jobs: %s', self.__success_jobs)
            for job_file in os.listdir(JOBS_FOLDER):
                file_name = f'{JOBS_FOLDER}/{job_file}'
                job = self.load_job(file_name)
                job.path = file_name
                if job.id not in [pending.job.id for pending in self.__pool.values() if pending is not None]:
                    self.__put(job)

            for delayed_job_file in os.listdir(DELAYED_JOBS_FOLDER):
                file_name = f'{DELAYED_JOBS_FOLDER}/{delayed_job_file}'
                job = self.load_job(file_name)
                job.path = file_name
                if job.id not in [pending.job.id for pending in self.__pool.values() if pending is not None]:
                    self.__check_delayed(job)

            for pending_job in [p for p in self.__pool.values() if p is not None]:
                if pending_job.job.max_working_time > 0:
                    execution_time = datetime.now() - pending_job.started_at
                    if execution_time > pending_job.job.max_working_time:
                        pending_job.thread.stop()

    def run(self):
        t = Thread(target=self._run)
        t.start()

    def restart(self):
        self.stop()
        self.run()

    def stop(self):
        self.__is_running = False
