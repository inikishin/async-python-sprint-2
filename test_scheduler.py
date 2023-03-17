import os
import time

from job import Job
from scheduler import Scheduler


def foo(value: int):
    """
    Тестовая функция
    """
    return value * 2


def make_file(file_name):
    """
    Тестовая функция
    """
    open(file_name, 'w+').close()


def test_scheduler():
    """
    Тестируем работу планировщика.
    """
    sch = Scheduler()
    worker = sch.schedule()
    worker.send(None)

    file_name = 'test-file'
    if os.path.exists(file_name):
        os.remove(file_name)
    assert not os.path.exists(file_name)

    job = Job(make_file, (file_name, ))
    worker.send(job)

    sch.run()
    time.sleep(3) # Чтобы файлы точно успели создаться
    sch.stop()

    assert os.path.exists(file_name)


def test_job():
    """
    Тестируем работу джоба.
    """
    job = Job(foo, (2, ))
    res = job.run()
    assert res == 4
