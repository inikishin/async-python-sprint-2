from datetime import datetime, timedelta
import time

from config.logger import logger
from job import Job
from scheduler import Scheduler


def delayed(name):
    logger.info(f'Hi, {name}!')


s = Scheduler(pool_size=2)
scheduler = s.schedule()
scheduler.send(None)

j = Job(time.sleep, (1, ))
scheduler.send(j)

j = Job(time.sleep, (1, ))
scheduler.send(j)

j = Job(time.sleep, (1, ))
scheduler.send(j)

j = Job(time.sleep, (1, ))
scheduler.send(j)

j = Job(delayed, ('Ilya', ), start_at=datetime.now() + timedelta(seconds=15))
scheduler.send(j)

s.run()

time.sleep(10)

s.stop()
