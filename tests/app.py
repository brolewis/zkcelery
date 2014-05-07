# Local
import random
import time
# Third Party
import celery
import celery.schedules
# Local
import zkcelery

COUNTDOWN = random.randint(0, 2)


app = celery.Celery('app', broker='amqp://guest@localhost//')
app.conf.ZOOKEEPER_HOSTS = 'localhost:2181'
app.conf.CELERY_ACCEPT_CONTENT = ['pickle', 'json']
app.conf.CELERYBEAT_SCHEDULE = {
    'mutex_test': {
        'task': 'tests.app.test_mutex',
        'schedule': celery.schedules.crontab(),
        'args': (1, 2)
    },
}


@app.task(base=zkcelery.LockTask, bind=True)
def lock_block(self, x, y):
    print 'lock_block'
    with self.lock(blocking=True):
        print x + y
        time.sleep(50)


@app.task(base=zkcelery.LockTask, bind=True)
def lock_args(self, x, y):
    print 'lock_args'
    with self.lock(x, y, blocking=True):
        print x + y
        time.sleep(5)


@app.task(base=zkcelery.LockTask, bind=True)
def lock_identifier(self, x, y):
    print 'lock_identifier'
    with self.lock(identifier='lewis_was_here'):
        print x + y


@app.task(base=zkcelery.LockTask, bind=True)
def lock_path(self, x, y):
    print 'lock_path'
    with self.lock(x, y, lock_path='/home'):
        print x - y


@app.task(base=zkcelery.LockTask, bind=True)
def lock_no_block(self, x, y):
    print 'lock_no_block'
    try:
        with self.lock() as lock_acquired:
            if lock_acquired:
                print x+y
                time.sleep(5)
            else:
                print 'retry'
                self.retry(countdown=1)
    except celery.exceptions.MaxRetriesExceededError:
        print 'ALL DONE'


@app.task(base=zkcelery.LockTask, bind=True)
def lock_retry(self, x, y):
    print 'lock_retry'
    try:
        with self.lock(retry=True, countdown=COUNTDOWN, max_retries=1):
            print x+y
            time.sleep(10)
    except celery.exceptions.MaxRetriesExceededError:
        print 'ALL DONE'


@app.task(base=zkcelery.LockTask, bind=True)
def semaphore_retry(self, x, y):
    print 'semaphore_retry'
    try:
        with self.semaphore(x, retry=True, max_leases=2, countdown=COUNTDOWN,
                            max_retries=1):
            print x+y
            time.sleep(30)
    except celery.exceptions.MaxRetriesExceededError:
        print 'ALL DONE: %s' % (x + y)


@app.task(base=zkcelery.EarlyMutexTask)
def test_early_mutex(x, y):
    print 'test_mutex: %s' % (x + y)
    time.sleep(150)


@app.task(base=zkcelery.MutexTask)
def test_mutex(x, y):
    print 'test_mutex: %s' % (x + y)
    time.sleep(10)
