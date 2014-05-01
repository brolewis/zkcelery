'''MutexTask'''
# Standard Library
import contextlib
try:
    from inspect import getcallargs
except ImportError:
    from .backports import getcallargs
import time
# Third Party
import kazoo.client
import celery
import celery.exceptions


class MutexTask(celery.Task):
    '''
    Mutex task to prevent task stacking. The first task wins; any subsequent
    tasks are cancelled.

    Optional parameter:
        - mutex_keys (tuple of strings) - A list of the keys (args or kwargs)
                                          from the task definition that refine
                                          the mutex.
        - mutex_timeout (integer) - Time, in seconds, when the mutex should
                                    expire.
    '''
    abstract = True

    def _get_node(self, args, kwargs):
        '''Get the lock node from the function arguments.'''
        mutex_keys = getattr(self, 'mutex_keys', ())
        lock_node = u'/zkcelery/mutexes/%s' % (self.name)
        items = getcallargs(self.run, *args, **kwargs)
        for value in (unicode(items[x]) for x in mutex_keys if items.get(x)):
            # This replace here converts a slash into a fraction-slash.
            # They look the same but ZooKeeper uses slashes to denote a
            # new node and since a value could contain a slash (e.g. a
            # uri) we want to make it into a non-reserved character.
            lock_node += u'.%s' % (value.replace('/', u'\u2044'))
        return lock_node

    @contextlib.contextmanager
    def mutex(self, args, kwargs, delete=False):
        '''Creates the mutex locks and yields the mutex status.'''
        conf = self._get_app().conf
        global_timeout = getattr(conf, 'MUTEX_TIMEOUT', None)
        items = getcallargs(self.run, *args, **kwargs)
        timeout = items.get('mutex_timeout') or global_timeout or 3600
        success = False
        try:
            hosts = conf.ZOOKEEPER_HOSTS
            client = kazoo.client.KazooClient(hosts=hosts)
            client.start()
            lock_node = self._get_node(args, kwargs)
            if client.exists(lock_node):
                if time.time() - client.get(lock_node)[1].created > timeout:
                    client.delete(lock_node)
                    success = True
            else:
                success = True
        except kazoo.exceptions.KazooException:
            yield False
        else:
            if success:
                client.create(lock_node, makepath=True)
                yield True
                if delete:
                    client.delete(lock_node)
            else:
                yield False
        finally:
            try:
                client.stop()
                client.close()
            except kazoo.exceptions.KazooException:
                pass

    def apply_async(self, args=None, kwargs=None, **options):
        '''Apply the task asynchronously.'''
        with self.mutex(args, kwargs) as mutex_acquired:
            if mutex_acquired:
                return super(MutexTask, self).apply_async(args, kwargs,
                                                          **options)
            else:
                raise celery.exceptions.Reject('Task already running',
                                               requeue=False)

    def __call__(self, *args, **kwargs):
        '''Direct method call.'''
        if self.request.called_directly or self.request.is_eager:
            # This conditional ensures that we only attempt to acuire the mutex
            # if the method has been called directly. Since apply_async will
            # make its way back to this point, we don't want to try and
            # re-acquire the mutex that has already been acquired, leading to
            # certain failure.
            ret = None
            with self.mutex(args, kwargs, delete=True) as mutex_acquired:
                if mutex_acquired:
                    ret = super(MutexTask, self).__call__(*args, **kwargs)
                else:
                    raise celery.exceptions.Reject('Task already running',
                                                   requeue=False)
            return ret
        else:
            return super(MutexTask, self).__call__(*args, **kwargs)

    def after_return(self, *args, **kwargs):
        '''Delete lock node of task, regardles of status.'''
        task_args, task_kwargs = args[3:5]
        if not (self.request.called_directly or self.request.is_eager):
            # Only remove the lock if the job was not called locally
            client = None
            try:
                hosts = self._get_app().conf.ZOOKEEPER_HOSTS
                client = kazoo.client.KazooClient(hosts=hosts)
                client.start()
                lock_node = self._get_node(task_args, task_kwargs)
                if client.exists(lock_node):
                    client.delete(lock_node)
            finally:
                if hasattr(client, 'stop'):
                    client.stop()
                    client.close()
