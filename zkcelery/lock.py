'''Celery Abstract Class Lock Task'''
# Standard Library
import contextlib
# Third Party
import celery
import kazoo.client


class LockTask(celery.Task):
    '''
    LockTask is a Celery abstract class that provides to context managers for
    manipulating locks and semaphores. The easiest way to access the context
    managers is to set `bind` to `True`:

    .. code-block:: python

        import zkcelery

        @app.task(base=zkcelery.LockTask, bind=True)
        def locking_task(self, data):
            with self.lock() as lock_acquired:
                do_work(data)
    '''
    abstract = True

    def _lock(self, args, kwargs, use_lock=False):
        '''Actual method for creating lock/semaphore.'''
        max_leases = kwargs.pop('max_leases', 1)
        identifier = kwargs.pop('identifier', None)
        blocking = kwargs.pop('blocking', False)
        retry = False if blocking else kwargs.pop('retry', False)
        if not identifier:
            identifier = '%s->%s' % (self.request.id, self.request.hostname)
        node_name = 'locks' if use_lock else 'semaphores'
        node_path = u'/zkcelery/%s/%s' % (node_name, self.name)
        for value in (unicode(x) for x in args):
            # This replace here converts a slash into a fraction-slash.
            # They look the same but ZooKeeper uses slashes to denote a
            # new node and since a value could contain a slash (e.g. a
            # uri) we want to make it into a non-reserved character.
            node_path += u'.%s' % (value.replace('/', u'\u2044'))
        client = lock = None
        hosts = getattr(self.app.conf, 'ZOOKEEPER_HOSTS', '127.0.0.1:2181')
        try:
            client = kazoo.client.KazooClient(hosts=hosts)
            client.start()
            if use_lock:
                lock = client.Lock(node_path, identifier=identifier)
            else:
                lock = client.Semaphore(node_path, identifier=identifier,
                                        max_leases=max_leases)
            success = lock.acquire(blocking=blocking)
            if retry:
                if success:
                    yield
                else:
                    self.retry(**kwargs)
            else:
                yield success
        except kazoo.exceptions.KazooException:
            if retry:
                self.retry(**kwargs)
            else:
                yield False
        finally:
            if lock:
                lock.release()
            if client:
                client.stop()
                client.close()

    @contextlib.contextmanager
    def lock(self, *args, **kwargs):
        '''
        lock(self, identifier='', blocking=False, retry=False, *args, **kwargs)

        Set ZooKeeper lock.

        :param identifier: Identifier used by ZooKeeper to identify each
                           instance of the lock. The default is
                           {id}->{hostname}.
        :param blocking: Determines if the lock should be blocking. The
                         default is `False`.
        :param retry: If set to `True`, and `blocking` set to False, a task
                      where the lock was not acquired is retried.
        :param \*args: List of values to use to refine the lock.
        :param \**kwargs: Any arguments to be apssed to self.retry().
        '''
        kwargs['identifier'] = identifier
        kwargs['blocking'] = blocking
        kwargs['retry'] = retry
        return self._lock(args, kwargs, use_lock=True)

    @contextlib.contextmanager
    def semaphore(self, *args, **kwargs):
        '''
        semaphore(self, max_leases=1, identifier='', blocking=False, retry=False, \*args, \**kwargs)

        Set ZooKeeper semaphore.

        :param max_leases: Number of concurrent leases to allow. The
                           default is 1.
        :param identifier: Identifier used by ZooKeeper to identify each
                           instance of the lock. The default is
                           {id}->{hostname}.
        :param blocking: Determines if the lock should be blocking. The
                         default is `False`.
        :param retry: If set to `True`, and `blocking` set to False, a task
                      where the lock was not acquired is retried.
        :param \*args: List of values to use to refine the lock.
        :param \**kwargs: Any arguments to be apssed to self.retry().
        '''
        return self._lock(args, kwargs)
