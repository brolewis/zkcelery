'''LockTask'''
# Standard Library
import contextlib
# Third Party
import celery
import kazoo.client


class LockTask(celery.Task):
    '''
    Lock task to prevent/restrict concurrent execution within a task.
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
        hosts = self._get_app().conf.ZOOKEEPER_HOSTS
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
                    print '-RETRY-'
                    self.retry(**kwargs)
            else:
                yield success
        except kazoo.exceptions.KazooException:
            if retry:
                print '-RETRY-'
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
        Set ZooKeeper lock.
            Optional parameters:
                - identifier (unicode) - Identifier used by ZooKeeper to
                                         identify each instance of the lock.
                                         The default is {id}->{hostname}.
                - blocking (boolean) - Determines if the lock should be
                                       blocking. Defaults to False.
                - *args  - List of values to use to refine the lock.
                - retry (boolean) - If a non-blocking lock cannot be required,
                                    retry. Defaults to False
                - **kwargs - Any arguments to be passed to self.retry().
        '''
        return self._lock(args, kwargs, use_lock=True)

    @contextlib.contextmanager
    def semaphore(self, *args, **kwargs):
        '''
        Set ZooKeeper semaphore.
            Optional parameters:
                - max_leases (integer) - Number of concurrent leases to allow.
                                         Defaults to 1.
                - identifier (unicode) - Identifier used by ZooKeeper to
                                         identify each instance of the
                                         semaphore.  The default is
                                         {id}->{hostname}.
                - blocking (boolean) - Determines if the semaphore should be
                                       blocking. Defaults to False.
                - *args  - List of values to use to refine the semaphore.
                - retry (boolean) - If a non-blocking semaphore cannot be
                                    required, retry. Defaults to False
                - **kwargs - Any arguments to be passed to self.retry().
        '''
        return self._lock(args, kwargs)
