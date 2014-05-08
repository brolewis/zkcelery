.. _usage:

===========
Basic Usage
===========

Connection Handling
===================

In order to work with ZKCelery, a connection must be established. By default ZKCelery will attempt to connect to a local ZooKeeper server on the default port (2181). To supply an alternate host/port, or to provide a list of servers, create a new Celery configuration variable:

.. code-block:: python

   ZOOKEEPER_HOSTS = 'zk_server:2181'

This value should be a comma-separated list of hosts to connect to (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).

Locking
=======

ZKCelery provides a Celery abstract task that allows distributed lock to be used. The distributed lock can either be ``lock``, which only allows one instance while ``semaphore`` allows the user to set the number of instances, or leases, to be set. The lock can be accessed as a context manager. As such, it is best to set `bind` to `True` for easy access.

.. code-block:: python

    import zkcelery

    @app.task(base=zkcelery.LockTask, bind=True)
    def locking_task(self, data):
        with self.lock() as lock_acquired:
            if lock_acquired:
                do_work(data)

.. note::

    While ``semaphore`` can be used as a lock, the back end mechanisms are different between it and ``lock``, thus the reason for both.

By default, the lock is a non-blocking lock and the status of successful acquisition of the lock is yielded. This allows the end user to determine what should be done if the lock could not be acquired.

When a lock is set, it uses the name of the task to set the lock and make it unique. While this provides a reasonable default, there can be the need to change the uniqueness of the lock. To that end, any value passed in `\*args` will be added to the lock. This allows a lock to be better refined.

.. code-block:: python

    import zkcelery

    @app.task(base=zkcelery.LockTask, bind=True)
    def locking_task(self, a, data):
        with self.lock(retry=True, a):
            do_work(data)

A common use case is to attempt to acquire a lock and, if unsuccessful, retry the task. As such, the lock can accept an optional parameter: retry. If set to true, the task will automatically retry without any further intervention needed. If any additional parameters need to be passed to the retry call, add them in as keyword arguments.

.. code-block:: python

    import zkcelery

    @app.task(base=zkcelery.LockTask, bind=True)
    def locking_task(self, data):
        with self.lock(retry=True, countdown=10, max_retries=1):
            do_work(data)

While the default lock acquisition is non-blocking, there can be use cases where it is better to block and wait for the lock to be acquired. In those cases, the lock can accept another optional parameter: blocking.

.. code-block:: python

    import zkcelery

    @app.task(base=zkcelery.LockTask, bind=True)
    def locking_task(self, data):
        with self.lock(blocking=True):
            do_work(data)

.. note::

    If `blocking` is set to `True`, `retry` is forced to `False`.

When using ``semaphore``, the number of leases can be set by using `max_leases`.
