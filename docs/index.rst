========
ZKCelery
========

ZKCelery is a Python library designed to ease the use of ZooKeeper_ within Celery_. This library provides a lot of the needed boilerplate code so that you can quickly and easily leverage ZooKeeper_ within your project.

ZKCelery's features:

* An implementation of distributed lock and distributed semaphore
* A distributed mutex for tasks
* A number of configuration options for setting the lock, semaphore, and mutex to your needs



Contents:

.. toctree::
   :maxdepth: 1

   install
   usage
   api

Why
===
ZooKeeper_ is a great tool and can be quite useful when developing Celery tasks. Unfortunately, ZooKeeper_ can be challenging, especially to someone new to ZooKeeper_. This library exists to lower the bar for entry and allow developers to quickly start using ZooKeeper_ without first having to understand all the intricacies of ZooKeeper_ and worry about introducing bugs due to a poor implementation.

Source Code
===========

All source code is available on `github under zkcelery <https://github.com/brolewis/zkcelery>`_.

Bugs/Support
============

Bugs and support issues should be reported on the `zkcelery github issue tracker <https://github.com/brolewis/zkcelery/issues>`_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

License
=======

``ZKCelery`` is offered under the The BSD 3-Clause License.

.. _ZooKeeper: http://zookeeper.apache.org
.. _Celery: http://celeryproject.org
