# zkcelery

ZKCelery is a collection of helpers for easily using ZooKeeper within Celery.


# Installation

Simply run `pip install zkcelery`


# Requirements

ZKCelery relies on [ZooKeeper](http://zookeeper.apache.org/).


# Usage

A new configuration variable is required in order to let ZKCelery know about your ZooKeeper servers:

    ZOOKEEPER_HOSTS = 'localhost:2181'

This is a comma-separated list of hosts to connect to.
