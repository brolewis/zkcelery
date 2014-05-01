#! /usr/bin/env python
'''ZooKeeper Data Viewer'''
# Standard Library
import sys           # Command line arguments
import time          # Format lock time
# Third Party
import kazoo.client  # Connect to ZooKeeper


def list_controls():
    '''View current locks/semaphores/mutexes'''
    client = kazoo.client.KazooClient('localhost:2181')
    client.start()
    # Locks
    lock_node = u'/zkcelery/locks'
    lock_header = False
    if client.exists(lock_node):
        for lock_name in client.get_children(lock_node):
            full_path = u'%s/%s' % (lock_node, lock_name)
            lock = client.Lock(full_path)
            contenders = lock.contenders()
            if contenders:
                if not lock_header:
                    print 'Current Locks'
                    lock_header = True
                print '\t%s - %s' % (full_path, contenders)
    # Semaphores
    semaphore_node = u'/zkcelery/semaphores'
    semaphore_header = False
    if client.exists(semaphore_node):
        for semaphore_name in client.get_children(semaphore_node):
            if '__lock__' not in semaphore_name:
                full_path = u'%s/%s' % (semaphore_node, semaphore_name)
                semaphore = client.Semaphore(full_path)
                lease_holders = semaphore.lease_holders()
                if lease_holders:
                    if not semaphore_header:
                        print 'Current Semaphores'
                        semaphore_header = True
                    print '\t%s - %s' % (full_path, lease_holders)
    # Mutex
    mutex_node = u'/zkcelery/mutexes'
    mutex_header = False
    if client.exists(mutex_node):
        for mutex_name in client.get_children(mutex_node):
            full_path = u'%s/%s' % (mutex_node, mutex_name)
            lock_time = time.localtime(client.get(full_path)[1].created)
            lock_str = time.strftime('Set at %Y-%m-%d %H:%M:%S', lock_time)
            if not mutex_header:
                print 'Current Mutexes'
                mutex_header = True
            print '\t%s - %s' % (full_path, lock_str)
    client.stop()
    client.close()


def delete(node):
    '''Delete a node (useful for clearning a lock/semaphore)'''
    client = kazoo.client.KazooClient('localhost:2181')
    client.start()
    client.delete(node, recursive=True)
    client.stop()
    client.close()

if __name__ == '__main__':
    COMMAND = sys.argv[1] if len(sys.argv) >= 2 else None
    if COMMAND == 'list':
        list_controls()
    elif COMMAND == 'delete' and len(sys.argv) == 3:
        delete(sys.argv[2])
    else:
        print 'Supply a command [list | delete]'
