'''Init Class for ZK-related Tasks.'''
# Local
from .lock import LockTask
from .mutex import MutexTask

__all__ = ['MutexTask', 'LockTask']
