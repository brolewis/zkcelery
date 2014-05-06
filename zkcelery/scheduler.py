'''Scheduler'''
# Standard Library
import sys
import traceback
# Third Party
import celery.five
import celery.beat
import celery.utils.log

logger = celery.utils.log.get_logger(__name__)


class EarlyMutexScheduler(celery.beat.PersistentScheduler):
    def apply_async(self, entry, publisher=None, **kwargs):
        entry = self.reserve(entry)
        task = self.app.tasks.get(entry.task)

        try:
            if task:
                result = task.apply_async(entry.args, entry.kwargs,
                                          publisher=publisher, **entry.options)
            else:
                result = self.send_task(entry.task, entry.args, entry.kwargs,
                                        publisher=publisher, **entry.options)
        except celery.exceptions.Reject:
            # Make sure Reject gets propagated to maybe_due so that a proper
            # message may be displayed
            raise
        except Exception as exc:
            message = "Couldn't apply scheduled task {0.name}: {exc}"
            message = message.format(entry, exc=exc)
            celery.five.reraise(celery.beat.SchedulingError,
                                celery.beat.SchedulingError(message),
                                sys.exc_info()[2])
        finally:
            self._tasks_since_sync += 1
            if self.should_sync():
                self._do_sync()
        return result

    def maybe_due(self, entry, publisher=None):
        is_due, next_time_to_run = entry.is_due()

        if is_due:
            logger.info('Scheduler: Sending due task %s (%s)', entry.name,
                        entry.task)
            try:
                result = self.apply_async(entry, publisher=publisher)
            except celery.exceptions.Reject as exc:
                logger.debug('%s skipped: %s', entry.task, exc.reason)
            except Exception as exc:
                logger.error('Message Error: %s\n%s', exc,
                             traceback.format_stack(), exc_info=True)
            else:
                logger.debug('%s sent. id->%s', entry.task, result.id)
        return next_time_to_run
