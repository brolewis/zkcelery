# Third Party
import celery
# Local
import app

if __name__ == '__main__':
    for x in xrange(1, 6, 2):
        try:
            app.test_mutex.delay(x, x+1)
        except celery.exceptions.Reject as exc:
            print exc.reason
        else:
            print x, x+1
#    test_mutex.delay(3, 4)
#    test_mutex.delay(5, 6)
#    test_mutex.delay(7, 8)
