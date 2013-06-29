from Queue import Queue
import threading
import logging

log = logging.getLogger(__name__)


def threaded(items, func, num_threads=5, max_queue=200):
    def queue_consumer():
        while True:
            item = queue.get(True)
            try:
                func(item)
            except Exception, e:
                log.exception(e)
            queue.task_done()

    queue = Queue(maxsize=max_queue)

    for i in range(num_threads):
        t = Thread(target=queue_consumer)
        t.daemon = True
        t.start()

    for item in items:
        queue.put(item, True)

    if queue is not threading.currentThread():
        queue.join()
