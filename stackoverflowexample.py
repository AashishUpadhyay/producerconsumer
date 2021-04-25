import Queue
import random
import threading

MAX_QSIZE = 10  # max queue size
BUF_SIZE = 100  # total number of iterations/items to process

class Producer:
    def __init__(self, queue, buf_size=BUF_SIZE):
        self.queue = queue
        self.buf_size = buf_size

    def run(self):
        for _ in range(self.buf_size):
            self.queue.put(random.randint(0, 100))

class Consumer:
    def __init__(self, queue):
        self.queue = queue

    def run(self):
        while not self.queue.empty():
            item = self.queue.get()
            self.queue.task_done()
            print(item)

def main():
    q = Queue.Queue(maxsize=MAX_QSIZE)

    producer = Producer(q)
    producer_thread = threading.Thread(target=producer.run)

    consumer = Consumer(q)
    consumer_thread = threading.Thread(target=consumer.run)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()
    q.join()
    print("done!")


if __name__ == "__main__":
    main()