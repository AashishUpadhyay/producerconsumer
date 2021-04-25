import Queue
import time
import csv
import threading

class Producer:
    def __init__(self, source_queue, target_queue):
        self.source_queue = source_queue
        self.target_queue = target_queue

    def fetch_project_and_tasks(self, batch):
        batch_no = batch[0]
        batch_size = batch[1]
        time.sleep(3)
        start = (batch_no - 1) * batch_size
        end = start + batch_size

        return [start, end]

    def run(self):
        while not self.source_queue.empty():
            batch = self.source_queue.get()
            start, end = self.fetch_project_and_tasks(batch)
            for i in range(start, end):
                item = "Task + " + str(i)
                self.target_queue.put(item)
        
class Consumer:
    def __init__(self, source_queue, sc):
        self.source_queue = source_queue
        self._sc = sc 

    def create_csv(self, file_name):
        output_csv_object = open(file_name, 'wb')
        writer = csv.writer(output_csv_object, dialect='excel')
        return writer

    def run(self):
        cur_date = time.strftime("%Y%m%d")
        file_name = 'output_'+cur_date+'.csv'
        writer = self.create_csv(file_name)

        while True:
            if self.source_queue.empty() and self._sc.completed:
                break
            item = self.source_queue.get(True)
            writer.writerow([item])
            print(item + " processed")

class SynchronizationContext:
    def __init__(self):
        self._completed = False

    @property
    def completed(self):
        return self._completed 

    @completed.setter
    def completed(dself, val):
        self._completed = val    

class Pipeline: 

    def build_and_run(self, pgcount, pgsize):
        source_queue = self._build_projects_queue(pgcount, pgsize)
        target_queue = Queue.Queue()

        sc = SynchronizationContext()

        producer = Producer(source_queue, target_queue)
        producer_thread = threading.Thread(target=producer.run)

        consumer = Consumer(target_queue, sc)
        consumer_thread = threading.Thread(target=consumer.run)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join()
        sc.completed = True
        consumer_thread.join()

    def _build_projects_queue(self, pgcount, pgsize):
        projects_batch_queue = Queue.Queue()

        for i in range(1, pgcount+1):
            projects_batch_queue.put((i,pgsize))

        return projects_batch_queue