import Queue
import time
import csv
import threading

class Producer:
    def __init__(self, sourcequeue, targetqueue, sc):
        self.sourcequeue = sourcequeue
        self.targetqueue = targetqueue
        self._sc = sc 

    def fetchprojectandtasks(self, batch):
        batchNo = batch[0]
        batchSize = batch[1]
        time.sleep(3)
        start = (batchNo - 1) * batchSize
        end = start + batchSize

        return [start, end]

    def run(self):
        while not self.sourcequeue.empty():
            batch = self.sourcequeue.get()
            start, end = self.fetchprojectandtasks(batch)
            for i in range(start, end):
                item = "Task + " + str(i)
                self.targetqueue.put(item)

        self._sc.completed = True     
        
class Consumer:
    def __init__(self, sourcequeue, sc):
        self.sourcequeue = sourcequeue
        self._sc = sc 

    def createcsv(self, filename):
        outputcsvobject = open(filename, 'wb')
        writer = csv.writer(outputcsvobject, dialect='excel')
        return writer

    def run(self):
        curDate = time.strftime("%Y%m%d")
        filename = 'output_'+curDate+'.csv'
        writer = self.createcsv(filename)

        while True:
            if self.sourcequeue.empty() and self._sc.completed:
                break
            item = self.sourcequeue.get(True)
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
        sourcequeue = self._build_projects_queue(pgcount, pgsize)
        targetqueue = Queue.Queue()

        sc = SynchronizationContext()

        producer = Producer(sourcequeue, targetqueue, sc)
        producer_thread = threading.Thread(target=producer.run)

        consumer = Consumer(targetqueue, sc)
        consumer_thread = threading.Thread(target=consumer.run)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join()
        consumer_thread.join()

    def _build_projects_queue(self, pgcount, pgsize):
        projectsBatchQueue = Queue.Queue()

        for i in range(1, pgcount+1):
            projectsBatchQueue.put((i,pgsize))

        return projectsBatchQueue