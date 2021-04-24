from producerconsumer import *
import os

def test_build_projects_queue():
    pipeline = Pipeline()
    q = pipeline._build_projects_queue(10,100)
    assert q.qsize() == 10

def test_consumer_createcsv_smoke_test():
    q = Queue.Queue()
    sc = SynchronizationContext()
    consumer = Consumer(q, sc)
    consumer.createcsv("asdf")
    assert True
    os.remove("asdf")

def test_producer_fetchprojectandtasks():
    sq = Queue.Queue()
    tq = Queue.Queue()
    sc = SynchronizationContext()
    producer = Producer(sq, tq, sc)
    start, end = producer.fetchprojectandtasks((1,100))
    assert start == 0
    assert end == 100

def test_pipeline_build_and_run():
    pipeline = Pipeline()
    pipeline.build_and_run(2, 10)