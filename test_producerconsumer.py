from producerconsumer import *
import os

def test_build_projects_queue():
    pipeline = Pipeline()
    q = pipeline._build_projects_queue(10,100)
    assert q.qsize() == 10

def test_consumer_create_csv_smoke_test():
    q = Queue.Queue()
    sc = SynchronizationContext()
    consumer = Consumer(q, sc)
    consumer.create_csv("asdf")
    assert True
    os.remove("asdf")

def test_producer_fetch_project_and_tasks():
    sq = Queue.Queue()
    tq = Queue.Queue()
    producer = Producer(sq, tq)
    start, end = producer.fetch_project_and_tasks((1,100))
    assert start == 0
    assert end == 100

def test_pipeline_build_and_run():
    pipeline = Pipeline()
    pipeline.build_and_run(10, 100)