import os
import time
import threading

import psycopg2

import puqu


class DummyJob(object):
    def __init__(self):
        self.calls = []

    def __call__(self, data):
        self.calls.append(data)


class DummyOnTimeout(object):
    def __init__(self, event):
        self.event = event

    def __call__(self, listener_instance):
        if self.event.isSet():
            listener_instance.disconnect()
            raise puqu.exc.StopListening()


def listener_thread(dsn, job, event):
    listener = puqu.PuQuListener(
        select_timeout=1,
        on_timeout=DummyOnTimeout(event))
    listener.configure(dsn)
    listener.register_job('dummy_job', job)
    listener.poll()


def test_puqu():
    DSN = os.environ.get('PUQU_DSN')
    if DSN is None:
        raise Exception('PUQU_DSN env var is not defined')

    puqu.setup_db(DSN, drop=True)

    spawn_threads = 2
    listener_events = [threading.Event() for _ in range(spawn_threads)]
    jobs = [DummyJob() for _ in range(spawn_threads)]

    listener_threads = []
    for i in range(spawn_threads):
        thread = threading.Thread(
            target=listener_thread, args=(DSN, jobs[i], listener_events[i]))
        listener_threads.append(thread)
        thread.start()

    queuer = puqu.PuQu()
    queuer.configure(DSN)
    all_jobs_num = 21
    for jn in range(all_jobs_num):
        queuer.queue('dummy_job', {'job_num': jn + 1})
    queuer.disconnect()

    for ev in listener_events:
        ev.set()

    for thread in listener_threads:
        thread.join()

    job_1 = jobs[0]
    job_2 = jobs[1]
    assert len(job_1.calls) != len(job_2.calls)
    # & means intersection
    assert not (
        set([d['job_num'] for d in job_1.calls])
        & set([d['job_num'] for d in job_2.calls])
    )
    assert (len(job_1.calls) + len(job_2.calls)) == all_jobs_num

    conn = psycopg2.connect(DSN)
    curs = conn.cursor()
    curs.execute('SELECT * from {}'.format(puqu.JOBS_TABLE))
    rows = curs.fetchall()
    for row in rows:
        status = row[-1]
        assert status == puqu.status.PROCESSED


def test_fn_in_shared_lib():
    DSN = os.environ.get('PUQU_DSN')
    if DSN is None:
        raise Exception('PUQU_DSN env var is not defined')
    puqu.setup_db(DSN, drop=True)

    listener = puqu.PuQuListener()
    queuer = puqu.PuQu(dsn=DSN)

    job_ev = threading.Event()
    timeout_ev = threading.Event()

    @listener.job
    def dummy_job(data):
        job_ev.set()

    assert dummy_job.puqu_registered is True

    listener.configure(
        DSN,
        select_timeout=1,
        on_timeout=DummyOnTimeout(timeout_ev),
    )
    listener_thread = threading.Thread(target=listener.poll)
    listener_thread.start()
    time.sleep(1)

    queuer.queue(dummy_job)
    timeout_ev.set()
    listener_thread.join()
    assert job_ev.isSet()
