import json
import sys
import select
import logging

import psycopg2

from puqu import exc as puqu_exc


JOBS_TABLE = 'puqu_jobs'
CHANNEL = 'puqu_jobs'


class status(object):
    NEW = 0
    DELIVERED = 1
    MISSING_JOB = 10
    EXC = 20
    PROCESSED = 30


class _PuQuBase(object):

    def __init__(self, channel=CHANNEL, dsn=None):
        self.channel = channel
        self._dsn = dsn
        self._connection = None
        self._cursor = None

    def configure(self, dsn):
        self._dsn = dsn

    def connect(self, dsn=None):
        if dsn is None and self._dsn is None:
            raise puqu_exc.NotConfiguredError(
                "Not configured. "
                "Pass 'dsn' to '__init__', 'configure' or 'connect' method"
            )
        if dsn is not None:
            self._dsn = dsn
        self._connection = psycopg2.connect(self._dsn)
        self._cursor = self._connection.cursor()

    def disconnect(self):
        if self._connection is None:
            raise puqu_exc.NotConnectedError('Not connected')
        self._connection.close()


class PuQu(_PuQuBase):
    logger = logging.getLogger('puqu.puqu')

    def queue(self, job, data=None):
        if self._connection is None and self._dsn is not None:
            self.connect()

        if self._connection is None:
            raise puqu_exc.NotConnectedError(
                "Not connected. Call 'configure' first")

        if hasattr(job, '__call__'):
            if not getattr(job, 'puqu_registered', False):
                raise puqu_exc.JobFunctionNotRegistered(
                    'This function is not registered as job')
            job = job.__name__

        data = data or {}
        if data is not None:
            data = json.dumps(data)
        self._cursor.execute('BEGIN')
        self._cursor.execute(
            ("INSERT INTO {} (name, data) VALUES (%s, %s) RETURNING id"
             .format(JOBS_TABLE)),
            (job, data)
        )
        job_id = str(self._cursor.fetchone()[0])
        self._connection.commit()
        self._cursor.execute('NOTIFY {}, %s'.format(self.channel), (job_id,))
        self._connection.commit()


class PuQuListener(_PuQuBase):
    logger = logging.getLogger('puqu.listener')

    def __init__(self, channel=CHANNEL, dsn=None, select_timeout=5,
                 on_timeout=None, catch_job_exc=True):
        self._select_timeout = select_timeout
        super(PuQuListener, self).__init__(channel, dsn)
        self._on_timeout = on_timeout
        self._registered_jobs = {}
        self._catch_job_exc = catch_job_exc

    def configure(self, dsn, select_timeout=None, on_timeout=None):
        self._dsn = dsn
        if select_timeout is not None:
            self._select_timeout = select_timeout
        if on_timeout is not None:
            self._on_timeout = on_timeout

    def poll(self):
        self.connect()
        self._cursor.execute("LISTEN {}".format(self.channel))
        self._connection.commit()
        while True:
            if (
                select.select([self._connection], [], [], self._select_timeout)
                == ([], [], [])
            ):
                if self._on_timeout is not None:
                    try:
                        self._on_timeout(self)
                    except puqu_exc.StopListening:
                        self.logger.info("StopListening catched - end loop")
                        break
            else:
                self._connection.poll()
                while self._connection.notifies:
                    notify = self._connection.notifies.pop()
                    self._handle_notify(notify)

    def register_job(self, name, handler):
        if name in self._registered_jobs:
            raise puqu_exc.JobNameAlreadyRegistered(
                "Job naimed '{}' already registered"
                .format(name)
            )
        handler.puqu_registered = True
        self._registered_jobs[name] = handler

    def job(self, fn):
        self.register_job(fn.__name__, fn)
        return fn

    def _handle_notify(self, notify):
        self._cursor.execute('BEGIN')
        try:
            self._cursor.execute(
                'SELECT * FROM {} WHERE id=%s '
                'AND status=0 FOR UPDATE NOWAIT;'.format(JOBS_TABLE),
                (int(notify.payload), )
            )
        except psycopg2.OperationalError as exc:
            try:
                sqlstate = str(exc.diag.sqlstate)
            except AttributeError:
                sqlstate = ''
            if sqlstate.lower() == '55P03'.lower():
                self._connection.rollback()
                return
            else:
                raise

        job = self._cursor.fetchone()
        if job is None:
            self._connection.rollback()
            return

        curr_status = job[4]
        if curr_status == status.NEW:
            job_id = job[0]
            self._update_job(job_id, status.DELIVERED)
            self._connection.commit()

            job_name, job_data = job[2], job[3]
            job = self._registered_jobs.get(job_name)
            if job is None:
                self.logger.error(
                    "Job '{}' is not registered"
                    .format(job_name)
                )
                self._update_job(job_id, status.MISSING_JOB)
                self._connection.commit()
            else:
                try:
                    self.logger.debug(
                        'Execute job {}, data: {}'
                        .format(job.__name__, job_data)
                    )
                    job(job_data)
                    self._update_job(job_id, status.PROCESSED)
                    self._connection.commit()
                except Exception as exc:
                    self.logger.error(
                        "Exception while running job '{}'"
                        .format(job_name),
                        exc_info=sys.exc_info()
                    )
                    try:
                        self._update_job(job_id, status.EXC)
                        self._connection.commit()
                    except Exception as exc:
                        self.logger.error(
                            "Exception while updating job status",
                            exc_info=sys.exc_info()
                        )
                    if not self._catch_job_exc:
                        raise

        else:
            self._connection.rollback()

    def _update_job(self, job_id, status_):
        self._cursor.execute(
            "UPDATE {} set status=%s WHERE id=%s".format(JOBS_TABLE),
            (status_, job_id)
        )


def setup_db(dsn, drop=False):
    conn = psycopg2.connect(dsn)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()

    index_name = 'ix_{}_status'.format(JOBS_TABLE)

    if drop:
        curs.execute('DROP TABLE IF EXISTS {}'.format(JOBS_TABLE))
        curs.execute('DROP INDEX IF EXISTS {}'.format(index_name))

    curs.execute('''
        CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC'),
            name TEXT,
            data JSONB,
            status INT DEFAULT 0
        );
    '''.format(JOBS_TABLE))
    curs.execute(
        "CREATE INDEX {} ON {} (status)"
        .format(index_name, JOBS_TABLE))
