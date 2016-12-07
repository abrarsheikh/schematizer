# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import atexit
from contextlib import contextmanager
from glob import glob

import pytest
import staticconf
import testing.mysqld
from cached_property import cached_property
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa

from schematizer.models import database


class PerProcessMySQLDaemon(object):

    _db_name = 'schematizer'

    # Generate Mysqld class which shares the generated database
    Mysqld = testing.mysqld.MysqldFactory(cache_initialized_db=True)

    def __init__(self):
        self._mysql_daemon = self.Mysqld()
        self._create_database()
        self._create_tables()

        atexit.register(self.clean_up)

    def _create_database(self):
        with self._engine_without_db.connect() as conn:
            conn.execute('create database ' + self._db_name)

    @property
    def _engine_without_db(self):
        url_without_db = self._mysql_daemon.url()
        return create_engine(url_without_db)

    @cached_property
    def engine(self):
        url = self._mysql_daemon.url(db=self._db_name)
        return create_engine(url)

    def _create_tables(self):
        fixtures = glob('schema/tables/*.sql')
        with self.engine.connect() as conn:
            conn.execute('use {0}'.format(self._db_name))
            for fixture in fixtures:
                with open(fixture, 'r') as fh:
                    conn.execute(fh.read())

    def clean_up(self):
        self._mysql_daemon.stop()

    def truncate_all_tables(self):
        self._session.execute('begin')
        for table in self._all_tables:
            was_modified = self._session.execute(
                "select count(*) from `%s` limit 1" % table
            ).scalar()
            if was_modified:
                self._session.execute('truncate table `%s`' % table)
        self._session.execute('commit')

    @cached_property
    def _make_session(self):
        # regular sqlalchemy session maker
        return sessionmaker_sa(bind=self.engine)

    @cached_property
    def _session(self):
        return self._make_session()

    @property
    def _all_tables(self):
        return self.engine.table_names()


class DBTestCase(object):

    _per_process_mysql_daemon = PerProcessMySQLDaemon()

    @property
    def engine(self):
        return self._per_process_mysql_daemon.engine

    @pytest.yield_fixture(autouse=True)
    def sandboxed_session(self):
        session_prev_engine = database.session.bind

        with self.setup_topology(self.engine):
            database.session.bind = self.engine
            database.session.enforce_read_only = False
            yield database.session

        database.session.bind = session_prev_engine

    @pytest.yield_fixture(autouse=True)
    def rollback_session_after_test(self, sandboxed_session):
        """After each test, rolls back the sandboxed_session"""
        yield
        sandboxed_session.rollback()

    @pytest.yield_fixture(autouse=True)
    def _truncate_all_tables(self):
        try:
            yield
        except:
            pass
        finally:
            self._per_process_mysql_daemon.truncate_all_tables()

    @contextmanager
    def setup_topology(self, engine):
        # If yelp_conn is not available, it falls back to sqlalchemy session.
        try:
            with _setup_yelp_conn_topology(engine):
                yield
        except ImportError:
            yield


@contextmanager
def _setup_yelp_conn_topology(engine):
    # yelp_conn is imported here instead of on top of the module because it
    # may not be available for external users.
    import yelp_conn

    yelp_conn.reset_module()
    mysql_url = engine.url
    topology = _create_yelp_conn_topology(
        cluster='schematizer',
        db=mysql_url.database,
        user=(mysql_url.username or ''),
        passwd=(mysql_url.password or ''),
        unix_socket=mysql_url.query.get('unix_socket')
    )
    topology_file = yelp_conn.parse_topology(topology)
    mock_conf = {
        'topology': topology_file,
        'connection_set_file': './connection_sets.yaml'
    }
    with staticconf.testing.MockConfiguration(
        mock_conf,
        namespace=yelp_conn.config.namespace
    ):
        yelp_conn.initialize()
        yield


def _create_yelp_conn_topology(cluster, db, unix_socket, user='', passwd=''):
    entries = [
        {
            'cluster': cluster,
            'replica': replica,
            'entries': [{
                'unix_socket': unix_socket,
                'user': user,
                'passwd': passwd,
                'db': db
            }]
        }
        for replica in ('master', 'slave', 'reporting')
    ]
    return {'topology': entries}
