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
#
#
# This module consists of DefaultScopedSession and sessionmaker, which are
# wrappers over sqlalchemy ScopedSession and sessionmaker with different
# interfaces, to create the sqlalchemy session for database operations. It is
# used when yelp_conn is not available for the service.
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa
from sqlalchemy.orm.scoping import ScopedSession


def sessionmaker(topology_path, cluster_name):
    """This is a wrapper over sqlalchemy sessionmaker that takes specific
    topology file and cluster name for the database used in the Schematizer.

    Args:
        topology_path (str): path pointing to the topology file.
        cluster_name (str): name of db cluster that points to the connection
            settings of the Schematizer db in the topology file.
    Return:
        :class:sqlalchemy.orm.session.sessionmaker session object.
    """
    topology = _read_topology(topology_path)
    cluster_config = _get_cluster_config(topology, cluster_name)
    engine = _create_engine(cluster_config)
    return sessionmaker_sa(bind=engine)


def _read_topology(topology_path):
    with open(str(topology_path)) as f:
        topology_str = f.read()
    return yaml.load(topology_str)


def _get_cluster_config(topology, cluster_name):
    for topo_item in topology.get('topology'):
        if topo_item.get('cluster') == cluster_name:
            return topo_item['entries'][0]


def _create_engine(config):
    return create_engine(
        'mysql://{db_user}@{db_host}/{db_database}'.format(
            db_user=config['user'],
            db_host=config['host'],
            db_database=config['db']
        )
    )


class DefaultScopedSession(ScopedSession):
    """This is a wrapper over sqlalchamy ScopedSession that does sql operations
    in a context manager. Commits happens on exit of context manager, rollback
    if there is an exception inside the context manager. Safely close the
    session in the end.
    """
    @contextmanager
    def connect_begin(self, *args, **kwargs):
        session = self()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            self.remove()
