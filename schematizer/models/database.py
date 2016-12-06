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
# This module setup the database session for database operations. If yelp_conn
# is available, it uses yelp_conn to create session. yelp_conn takes care of
# initializing connection sets (rw and ro). If yelp_conn is unavailable, it
# falls back to DefaultScopedSession which is SQLAlchemy session.
from __future__ import absolute_import
from __future__ import unicode_literals

try:
    from yelp_conn.session import declarative_base
    from yelp_conn.session import scoped_session
    from yelp_conn.session import sessionmaker

    # Single global session manager used to provide sessions through yelp_conn
    session = scoped_session(sessionmaker())

except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

    from schematizer.config import get_config
    from schematizer.models.default_connection import DefaultScopedSession
    from schematizer.models.default_connection import sessionmaker

    # Single global session (sqlalchemy Session) used to provide sessions.
    # Note that it currently doesn't expose a way to specify additional args
    # of sqlalchemy sessionmaker class.
    session = DefaultScopedSession(
        sessionmaker(
            topology_path=get_config().topology_path,
            cluster_name=get_config().schematizer_cluster
        )
    )


# The common declarative base used by every data model.
Base = declarative_base()
Base.__cluster__ = 'schematizer'
