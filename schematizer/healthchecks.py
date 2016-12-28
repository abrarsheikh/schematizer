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

import random

from schematizer import models
from schematizer.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES
from schematizer.models.database import session


can_watch_config = False
try:
    if FORCE_AVOID_INTERNAL_PACKAGES:
        # TODO(DATAPIPE-1506|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from schematizer
        # after we have completely ready for open source.
        raise ImportError
    from yelp_conn import load
    can_watch_config = True
except ImportError:
    pass


class MysqlHealthCheck(object):

    def __init__(
        self,
        clusters,
        min_reload_interval_s=5,
        max_reload_interval_s=120
    ):
        self.clusters = clusters
        self.reload_interval_s = random.randint(
            min_reload_interval_s,
            max_reload_interval_s
        )
        self.watcher = None

    def get_watcher(self):
        # Lazily instantiate the watcher because the yelp_conn staticconf
        # namespace needs to be loaded.  Do this in the `init` function that
        # you pass to `yelp_pyramid.healthcheck.install_healthcheck`.
        if self.watcher is None:
            self.watcher = load.build_config_watcher(
                self.reload_interval_s,
                self.clusters
            )
        return self.watcher

    def __call__(self, *args, **kwargs):
        if can_watch_config:
            self.get_watcher().reload_if_changed()

        with session.connect_begin(ro=True):
            session.query(models.Namespace).order_by(
                models.Namespace.id
            ).limit(1).all()
