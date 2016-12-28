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

import os

import uwsgi_metrics
from pyramid.config import Configurator
from pyramid.tweens import EXCVIEW

import schematizer.config
import schematizer.models.database
from schematizer import healthchecks
from schematizer.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES
from schematizer.helpers.decorators import memoized
from schematizer.servlib import config_util
from schematizer.servlib import logging_util

SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH')
SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH')


CLUSTERS = [
    ('schematizer', 'master'),
    ('schematizer', 'slave'),
    ('schematizer', 'reporting'),
]


uwsgi_metrics.initialize()


@memoized
def initialize_application():
    """Initialize required configuration variables. Note that it is important
    for this to be `@memoized` as it has caused problems when it happens
    repeatedly (such as during the healthcheck, see DATAPIPE-360)
    """
    config_util.load_default_config(
        SERVICE_CONFIG_PATH,
        SERVICE_ENV_CONFIG_PATH
    )


try:
    # TODO(DATAPIPE-1506|abrar): Currently we have
    # force_avoid_internal_packages as a means of simulating an absence
    # of a yelp's internal package. And all references
    # of force_avoid_internal_packages have to be removed from
    # schematizer after we have completely ready for open source.
    if FORCE_AVOID_INTERNAL_PACKAGES:
        raise ImportError
    import yelp_pyramid.healthcheck
    yelp_pyramid.healthcheck.install_healthcheck(
        'mysql',
        healthchecks.MysqlHealthCheck(CLUSTERS),
        unhealthy_threshold=5,
        healthy_threshold=2,
        init=initialize_application
    )
except ImportError:
    pass


def _create_application():
    """Create the WSGI application, post-fork."""

    settings = {
        'service_name': 'schematizer',
        'zipkin.tracing_percent': 100,
        'pyramid_swagger.swagger_versions': ['1.2', '2.0'],
        'pyramid_swagger.skip_validation': [
            '/(static)\\b',
            '/(api-docs)\\b',
            '/(status)\\b',
            '/(swagger.json)\\b'
        ],
    }

    # Create a basic pyramid Configurator.
    try:
        # TODO(DATAPIPE-1506|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from
        # schematizer after we have completely ready for open source.
        if FORCE_AVOID_INTERNAL_PACKAGES:
            raise ImportError
        import pyramid_yelp_conn    # NOQA
        settings['pyramid_yelp_conn.reload_clusters'] = CLUSTERS
    except ImportError:
        pass
    config = Configurator(settings=settings)

    initialize_application()

    # Add the service's custom configuration, routes, etc.
    config.include(schematizer.config.routes)

    try:
        # TODO(DATAPIPE-1506|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from
        # schematizer after we have completely ready for open source.
        if FORCE_AVOID_INTERNAL_PACKAGES:
            raise ImportError

        import yelp_pyramid
        # Include the yelp_pyramid library default configuration after our
        # configuration so that the yelp_pyramid configuration can base
        # decisions on the service's configuration.
        config.include(yelp_pyramid)

        config.include('pyramid_yelp_conn')
        config.set_yelp_conn_session(schematizer.models.database.session)

        import pyramid_uwsgi_metrics
        # Display metrics on the '/status/metrics' endpoint
        config.include(pyramid_uwsgi_metrics)

        # Including the yelp profiling tween.
        config.include('yelp_profiling')
    except ImportError:
        config.add_tween(
            "schematizer.schematizer_tweens.db_session_tween_factory",
            under=EXCVIEW
        )

    # Include pyramid_swagger for REST endpoints (see ../api-docs/)
    config.include('pyramid_swagger')

    # Include pyramid_mako for template rendering
    config.include('pyramid_mako')

    # Scan the service package to attach any decorated views.
    config.scan(package='schematizer.views')

    return config.make_wsgi_app()


def create_application():
    with logging_util.log_create_application('schematizer_uwsgi'):
        return _create_application()
