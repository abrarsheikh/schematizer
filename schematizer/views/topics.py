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

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.topic import Topic


@view_config(
    route_name='api.v1.get_topic_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_topic_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic = schema_repository.get_topic_by_name(topic_name)
    if topic is None:
        e = EntityNotFoundError(
            entity_cls=Topic,
            entity_desc='Topic name `{}`'.format(topic_name)
        )
        raise exceptions_v1.entity_not_found_exception(e.message)
    return responses_v1.get_topic_response_from_topic(topic)


@view_config(
    route_name='api.v1.list_schemas_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_schemas_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    try:
        schemas = schema_repository.get_schemas_by_topic_name(topic_name)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)
    return [responses_v1.get_schema_response_from_avro_schema(avro_schema)
            for avro_schema in schemas]


@view_config(
    route_name='api.v1.get_latest_schema_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_latest_schema_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    try:
        avro_schema = schema_repository.get_latest_schema_by_topic_name(
            topic_name
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)

    if avro_schema is None:
        raise exceptions_v1.latest_schema_not_found_exception()

    return responses_v1.get_schema_response_from_avro_schema(avro_schema)


@view_config(
    route_name='api.v1.get_topics_by_criteria',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_topics_by_criteria(request):
    param = request.params.get('created_after')
    created_after = int(param) if param is not None else None

    pagination = requests_v1.get_pagination_info(request.params)

    topics = schema_repository.get_topics_by_criteria(
        namespace=request.params.get('namespace'),
        source=request.params.get('source'),
        created_after=created_after,
        page_info=pagination
    )
    return [responses_v1.get_topic_response_from_topic(topic)
            for topic in topics]
