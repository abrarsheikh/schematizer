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

import simplejson
from pyramid.view import view_config

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.config import get_config
from schematizer.logic import registration_repository as reg_repo
from schematizer.logic import schema_repository
from schematizer.models.avro_schema import AvroSchema
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.views import view_common


@view_config(
    route_name='api.v1.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_by_id(request):
    schema_id = int(request.matchdict.get('schema_id'))
    try:
        avro_schema = AvroSchema.get_by_id(schema_id)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)
    request.response.cache_control = 'max-age=86400'
    return responses_v1.get_schema_response_from_avro_schema(avro_schema)


@view_config(
    route_name='api.v1.get_schemas_created_after',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schemas_created_after(request):
    created_after_param = request.params.get('created_after')
    created_after_timestamp = (
        int(created_after_param) if created_after_param is not None else None
    )
    page_info = requests_v1.get_pagination_info(request.params)
    include_disabled = request.params.get('include_disabled', False)
    schemas = schema_repository.get_schemas_by_criteria(
        created_after=created_after_timestamp,
        page_info=page_info,
        include_disabled=include_disabled
    )
    return [responses_v1.get_schema_response_from_avro_schema(avro_schema)
            for avro_schema in schemas]


@view_config(
    route_name='api.v1.register_schema',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def register_schema(request):
    try:
        schema_str = request.json_body['schema']
        schema_json = simplejson.loads(schema_str)
    except simplejson.JSONDecodeError as e:
        raise exceptions_v1.unprocessable_entity_exception(
            'Error "{error}" encountered decoding JSON: "{schema}"'.format(
                error=str(e),
                schema=schema_str
            )
        )
    namespace = request.json_body['namespace']
    docs_required = namespace not in get_config().namespace_no_doc_required
    return _register_avro_schema(
        schema_json=schema_json,
        namespace=namespace,
        source=request.json_body['source'],
        source_owner_email=request.json_body['source_owner_email'],
        contains_pii=request.json_body['contains_pii'],
        cluster_type=request.json_body.get(
            'cluster_type',
            requests_v1.DEFAULT_KAFKA_CLUSTER_TYPE
        ),
        base_schema_id=request.json_body.get('base_schema_id'),
        docs_required=docs_required
    )


@view_config(
    route_name='api.v1.register_schema_from_mysql_stmts',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def register_schema_from_mysql_stmts(request):
    req = requests_v1.RegisterSchemaFromMySqlRequest(**request.json_body)
    avro_schema_json = view_common.convert_to_avro_from_mysql(
        schema_repository,
        req.new_create_table_stmt,
        req.old_create_table_stmt,
        req.alter_table_stmt
    )
    return _register_avro_schema(
        schema_json=avro_schema_json,
        namespace=req.namespace,
        source=req.source,
        source_owner_email=req.source_owner_email,
        contains_pii=req.contains_pii,
        cluster_type=req.cluster_type,
        docs_required=False
    )


def _register_avro_schema(
    schema_json,
    namespace,
    source,
    source_owner_email,
    contains_pii,
    cluster_type,
    base_schema_id=None,
    docs_required=True
):
    try:
        avro_schema = schema_repository.register_avro_schema_from_avro_json(
            avro_schema_json=schema_json,
            namespace_name=namespace,
            source_name=source,
            source_owner_email=source_owner_email,
            contains_pii=contains_pii,
            cluster_type=cluster_type,
            base_schema_id=base_schema_id,
            docs_required=docs_required
        )
        return responses_v1.get_schema_response_from_avro_schema(avro_schema)
    except ValueError as e:
        raise exceptions_v1.unprocessable_entity_exception(e.message)


@view_config(
    route_name='api.v1.get_schema_elements_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_elements_by_schema_id(request):
    schema_id = int(request.matchdict.get('schema_id'))
    try:
        AvroSchema.get_by_id(schema_id)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)

    elements = schema_repository.get_schema_elements_by_schema_id(schema_id)
    return [responses_v1.get_element_response_from_element(element)
            for element in elements]


@view_config(
    route_name='api.v1.get_meta_attributes_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_meta_attributes_by_schema_id(request):
    try:
        schema_id = int(request.matchdict.get('schema_id'))
        return schema_repository.get_meta_attributes_by_schema_id(schema_id)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_data_targets_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_data_targets_by_schema_id(request):
    try:
        schema_id = int(request.matchdict.get('schema_id'))
        data_targets = reg_repo.get_data_targets_by_schema_id(schema_id)

        return [
            responses_v1.get_data_target_response_from_data_target(data_target)
            for data_target in data_targets
        ]
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)
