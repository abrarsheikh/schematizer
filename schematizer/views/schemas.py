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
from schematizer.config import log
from schematizer.logic import registration_repository as reg_repo
from schematizer.logic import schema_repository
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.utils.utils import get_current_func_arg_name_values
from schematizer.views import view_common


@view_config(
    route_name='api.v1.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_by_id(request):
    schema_id = request.matchdict.get('schema_id')
    avro_schema = schema_repository.get_schema_by_id(int(schema_id))
    if avro_schema is None:
        raise exceptions_v1.schema_not_found_exception()
    return responses_v1.get_schema_response_from_avro_schema(avro_schema)


@view_config(
    route_name='api.v1.get_schemas_created_after',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schemas_created_after(request):
    created_after_param = request.matchdict.get('created_after')
    created_after_timestamp = (
        int(created_after_param) if created_after_param is not None else None
    )
    page_info = requests_v1.get_pagination_info(request.matchdict)
    include_disabled = request.matchdict.get('include_disabled', False)
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
        req = requests_v1.RegisterSchemaRequest(**request.json_body)
        validate_names([req.namespace, req.source])
        docs_required = (
            req.namespace not in get_config().namespace_no_doc_required
        )

        return _register_avro_schema(
            schema_json=req.schema_json,
            namespace=req.namespace,
            source=req.source,
            source_owner_email=req.source_owner_email,
            contains_pii=req.contains_pii,
            cluster_type=req.cluster_type,
            base_schema_id=req.base_schema_id,
            docs_required=docs_required
        )
    except simplejson.JSONDecodeError as e:
        log.exception("Failed to construct RegisterSchemaRequest. {}"
                      .format(request.json_body))
        raise exceptions_v1.invalid_schema_exception(
            'Error "{error}" encountered decoding JSON: "{schema}"'.format(
                error=str(e),
                schema=request.json_body['schema']
            )
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
        validate_names([namespace, source])
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
        log.exception('{0}'.format(get_current_func_arg_name_values()))
        raise exceptions_v1.invalid_schema_exception(e.message)


@view_config(
    route_name='api.v1.get_schema_elements_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_elements_by_schema_id(request):
    schema_id = int(request.matchdict.get('schema_id'))
    # First check if schema exists
    schema = schema_repository.get_schema_by_id(schema_id)
    if schema is None:
        raise exceptions_v1.schema_not_found_exception()
    # Get schema elements
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


def validate_name(name):
    if not name:
        # Have to check for None case here to avoid NoneType exception
        raise exceptions_v1.empty_src_name_exception()
    if '|' in name:
        # Restrict '|' to avoid ambiguity when parsing input of
        # data_pipeline tailer. One of the tailer arguments is topic
        # and optional offset separated by '|'.
        raise exceptions_v1.restricted_char_exception()
    if name.isdigit():
        raise exceptions_v1.numeric_name_exception()


def validate_names(names):
    for name in names:
        validate_name(name)
