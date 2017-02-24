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
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.refresh import Refresh


@view_config(
    route_name='api.v1.get_refresh_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_refresh_by_id(request):
    refresh_id = int(request.matchdict.get('refresh_id'))
    try:
        refresh = Refresh.get_by_id(refresh_id)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)
    return responses_v1.get_refresh_response_from_refresh(refresh)


@view_config(
    route_name='api.v1.update_refresh',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def update_refresh(request):
    refresh_id = int(request.matchdict.get('refresh_id'))
    try:
        refresh = Refresh.get_by_id(refresh_id)
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)

    if request.json_body.get('status') is not None:
        refresh.status = request.json_body.get('status')
    if request.json_body.get('offset') is not None:
        refresh.offset = request.json_body.get('offset')
    if request.json_body.get('max_primary_key') is not None:
        refresh.max_primary_key = request.json_body.get('max_primary_key')

    session.flush()
    return responses_v1.get_refresh_response_from_refresh(refresh)


@view_config(
    route_name='api.v1.get_refreshes_by_criteria',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_refreshes_by_criteria(request):
    param = request.params.get('created_after')
    created_after = int(param) if param is not None else None

    param = request.params.get('updated_after')
    updated_after = int(param) if param is not None else None

    refreshes = schema_repository.get_refreshes_by_criteria(
        namespace=request.params.get('namespace'),
        status=request.params.get('status'),
        created_after=created_after,
        updated_after=updated_after
    )
    return [responses_v1.get_refresh_response_from_refresh(refresh)
            for refresh in refreshes]
