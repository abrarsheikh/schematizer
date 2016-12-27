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

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import doc_tool
from schematizer.models.avro_schema import AvroSchema
from schematizer.models.avro_schema import AvroSchemaElement
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.note import ReferenceTypeEnum


@view_config(
    route_name='api.v1.create_note',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def create_note(request):
    req = requests_v1.CreateNoteRequest(**request.json_body)
    assert_reference_exists(req.reference_type, req.reference_id)
    note = doc_tool.create_note(
        reference_type=req.reference_type,
        reference_id=req.reference_id,
        note_text=req.note,
        last_updated_by=req.last_updated_by
    )
    return responses_v1.get_note_response_from_note(note)


def assert_reference_exists(reference_type, reference_id):
    """Checks to make sure that the reference for this note exists.
    If it does not, raise an exception
    """
    model_cls = None
    if reference_type == ReferenceTypeEnum.SCHEMA:
        model_cls = AvroSchema
    elif reference_type == ReferenceTypeEnum.SCHEMA_ELEMENT:
        model_cls = AvroSchemaElement

    if model_cls:
        try:
            return model_cls.get_by_id(reference_id)
        except EntityNotFoundError as e:
            raise exceptions_v1.entity_not_found_exception(e.message)
    raise exceptions_v1.invalid_request_exception(
        "reference_type {} is invalid. It must be one of the values: {}"
        .format(
            reference_type,
            ', '.join(
                (ReferenceTypeEnum.SCHEMA, ReferenceTypeEnum.SCHEMA_ELEMENT)
            )
        )
    )


@view_config(
    route_name='api.v1.update_note',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def update_note(request):
    req = requests_v1.UpdateNoteRequest(**request.json_body)
    note_id = int(request.matchdict.get('note_id'))
    note = doc_tool.get_note_by_id(note_id)
    if note is None:
        raise exceptions_v1.note_not_found_exception()

    note.note = req.note
    note.last_updated_by = req.last_updated_by
    session.flush()
    return responses_v1.get_note_response_from_note(note)
