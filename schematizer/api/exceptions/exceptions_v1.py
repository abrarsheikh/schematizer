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

from pyramid import httpexceptions


LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE = 'Latest schema is not found.'
LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Latest topic is not found.'
TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Topic is not found.'
INVALID_AVRO_SCHEMA_ERROR = 'Invalid Avro schema.'
CATEGORY_NOT_FOUND_ERROR_MESSAGE = 'Category not found for the given source'
UNSUPPORTED_TARGET_SCHEMA_MESSAGE = 'Desired target schema type is unsupported'


def invalid_schema_exception(err_message=INVALID_AVRO_SCHEMA_ERROR):
    return httpexceptions.exception_response(422, detail=err_message)


def entity_not_found_exception(err_message='Entity not found.'):
    return httpexceptions.exception_response(404, detail=err_message)


def topic_not_found_exception(err_message=TOPIC_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def latest_topic_not_found_exception(
    err_message=LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def latest_schema_not_found_exception(
    err_message=LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def invalid_request_exception(err_message='Invalid request.'):
    return httpexceptions.exception_response(400, detail=err_message)


def unprocessable_entity_exception(err_message='Unprocessable Entity.'):
    return httpexceptions.exception_response(422, detail=err_message)


def category_not_found_exception(err_message=CATEGORY_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def unsupported_target_schema_exception(
    err_message=UNSUPPORTED_TARGET_SCHEMA_MESSAGE
):
    return httpexceptions.exception_response(501, detail=err_message)
