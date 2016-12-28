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

import pytest

from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.helpers.formatting import _format_datetime
from schematizer.models.note import ReferenceTypeEnum
from schematizer.views import notes as note_views
from schematizer_testing import factories
from schematizer_testing import utils
from tests.views.api_test_base import ApiTestBase


class NotesViewTestBase(ApiTestBase):

    @property
    def user_email(self):
        return 'test.user@yelp.com'

    def _get_expected_note_response(
        self,
        note_id,
        ref_id,
        ref_type,
        note_text,
        updated_by
    ):
        note = utils.get_entity_by_id(models.Note, note_id)
        return {
            'id': note.id,
            'reference_id': ref_id,
            'reference_type': ref_type,
            'note': note_text,
            'last_updated_by': updated_by,
            'created_at': _format_datetime(note.created_at),
            'updated_at': _format_datetime(note.updated_at)
        }


class TestCreateNote(NotesViewTestBase):

    @property
    def note_text(self):
        return 'biz schema note'

    @pytest.fixture
    def create_note_request(self, biz_schema):
        return {
            'reference_id': biz_schema.id,
            'reference_type': ReferenceTypeEnum.SCHEMA,
            'note': self.note_text,
            'last_updated_by': self.user_email
        }

    def test_create_note(self, mock_request, create_note_request):
        mock_request.json_body = create_note_request
        actual = note_views.create_note(mock_request)
        expected = self._get_expected_note_response(
            note_id=actual['id'],
            ref_id=create_note_request['reference_id'],
            ref_type=create_note_request['reference_type'],
            note_text=self.note_text,
            updated_by=self.user_email
        )
        assert actual == expected

    def test_non_existing_schema(self, mock_request, create_note_request):
        create_note_request['reference_id'] = 0

        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = create_note_request
            note_views.create_note(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.REFERENCE_NOT_FOUND_ERROR_MESSAGE


class TestUpdateNote(NotesViewTestBase):

    @property
    def new_note_text(self):
        return 'add some updated notes.'

    @pytest.fixture
    def biz_schema_note(self, biz_schema):
        return factories.create_note(
            reference_type=ReferenceTypeEnum.SCHEMA,
            reference_id=biz_schema.id,
            note_text='biz schema notes.',
            last_updated_by='initial.user@yelp.com'
        )

    @pytest.fixture
    def update_note_request(self):
        return {
            'note': self.new_note_text,
            'last_updated_by': self.user_email
        }

    def test_update_note(
        self,
        mock_request,
        biz_schema_note,
        update_note_request
    ):
        mock_request.json_body = update_note_request
        mock_request.matchdict = {'note_id': str(biz_schema_note.id)}
        actual = note_views.update_note(mock_request)
        expected = self._get_expected_note_response(
            note_id=biz_schema_note.id,
            ref_id=biz_schema_note.reference_id,
            ref_type=biz_schema_note.reference_type,
            note_text=self.new_note_text,
            updated_by=self.user_email
        )
        assert actual == expected

    def test_update_non_existing_note(self, mock_request, update_note_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = update_note_request
            mock_request.matchdict = {'note_id': '0'}
            note_views.update_note(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.NOTE_NOT_FOUND_ERROR_MESSAGE
