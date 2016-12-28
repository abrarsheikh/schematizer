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

from schematizer.models.note import Note
from schematizer.models.note import ReferenceTypeEnum
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests


class TestGetNotes(GetModelsBasicTests):

    def create_note(self):
        schema = factories.create_avro_schema(
            schema_json={
                "type": "fixed",
                "size": 16,
                "name": factories.generate_name("fixed_type")
            }
        )
        return factories.create_note(
            ReferenceTypeEnum.SCHEMA,
            reference_id=schema.id,
            note_text=factories.generate_name("some notes"),
            last_updated_by="test@example.com"
        )

    entity_cls = Note
    create_entity_func = create_note

    def get_assert_func(self):
        return asserts.assert_equal_note
