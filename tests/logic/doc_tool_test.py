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
from schematizer.logic import doc_tool
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer_testing import asserts
from schematizer_testing import factories
from schematizer_testing import utils
from tests.models.testing_db import DBTestCase


class TestDocTool(DBTestCase):

    @property
    def namespace_foo(self):
        return 'foo'

    @property
    def source_bar(self):
        return 'bar'

    @property
    def note_text(self):
        return "qwer<3"

    @property
    def user_email(self):
        return "user@yelp.com"

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            'some_topic_name',
            self.namespace_foo,
            self.source_bar,
        )

    @property
    def schema_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    @property
    def schema_elements(self):
        return [
            models.AvroSchemaElement(
                key="yelp.foo",
                element_type="record",
                doc="table foo"
            ),
        ]

    @pytest.fixture
    def schema(self, topic):
        return factories.create_avro_schema(
            self.schema_json,
            self.schema_elements,
            topic_name=topic.name
        )

    @pytest.fixture
    def schema_element(self, schema):
        return schema.avro_schema_elements[0]

    @pytest.fixture
    def schema_note(self, schema):
        return factories.create_note(
            models.ReferenceTypeEnum.SCHEMA,
            schema.id,
            self.note_text,
            self.user_email
        )

    @pytest.fixture
    def schema_element_note(self, schema_element):
        return factories.create_note(
            models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            schema_element.id,
            self.note_text,
            self.user_email
        )

    @pytest.fixture
    def source(self):
        return factories.create_source(
            self.namespace_foo,
            self.source_bar
        )

    @property
    def category(self):
        return 'Business Info'

    @property
    def new_category(self):
        return 'Deals'

    @pytest.fixture
    def source_category(self, source):
        return factories.create_source_category(source.id, self.category)

    def test_get_schema_note(self, schema_note):
        note = doc_tool.get_note_by_reference_id_and_type(
            schema_note.reference_id,
            models.ReferenceTypeEnum.SCHEMA
        )
        self.assert_equal_note(schema_note, note)

    def test_get_notes_by_schemas_and_elements(
        self,
        schema,
        schema_element,
        schema_element_note,
        schema_note
    ):
        actual = doc_tool.get_notes_by_schemas_and_elements(
            [schema],
            [schema_element]
        )
        assert len(actual) == 2
        try:
            self.assert_equal_note(schema_element_note, actual[0])
            self.assert_equal_note(schema_note, actual[1])
        except AssertionError:
            self.assert_equal_note(schema_element_note, actual[1])
            self.assert_equal_note(schema_element_note, actual[0])

    def test_get_schema_element_note(self, schema_element_note):
        note = doc_tool.get_note_by_reference_id_and_type(
            schema_element_note.reference_id,
            models.ReferenceTypeEnum.SCHEMA_ELEMENT
        )
        self.assert_equal_note(schema_element_note, note)

    def test_get_note_with_no_note(self):
        note = doc_tool.get_note_by_reference_id_and_type(1, "type")
        assert note is None

    def test_create_schema_note(self, schema):
        actual_note = doc_tool.create_note(
            models.ReferenceTypeEnum.SCHEMA,
            schema.id,
            self.note_text,
            self.user_email
        )
        expected_note = models.Note(
            reference_type=models.ReferenceTypeEnum.SCHEMA,
            reference_id=schema.id,
            note=self.note_text,
            last_updated_by=self.user_email
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def test_create_schema_element_note(self, schema_element):
        actual_note = doc_tool.create_note(
            models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            schema_element.id,
            self.note_text,
            self.user_email
        )
        expected_note = models.Note(
            reference_type=models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            reference_id=schema_element.id,
            note=self.note_text,
            last_updated_by=self.user_email,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def test_get_distinct_categories(self, source_category):
        actual = doc_tool.get_distinct_categories()
        assert actual == [source_category.category]

    def test_get_source_category(self, source, source_category):
        actual = doc_tool.get_source_category_by_source_id(source.id)
        self.assert_equal_source_category(source_category, actual)

    def test_get_source_categories_by_criteria_namespace(
        self,
        source_category
    ):
        actual = doc_tool.get_source_categories_by_criteria(
            namespace_name=self.namespace_foo
        )
        assert len(actual) == 1
        self.assert_equal_source_category(source_category, actual[0])

    def test_get_source_categories_by_criteria_namespace_and_source(
        self,
        source_category
    ):
        actual = doc_tool.get_source_categories_by_criteria(
            namespace_name=self.namespace_foo,
            source_name=self.source_bar
        )
        assert len(actual) == 1
        self.assert_equal_source_category(source_category, actual[0])

    def test_get_source_categories_by_criteria_nonexistant_source(
        self,
        source_category
    ):
        actual = doc_tool.get_source_categories_by_criteria(
            namespace_name=self.namespace_foo,
            source_name="this_source_does_not_exist"
        )
        assert not actual

    def test_create_source_category(self, source):
        actual = doc_tool.create_source_category(source.id, self.category)
        expected = models.SourceCategory(
            source_id=source.id,
            category=self.category
        )
        self.assert_equal_source_category_partial(expected, actual)

    def test_update_source_category(self, source, source_category):
        doc_tool.update_source_category(
            source.id,
            self.new_category
        )
        expected = models.SourceCategory(
            source_id=source_category.source_id,
            category=self.new_category
        )
        self.assert_equal_source_category_partial(expected, source_category)

    def assert_equal_note(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        self.assert_equal_note_update(expected, actual)

    def assert_equal_note_update(self, expected, actual):
        assert expected.updated_at == actual.updated_at
        self.assert_equal_note_partial(expected, actual)

    def assert_equal_note_partial(self, expected, actual):
        assert expected.reference_type == actual.reference_type
        assert expected.reference_id == actual.reference_id
        assert expected.note == actual.note
        assert expected.last_updated_by == actual.last_updated_by

    def assert_equal_source_category(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_source_category_partial(expected, actual)

    def assert_equal_source_category_partial(self, expected, actual):
        assert expected.source_id == actual.source_id
        assert expected.category == actual.category


class TestUpdateNote(DBTestCase):

    @pytest.fixture
    def schema(self):
        return factories.create_avro_schema(
            schema_json={
                "type": "record", "name": "foo", "doc": "foo",
                "fields": [{"name": "bar", "type": "int", "doc": "bar"}]
            },
        )

    @pytest.fixture
    def schema_element(self, schema):
        return session.query(models.AvroSchemaElement).filter(
            models.AvroSchemaElement.avro_schema_id == schema.id
        ).first()

    def test_update_existing_schema_note(self, schema):
        self._test_update_existing_note(
            reference_type=models.ReferenceTypeEnum.SCHEMA,
            reference_id=schema.id
        )

    def test_update_existing_element_note(self, schema_element):
        self._test_update_existing_note(
            reference_type=models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            reference_id=schema_element.id
        )

    def _test_update_existing_note(self, reference_type, reference_id):
        initial_note = factories.create_note(
            reference_type=reference_type,
            reference_id=reference_id,
            note_text='initial notes',
            last_updated_by='test_dev1@example.com'
        )
        actual = doc_tool.update_note(
            note_id=initial_note.id,
            note_text='new notes',
            last_updated_by='test_dev2@example.com'
        )
        expected = utils.get_entity_by_id(models.Note, initial_note.id)
        asserts.assert_equal_note(actual, expected)

    def test_note_id_does_not_exist(self, schema):
        with pytest.raises(EntityNotFoundError):
            doc_tool.update_note(
                note_id=0,
                note_text='new notes',
                last_updated_by='test_dev2@example.com'
            )
