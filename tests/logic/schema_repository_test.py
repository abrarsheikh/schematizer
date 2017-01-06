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

import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from schematizer import models
from schematizer.logic import schema_repository as schema_repo
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.page_info import PageInfo
from schematizer.models.schema_meta_attribute_mapping import (
    SchemaMetaAttributeMapping)
from schematizer_testing import asserts
from schematizer_testing import factories
from schematizer_testing import utils
from tests.models.testing_db import DBTestCase


# The test module uses sqlalchemy IntegrityError instead of yelp_conn
# IntegrityError because yelp_conn catches and replaces it in the higher
# level. See DATAPIPE-1471

class TestSchemaRepository(DBTestCase):

    @property
    def namespace_name(self):
        return 'foo'

    @property
    def source_name(self):
        return 'bar'

    @property
    def another_source_name(self):
        return "business_v2"

    @property
    def user_source_name(self):
        return "business_v3"

    @property
    def source_owner_email(self):
        return 'dev@test.com'

    @property
    def cluster_type(self):
        return 'datapipe'

    @pytest.fixture
    def namespace(self):
        return factories.create_namespace(self.namespace_name)

    @pytest.fixture
    def source(self, namespace):
        return factories.create_source(self.namespace_name, self.source_name)

    @pytest.fixture
    def user_source(self, namespace):
        return factories.create_source(
            self.namespace_name,
            self.user_source_name
        )

    @pytest.fixture
    def another_source(self, namespace):
        return factories.create_source(
            self.namespace_name,
            self.another_source_name
        )

    @pytest.fixture
    def sorted_sources(self, source, another_source, user_source):
        return sorted(
            [source, another_source, user_source],
            key=lambda source: source.id
        )

    @property
    def some_datetime(self):
        return datetime.datetime(2014, 8, 11, 19, 23, 5, 254)

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            topic_name='topic_one',
            namespace_name=self.namespace_name,
            source_name=self.source_name,
            created_at=self.some_datetime + datetime.timedelta(seconds=1)
        )

    @pytest.fixture
    def refresh(self, source):
        return factories.create_refresh(source_id=source.id)

    @property
    def rw_schema_name(self):
        return "foo"

    @property
    def rw_schema_json(self):
        return {
            "name": self.rw_schema_name,
            "namespace": self.namespace_name,
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    def _build_elements(self, json):
        base_key = "{}.{}".format(json['namespace'], json['name'])
        avro_schema_elements = [
            models.AvroSchemaElement(
                key=base_key,
                element_type="record",
                doc=json['doc']
            )
        ]
        for field in json['fields']:
            avro_schema_elements.append(
                models.AvroSchemaElement(
                    key=models.AvroSchemaElement.compose_key(
                        base_key,
                        field['name']
                    ),
                    element_type='field',
                    doc=field.get('doc')
                )
            )
        return avro_schema_elements

    @property
    def rw_schema_elements(self):
        return self._build_elements(self.rw_schema_json)

    @pytest.fixture
    def rw_schema(self, topic):
        return factories.create_avro_schema(
            self.rw_schema_json,
            self.rw_schema_elements,
            topic_name=topic.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=3)
        )

    @pytest.fixture
    def user_schema(self, topic):
        return factories.create_avro_schema(
            self.rw_schema_json,
            self.rw_schema_elements,
            topic_name=topic.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=6)
        )

    @property
    def another_rw_schema_json(self):
        return {
            "name": self.rw_schema_name,
            "namespace": self.namespace_name,
            "type": "record",
            "fields": [{"name": "baz", "type": "int", "doc": "baz"}],
            "doc": "table foo"
        }

    @property
    def another_rw_schema_elements(self):
        return self._build_elements(self.another_rw_schema_json)

    @pytest.fixture
    def another_rw_schema(self, topic):
        return factories.create_avro_schema(
            self.another_rw_schema_json,
            self.another_rw_schema_elements,
            topic_name=topic.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=4)
        )

    @property
    def disabled_schema_json(self):
        return {
            "type": "record",
            "name": "disabled",
            "namespace": self.namespace_name,
            "fields": [],
            "doc": "I am disabled!"
        }

    @property
    def disabled_schema_elements(self):
        return self._build_elements(self.disabled_schema_json)

    @pytest.fixture
    def disabled_schema(self, topic):
        return factories.create_avro_schema(
            self.disabled_schema_json,
            self.disabled_schema_elements,
            topic_name=topic.name,
            status=models.AvroSchemaStatus.DISABLED,
            created_at=self.some_datetime + datetime.timedelta(seconds=5)
        )

    @pytest.fixture
    def setup_meta_attr_mapping(self, meta_attr_schema, biz_source):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            models.Source.__name__,
            biz_source.id
        )

    @pytest.fixture
    def new_biz_schema_json(self):
        return {
            "name": "biz",
            "type": "record",
            "fields": [
                {"name": "id", "type": "int", "doc": "id", "default": 0},
                {"name": "name", "type": "string", "doc": "biz name"}
            ],
            "doc": "biz table"
        }

    @pytest.fixture
    def new_biz_schema(self, new_biz_schema_json, biz_source):
        return schema_repo.register_avro_schema_from_avro_json(
            new_biz_schema_json,
            biz_source.namespace.name,
            biz_source.name,
            'biz.user@yelp.com',
            contains_pii=False,
            cluster_type=self.cluster_type
        )

    def test_get_latest_topic_of_namespace_source(
        self,
        namespace,
        source,
        topic
    ):
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        asserts.assert_equal_topic(topic, actual)
        new_topic = factories.create_topic(
            topic_name='new_topic',
            namespace_name=source.namespace.name,
            source_name=source.name
        )
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        asserts.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_source_with_no_topic(self, namespace, source):
        # clear all the topics of the source
        topics = session.query(models.Topic).filter(
            models.Topic.source_id == source.id
        ).all()
        for topic in topics:
            session.delete(topic)
        session.flush()

        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        assert actual is None

    def test_get_latest_topic_of_source_with_nonexistent_source(self):
        with pytest.raises(EntityNotFoundError):
            schema_repo.get_latest_topic_of_namespace_source('foo', 'bar')

    def test_get_latest_topic_of_source_id_with_no_topic(self, source):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        assert actual is None

    def test_get_latest_topic_of_source_id_with_nonexistent_source(self):
        actual = schema_repo.get_latest_topic_of_source_id(0)
        assert actual is None

    def test_get_source_by_fullname(self, source):
        actual = schema_repo.get_source_by_fullname(
            self.namespace_name,
            self.source_name
        )
        asserts.assert_equal_source(source, actual)

    def test_get_source_by_fullname_with_nonexistent_source(self):
        actual = schema_repo.get_source_by_fullname('foo', 'bar')
        assert actual is None

    def test_get_latest_schema_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        asserts.assert_equal_avro_schema(rw_schema, actual)

    def test_get_latest_schema_by_topic_id_with_nonexistent_topic(self):

        actual = schema_repo.get_latest_schema_by_topic_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id_with_empty_topic(self, topic):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    @pytest.mark.usefixtures('disabled_schema')
    def test_get_latest_schema_by_topic_id_with_all_disabled_schema(
        self,
        topic
    ):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    def test_get_latest_schema_by_topic_name(self, topic, rw_schema):
        actual = schema_repo.get_latest_schema_by_topic_name(topic.name)
        asserts.assert_equal_avro_schema(rw_schema, actual)

    def test_get_latest_schema_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(EntityNotFoundError):
            schema_repo.get_latest_schema_by_topic_name('_bad.topic')

    def test_get_schemas_by_topic_name(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_name(topic.name)
        assert 1 == len(actual)
        asserts.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_by_topic_name_including_disabled(
        self,
        topic,
        rw_schema,
        disabled_schema
    ):
        actual = schema_repo.get_schemas_by_topic_name(topic.name, True)
        self.assert_equal_entities(
            expected_entities=[rw_schema, disabled_schema],
            actual_entities=actual,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(EntityNotFoundError):
            schema_repo.get_schemas_by_topic_name('foo')

    def test_get_schemas_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_id(topic.id)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[rw_schema],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_id_including_disabled(
        self,
        topic,
        rw_schema,
        disabled_schema
    ):
        actual = schema_repo.get_schemas_by_topic_id(topic.id, True)
        self.assert_equal_entities(
            expected_entities=[rw_schema, disabled_schema],
            actual_entities=actual,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_id_with_nonexistent_topic(self):
        actual = schema_repo.get_schemas_by_topic_id(0)
        assert [] == actual

    def test_get_topics_by_source_id(self, source, topic):
        actual = schema_repo.get_topics_by_source_id(source.id)
        assert 1 == len(actual)
        asserts.assert_equal_topic(topic, actual[0])

    def test_get_schema_elements_with_no_schema(self):
        actual = schema_repo.get_schema_elements_by_schema_id(1)
        assert 0 == len(actual)

    def test_get_schema_elements_by_schema_id(self, rw_schema):
        actual = schema_repo.get_schema_elements_by_schema_id(rw_schema.id)
        for i in range(len(self.rw_schema_elements)):
            self.assert_equal_avro_schema_element_partial(
                actual[i],
                self.rw_schema_elements[i]
            )

    def test_create_refresh(self):
        source = factories.create_source('foo_namespace', 'bar_source')
        actual = schema_repo.create_refresh(
            source_id=source.id,
            offset=0,
            batch_size=200,
            priority=50,
            filter_condition='user="someone"',
            avg_rows_per_second_cap=100
        )
        expected = utils.get_entity_by_id(models.Refresh, actual.id)
        asserts.assert_equal_refresh(actual, expected)

    def test_list_refreshes_source_id(self, refresh, source):
        refreshes = schema_repo.list_refreshes_by_source_id(source.id)
        expected_refresh = models.Refresh(
            source_id=refresh.source_id,
            status=refresh.status,
            offset=refresh.offset,
            batch_size=refresh.batch_size,
            priority=refresh.priority,
            filter_condition=refresh.filter_condition
        )
        assert len(refreshes) == 1
        self.assert_equal_refresh_partial(refreshes[0], expected_refresh)

    def test_list_refreshes_by_source_id(self, source, refresh):
        actual = schema_repo.list_refreshes_by_source_id(source.id)
        assert 1 == len(actual)
        self.assert_equal_refresh(actual[0], refresh)

    def test_get_meta_attr_by_new_schema_id(
        self,
        setup_meta_attr_mapping,
        new_biz_schema,
        meta_attr_schema
    ):
        actual = schema_repo.get_meta_attributes_by_schema_id(
            new_biz_schema.id
        )
        expected = [meta_attr_schema.id]
        assert actual == expected

    def test_get_meta_attr_by_old_schema_id(
        self,
        setup_meta_attr_mapping,
        biz_schema
    ):
        actual = schema_repo.get_meta_attributes_by_schema_id(biz_schema.id)
        expected = []
        assert actual == expected

    def test_get_meta_attr_by_invalid_schema_id(self, setup_meta_attr_mapping):
        with pytest.raises(EntityNotFoundError):
            schema_repo.get_meta_attributes_by_schema_id(schema_id=0)

    def assert_equal_entities(
        self,
        expected_entities,
        actual_entities,
        assert_func,
        filter_key='id',
    ):
        assert len(expected_entities) == len(actual_entities)
        for actual_elem in actual_entities:
            expected_elem = next(
                o for o in expected_entities
                if getattr(o, filter_key) == getattr(actual_elem, filter_key)
            )
            assert_func(expected_elem, actual_elem)

    def assert_equal_avro_schema_element_partial(self, expected, actual):
        assert expected.key == actual.key
        assert expected.element_type == actual.element_type
        assert expected.doc == actual.doc

    def assert_equal_avro_schema_element(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema_id == actual.avro_schema_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_avro_schema_element_partial(expected, actual)

    def assert_equal_refresh(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_refresh_partial(expected, actual)

    def assert_equal_refresh_partial(self, expected, actual):
        assert expected.source_id == actual.source_id
        assert expected.status == actual.status
        assert expected.offset == actual.offset
        assert expected.batch_size == actual.batch_size
        assert expected.priority == actual.priority
        assert expected.filter_condition == actual.filter_condition


class TestRegisterSchema(DBTestCase):

    @property
    def avro_schema_json(self):
        return {
            "type": "record",
            "name": "example_schema",
            "doc": "example schema for test",
            "fields": [{"type": "int", "name": "id", "doc": "id"}]
        }

    @property
    def pkey_avro_schema_json(self):
        return {
            "type": "record",
            "name": "example_schema",
            "doc": "example schema for test",
            "fields": [{"type": "int", "name": "id", "doc": "id", "pkey": 1}],
            "pkey": ["id"]
        }

    def test_register_new_schema_json(self):
        actual = self._register_avro_schema(self.avro_schema_json)
        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(actual, expected)

    def test_register_new_schema_json_with_alias(self):
        actual = self._register_avro_schema(
            self.avro_schema_json,
            alias='simple_schema_alias'
        )
        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(actual, expected)

    def test_register_same_schema_twice(self):
        schema_one = self._register_avro_schema(self.avro_schema_json)
        schema_two = self._register_avro_schema(self.avro_schema_json)
        asserts.assert_equal_avro_schema(schema_one, schema_two)

    def test_register_same_schema_with_alias_twice(self):
        alias = 'simple_schema_alias'
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            alias=alias
        )
        schema_two = self._register_avro_schema(
            self.avro_schema_json,
            alias=alias
        )
        asserts.assert_equal_avro_schema(schema_one, schema_two)

    def test_register_same_schema_in_diff_namespace(self):
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            namespace_name='foo'
        )
        schema_two = self._register_avro_schema(
            self.avro_schema_json,
            namespace_name='new_foo'
        )
        assert schema_one.topic.id != schema_two.topic.id

        source_one = schema_one.topic.source
        source_two = schema_two.topic.source
        assert source_one.id != source_two.id
        assert source_one.namespace.id != source_two.namespace.id
        assert source_one.namespace.name == 'foo'
        assert source_two.namespace.name == 'new_foo'

    def test_register_same_schema_in_diff_source(self):
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            source_name='bar'
        )
        schema_two = self._register_avro_schema(
            self.avro_schema_json,
            source_name='new_bar'
        )
        assert schema_one.topic.id != schema_two.topic.id

        src_one = schema_one.topic.source
        src_two = schema_two.topic.source
        assert src_one.id != src_two.id
        assert src_one.name == 'bar'
        assert src_two.name == 'new_bar'
        asserts.assert_equal_namespace(src_one.namespace, src_two.namespace)

    def test_register_same_schema_with_diff_base_schema(self):
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=None
        )
        schema_two = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

    def test_register_same_schema_with_alias_with_diff_base_schema(
        self
    ):
        alias = "simple_schema_alias"
        self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=None,
            alias=alias
        )
        with pytest.raises(ValueError) as err:
            self._register_avro_schema(
                self.avro_schema_json,
                base_schema_id=10,
                alias=alias
            )
        assert err.value.message == (
            "alias `{}` has already been taken.".format(alias)
        )

    def test_register_schema_with_different_pii(self):
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            contains_pii=False
        )
        schema_two = self._register_avro_schema(
            self.avro_schema_json,
            contains_pii=True
        )
        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

    def test_register_schema_with_alias_with_different_pii(self):
        alias = "simple_schema_alias"
        self._register_avro_schema(
            self.avro_schema_json,
            contains_pii=False,
            alias=alias
        )
        with pytest.raises(ValueError) as err:
            self._register_avro_schema(
                self.avro_schema_json,
                contains_pii=True,
                alias=alias
            )
        assert err.value.message == (
            "alias `{}` has already been taken.".format(alias)
        )

    def test_register_same_schema_with_different_aliases(self):
        self._register_avro_schema(
            self.avro_schema_json,
            alias="alias_one"
        )
        with pytest.raises(ValueError) as err:
            self._register_avro_schema(
                self.avro_schema_json,
                alias="alias_two"
            )
        assert err.value.message == (
            "Same schema with a different alias already exists."
        )

    def test_register_schema_with_pkey_added(self):
        schema_one = self._register_avro_schema(self.avro_schema_json)
        pkey_schema = self._register_avro_schema(self.pkey_avro_schema_json)

        assert schema_one.topic.id != pkey_schema.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, pkey_schema.topic.source
        )

    def test_register_schema_with_same_pkey(self):
        schema_one = self._register_avro_schema(self.pkey_avro_schema_json)
        schema_two = self._register_avro_schema(self.pkey_avro_schema_json)
        asserts.assert_equal_avro_schema(schema_one, schema_two)

    def test_register_schem_with_new_pkey(self):
        schema_json_one = {
            "type": "record",
            "name": "example_schema",
            "doc": "example schema for test",
            "fields": [
                {"type": "int", "name": "id", "doc": "id", "pkey": 1},
                {"type": "int", "name": "pid", "doc": "pid"},
            ],
            "pkey": ["id"]
        }
        schema_json_two = {
            "type": "record",
            "name": "example_schema",
            "doc": "example schema for test",
            "fields": [
                {"type": "int", "name": "id", "doc": "id"},
                {"type": "int", "name": "pid", "doc": "pid", "pkey": 1},
            ],
            "pkey": ["pid"]
        }
        schema_one = self._register_avro_schema(schema_json_one)
        pkey_schema = self._register_avro_schema(schema_json_two)

        assert schema_one.topic.id != pkey_schema.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, pkey_schema.topic.source
        )

    def test_register_schema_with_different_cluster_type(self):
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            cluster_type='datapipe'
        )
        schema_two = self._register_avro_schema(
            self.pkey_avro_schema_json,
            cluster_type='scribe'
        )

        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

    def test_register_schema_without_cluster_type(self):
        with pytest.raises(IntegrityError):
            self._register_avro_schema(
                self.avro_schema_json,
                cluster_type=None
            )

    def test_register_same_schema_with_same_base_schema(self):
        schema_1 = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        schema_2 = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        asserts.assert_equal_avro_schema(schema_1, schema_2)

    def test_register_same_schema_with_same_base_schema_and_alias(self):
        alias = 'simple_schema_alias'
        result_a1 = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10,
            alias=alias
        )
        result_a2 = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10,
            alias=alias
        )
        asserts.assert_equal_avro_schema(result_a1, result_a2)

    def test_register_different_schemas_with_same_base_schema(self):
        # Registering a different transformed schema should result in a
        # different schema/topic
        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        schema_two = self._register_avro_schema(
            self.pkey_avro_schema_json,
            base_schema_id=20
        )
        assert schema_one.base_schema_id != schema_two.base_schema_id
        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

        # Re-registering the original transformed schema will should
        # result in the original's schema/topic
        schema_three = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        asserts.assert_equal_avro_schema(schema_three, schema_one)

    def test_register_compatible_transformed_schema_stays_in_topic(self):
        # adding new field with default value is compatible change
        compatible_schema_json = dict(self.avro_schema_json)
        compatible_schema_json['fields'].append(
            {"type": "long", "name": "amount", "doc": "amount", "default": 0}
        )

        schema_one = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        schema_two = self._register_avro_schema(
            compatible_schema_json,
            base_schema_id=10
        )
        assert schema_one.id != schema_two.id
        asserts.assert_equal_topic(schema_one.topic, schema_two.topic)

        schema_three = self._register_avro_schema(
            self.avro_schema_json,
            base_schema_id=10
        )
        assert schema_one.id != schema_three.id
        asserts.assert_equal_topic(schema_one.topic, schema_three.topic)

    def test_register_full_compatible_schema(self):
        # adding new field with default value is compatible change
        compatible_schema_json = dict(self.avro_schema_json)
        compatible_schema_json['fields'].append(
            {"type": "long", "name": "amount", "doc": "amount", "default": 0}
        )

        schema_one = self._register_avro_schema(self.avro_schema_json)
        schema_two = self._register_avro_schema(compatible_schema_json)

        assert schema_one.id != schema_two.id
        asserts.assert_equal_topic(schema_one.topic, schema_two.topic)

    def test_register_incompatible_schema(self):
        # changing field type from int to string is incompatible change
        incompatible_schema_json = dict(self.avro_schema_json)
        incompatible_schema_json['fields'][0]['type'] = 'string'

        schema_one = self._register_avro_schema(self.avro_schema_json)
        schema_two = self._register_avro_schema(incompatible_schema_json)

        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

    def test_check_schema_compatibility_with_all_enabled_schems(self):
        schema_one_json = dict(self.avro_schema_json)
        schema_one_json['fields'].append(
            {"type": "long", "name": "abc", "doc": "abc", "default": 0}
        )
        schema_one = self._register_avro_schema(schema_one_json)

        # remove one column is compatible change
        schema_two = self._register_avro_schema(self.avro_schema_json)

        assert schema_one.id != schema_two.id
        asserts.assert_equal_topic(schema_one.topic, schema_two.topic)

        schema_three_json = dict(schema_one_json)
        schema_three_json['fields'][-1] = {
            "type": "string", "name": "abc", "doc": "abc", "default": ''
        }
        schema_three = self._register_avro_schema(schema_three_json)

        assert schema_three.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_three.topic.source, schema_two.topic.source
        )

    @pytest.fixture
    def meta_attr_one_id(self):
        return factories.create_avro_schema(
            schema_json={"type": "fixed", "name": "abc", "size": 8},
            namespace='meta_attr_foo',
            source='meta_attr_bar',
            topic_name='meta_attr_topic_one'
        ).id

    @pytest.fixture
    def meta_attr_two_id(self):
        return factories.create_avro_schema(
            schema_json={"type": "fixed", "name": "abc", "size": 8},
            namespace='meta_attr_foo',
            source='meta_attr_baz',
            topic_name='meta_attr_topic_two'
        ).id

    def test_register_schema_with_meta_attrs(
        self, meta_attr_one_id, meta_attr_two_id
    ):
        # add meta attribute to namespace and source
        some_schema = self._register_avro_schema(self.avro_schema_json)
        factories.create_meta_attribute_mapping(
            meta_attr_one_id,
            models.Namespace.__name__,
            some_schema.topic.source.namespace.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_two_id,
            models.Source.__name__,
            some_schema.topic.source.id
        )

        actual = self._register_avro_schema(self.avro_schema_json)

        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(actual, expected)

        actual_meta_attr_ids = self._get_meta_attr_ids(actual.id)
        expected_meta_attr_ids = {meta_attr_one_id, meta_attr_two_id}
        assert actual_meta_attr_ids == expected_meta_attr_ids

    def test_register_schema_will_pickup_new_meta_attrs(
        self, meta_attr_one_id
    ):
        schema_one = self._register_avro_schema(self.avro_schema_json)
        schema_one_meta_attr_ids = self._get_meta_attr_ids(schema_one.id)
        assert not schema_one_meta_attr_ids

        # add meta attribute to source
        factories.create_meta_attribute_mapping(
            meta_attr_one_id,
            models.Source.__name__,
            schema_one.topic.source.id
        )
        schema_two = self._register_avro_schema(self.avro_schema_json)
        schema_two_meta_attr_ids = self._get_meta_attr_ids(schema_two.id)
        assert schema_two_meta_attr_ids == {meta_attr_one_id}

        assert schema_one.topic.id != schema_two.topic.id
        asserts.assert_equal_source(
            schema_one.topic.source, schema_two.topic.source
        )

    def test_register_same_schema_and_meta_attrs_twice(self, meta_attr_one_id):
        some_schema = self._register_avro_schema(self.avro_schema_json)
        factories.create_meta_attribute_mapping(
            meta_attr_one_id,
            models.Source.__name__,
            some_schema.topic.source.id
        )

        schema_one = self._register_avro_schema(self.avro_schema_json)
        schema_two = self._register_avro_schema(self.avro_schema_json)
        asserts.assert_equal_avro_schema(schema_one, schema_two)

    def test_register_schema_when_same_meta_attr_mapped_to_ns_and_src(
        self, meta_attr_one_id
    ):
        schema_one = self._register_avro_schema(self.avro_schema_json)
        factories.create_meta_attribute_mapping(
            meta_attr_one_id,
            models.Namespace.__name__,
            schema_one.topic.source.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_one_id,
            models.Source.__name__,
            schema_one.topic.source.id
        )

        actual = self._register_avro_schema(self.avro_schema_json)

        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(expected, actual)
        actual_meta_attr_ids = self._get_meta_attr_ids(actual.id)
        assert actual_meta_attr_ids == {meta_attr_one_id}

    @pytest.mark.parametrize("schema_with_doc", [
        {
            "name": "foo",
            "doc": "test_doc",
            "type": "record",
            "namespace": "test_namespace",
            "fields": [{"type": "int", "name": "col", "doc": "test_doc"}]
        },
        {"name": "color", "doc": "test_d", "type": "enum", "symbols": ["red"]}
    ])
    @pytest.mark.parametrize("docs_required", [True, False])
    def test_register_schema_with_doc(self, schema_with_doc, docs_required):
        actual = self._register_avro_schema(
            schema_with_doc,
            docs_required=docs_required
        )
        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(actual, expected)

    @pytest.fixture(params=[
        {
            "name": "foo",
            "doc": " ",
            "type": "record",
            "namespace": "test_namespace",
            "fields": [{"type": "int", "name": "col"}]
        },
        {"name": "color", "type": "enum", "symbols": ["red"]}
    ])
    def avro_schema_without_doc(self, request):
        return request.param

    def test_register_schema_without_doc_but_docs_required(
        self, avro_schema_without_doc
    ):
        with pytest.raises(ValueError):
            self._register_avro_schema(avro_schema_without_doc)

    def test_register_schema_without_doc_and_doc_not_required(
        self, avro_schema_without_doc
    ):
        actual = self._register_avro_schema(
            avro_schema_without_doc,
            docs_required=False
        )
        expected = utils.get_entity_by_id(models.AvroSchema, actual.id)
        asserts.assert_equal_avro_schema(actual, expected)

    @pytest.mark.parametrize("empty_email", [(None), (' ')])
    def test_register_schema_with_empty_owner_email(self, empty_email):
        with pytest.raises(ValueError):
            self._register_avro_schema(
                self.avro_schema_json,
                source_owner_email=empty_email
            )

    @pytest.mark.parametrize("invalid_name", [None, ' ', '123', 'a|b'])
    def test_register_schema_with_invalid_namespace_name(self, invalid_name):
        with pytest.raises(ValueError):
            self._register_avro_schema(
                self.avro_schema_json,
                namespace_name=invalid_name
            )

    @pytest.mark.parametrize("invalid_name", [None, ' ', '123', 'a|b'])
    def test_register_schema_with_invalid_source_name(self, invalid_name):
        with pytest.raises(ValueError):
            self._register_avro_schema(
                self.avro_schema_json,
                source_name=invalid_name
            )

    def _register_avro_schema(self, avro_schema_json, **overrides):
        params = {
            'avro_schema_json': avro_schema_json,
            'namespace_name': 'foo',
            'source_name': 'bar',
            'source_owner_email': 'test@example.com',
            'contains_pii': False,
            'cluster_type': 'datapipe'
        }
        if overrides:
            params.update(overrides)
        return schema_repo.register_avro_schema_from_avro_json(**params)

    def _get_meta_attr_ids(self, schema_id):
        result = session.query(
            SchemaMetaAttributeMapping.meta_attr_schema_id
        ).filter(
            SchemaMetaAttributeMapping.schema_id == schema_id
        ).order_by(SchemaMetaAttributeMapping.id).all()
        return {entry[0] for entry in result}


class TestSchemaCompatibility(DBTestCase):

    @property
    def schema_one(self):
        return {
            "type": "record",
            "name": "example_schema",
            "doc": "example schema for test",
            "fields": [{"type": "int", "name": "id", "doc": "id"}]
        }

    @property
    def schema_with_int_field(self):
        schema = dict(self.schema_one)
        schema['fields'].append(
            {"type": "int", "name": "abc", "doc": "abc", "default": 0}
        )
        return schema

    @property
    def schema_with_string_field(self):
        schema = dict(self.schema_one)
        schema['fields'].append(
            {"type": "string", "name": "abc", "doc": "abc", "default": ""}
        )
        return schema

    def test_is_schema_compatible(self):
        factories.create_avro_schema(
            self.schema_one,
            namespace='foo',
            source='bar'
        )
        assert schema_repo.is_schema_compatible(
            self.schema_with_int_field,
            namespace='foo',
            source='bar'
        )

    def test_schema_is_not_compatible_with_all_schemas(self):
        factories.create_avro_schema(
            self.schema_with_int_field,
            namespace='foo',
            source='bar'
        )
        factories.create_avro_schema(
            self.schema_one,
            namespace='foo',
            source='bar'
        )
        assert not schema_repo.is_schema_compatible(
            self.schema_with_string_field,
            namespace='foo',
            source='bar'
        )

    def test_only_check_compatibility_with_enabled_schemas(self):
        factories.create_avro_schema(
            self.schema_with_int_field,
            namespace='foo',
            source='bar',
            status=models.AvroSchemaStatus.DISABLED
        )
        factories.create_avro_schema(
            self.schema_one,
            namespace='foo',
            source='bar'
        )
        assert schema_repo.is_schema_compatible(
            self.schema_with_string_field,
            namespace='foo',
            source='bar'
        )

    def test_is_schema_compatible_with_nonexistent_source(self):
        with pytest.raises(EntityNotFoundError):
            schema_repo.is_schema_compatible(
                self.schema_one,
                namespace='bad_namespace',
                source='bad_source'
            )


@pytest.mark.usefixtures('sorted_topics', 'sorted_refreshes')
class TestGetRefreshesByCriteria(DBTestCase):

    @pytest.fixture
    def yelp_namespace(self):
        return factories.create_namespace(namespace_name='yelp')

    @pytest.fixture
    def aux_namespace(self):
        return factories.create_namespace(namespace_name='aux')

    @pytest.fixture
    def biz_source(self, yelp_namespace):
        return factories.create_source(yelp_namespace.name, source_name='biz')

    @pytest.fixture
    def user_source(self, yelp_namespace):
        return factories.create_source(yelp_namespace.name, source_name='user')

    @pytest.fixture
    def cta_source(self, aux_namespace):
        return factories.create_source(aux_namespace.name, source_name='cta')

    @property
    def some_datetime(self):
        return datetime.datetime(2015, 3, 1, 10, 23, 5, 254)

    @pytest.fixture
    def biz_refresh(self, biz_source):
        return factories.create_refresh(source_id=biz_source.id)

    @pytest.fixture
    def user_refresh(self, user_source):
        return factories.create_refresh(source_id=user_source.id)

    @pytest.fixture
    def cta_refresh(self, cta_source):
        return factories.create_refresh(source_id=cta_source.id)

    @pytest.fixture
    def biz_topic(self, biz_source):
        return factories.create_topic(
            topic_name='yelp.biz.topic.1',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=3)
        )

    @pytest.fixture
    def user_topic_1(self, user_source):
        return factories.create_topic(
            topic_name='yelp.user.topic.1',
            namespace_name=user_source.namespace.name,
            source_name=user_source.name,
            created_at=self.some_datetime - datetime.timedelta(seconds=1)
        )

    @pytest.fixture
    def user_topic_2(self, user_source):
        return factories.create_topic(
            topic_name='yelp.user.topic.two',
            namespace_name=user_source.namespace.name,
            source_name=user_source.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=5)
        )

    @pytest.fixture
    def cta_topic(self, cta_source):
        return factories.create_topic(
            topic_name='aux.cta.topic.1',
            namespace_name=cta_source.namespace.name,
            source_name=cta_source.name,
            created_at=self.some_datetime + datetime.timedelta(minutes=1)
        )

    @pytest.fixture
    def sorted_refreshes(self, biz_refresh, user_refresh, cta_refresh):
        return sorted(
            [biz_refresh, user_refresh, cta_refresh],
            key=lambda refresh: refresh.created_at
        )

    @pytest.fixture
    def sorted_topics(self, user_topic_1, biz_topic, user_topic_2, cta_topic):
        return sorted(
            [user_topic_1, biz_topic, user_topic_2, cta_topic],
            key=lambda topic: topic.created_at
        )

    def test_get_refreshes_after_given_timestamp(self, sorted_refreshes):
        expected = sorted_refreshes[1:]
        after_dt = expected[0].created_at

        actual = schema_repo.get_refreshes_by_criteria(created_after=after_dt)
        assert all(refresh.created_at >= after_dt for refresh in actual)

    def test_no_newer_refresh(self, sorted_refreshes):
        last_refresh = sorted_refreshes[-1]
        after_dt = last_refresh.created_at + 1
        actual = schema_repo.get_refreshes_by_criteria(created_after=after_dt)
        assert actual == []

    def test_refresh_get_yelp_namespace_only(
        self,
        biz_refresh,
        user_refresh,
        yelp_namespace
    ):
        self.assert_equal_refreshes(
            actual_refreshes=schema_repo.get_refreshes_by_criteria(
                namespace=yelp_namespace.name
            ),
            expected_refreshes=self._sort_refreshes_by_id(
                [biz_refresh, user_refresh]
            )
        )

    def test_refresh_get_biz_source_only(
        self,
        biz_refresh,
        biz_source
    ):
        self.assert_equal_refreshes(
            actual_refreshes=schema_repo.get_refreshes_by_criteria(
                source_name=biz_source.name
            ),
            expected_refreshes=[biz_refresh]
        )

    def test_get_by_refresh_status_only(
        self,
        biz_refresh,
        user_refresh,
        cta_refresh
    ):
        self.assert_equal_refreshes(
            actual_refreshes=schema_repo.get_refreshes_by_criteria(
                status='NOT_STARTED'
            ),
            expected_refreshes=self._sort_refreshes_by_id(
                [biz_refresh, user_refresh, cta_refresh]
            )
        )

    def assert_equal_refreshes(self, expected_refreshes, actual_refreshes):
        assert len(actual_refreshes) == len(expected_refreshes)
        for i, actual_refresh in enumerate(actual_refreshes):
            assert actual_refresh == expected_refreshes[i]

    def _sort_refreshes_by_id(self, refreshes):
        return sorted(refreshes, key=lambda refresh: refresh.id)


class TestGetTopicsByCriteria(DBTestCase):

    @pytest.fixture
    def topic_1_in_foo_bar(self):
        return factories.create_topic(
            topic_name='topic_1',
            namespace_name='foo',
            source_name='bar'
        )

    @pytest.fixture
    def topic_2_in_foo_baz(self, topic_1_in_foo_bar):
        # reference topic_foo_bar fixture to make sure it's created first.
        return factories.create_topic(
            topic_name='topic_2',
            namespace_name='foo',
            source_name='baz',
            created_at=topic_1_in_foo_bar.created_at + 1,
            updated_at=topic_1_in_foo_bar.updated_at + 1
        )

    @pytest.fixture
    def topic_3_in_abc_bar(self, topic_2_in_foo_baz):
        return factories.create_topic(
            topic_name='topic_3',
            namespace_name='abc',
            source_name='bar',
            created_at=topic_2_in_foo_baz.created_at + 1,
            updated_at=topic_2_in_foo_baz.updated_at + 1
        )

    def test_get_all_topics(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria()
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[
                topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
            ],
            assert_func=asserts.assert_equal_topic
        )

    def test_get_topics_after_given_timestamp(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            created_after=topic_2_in_foo_baz.created_at
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_2_in_foo_baz, topic_3_in_abc_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_no_newer_topic(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            created_after=topic_3_in_abc_bar.created_at + 1
        )
        assert actual == []

    def test_filter_topics_by_source(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(source='bar')
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_1_in_foo_bar, topic_3_in_abc_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_filter_topics_by_namespace(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(namespace='foo')
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_1_in_foo_bar, topic_2_in_foo_baz],
            assert_func=asserts.assert_equal_topic
        )

    def test_filter_topics_by_namespace_and_source(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            namespace='foo',
            source='bar'
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_1_in_foo_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_nonexistent_namespace_or_source(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(namespace='missing')
        assert actual == []

        actual = schema_repo.get_topics_by_criteria(source='missing')
        assert actual == []

        actual = schema_repo.get_topics_by_criteria(
            namespace='foo',
            source='missing'
        )
        assert actual == []

    def test_get_only_one_topic(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            page_info=PageInfo(count=1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_1_in_foo_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_get_topics_with_id_greater_than_min_id(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            page_info=PageInfo(min_id=topic_2_in_foo_baz.id)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_2_in_foo_baz, topic_3_in_abc_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_min_id_exceeds_topic_max_id(
        self, topic_1_in_foo_bar, topic_2_in_foo_baz, topic_3_in_abc_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            page_info=PageInfo(min_id=topic_3_in_abc_bar.id + 1)
        )
        assert actual == []


class TestGetSchemasByCriteria(DBTestCase):

    @pytest.fixture
    def schema_1_in_foo_bar(self):
        return factories.create_avro_schema(
            schema_json={"type": "enum", "symbols": ["a"], "name": "enum1"},
            namespace='foo',
            source='bar',
            topic_name='topic_1'
        )

    @pytest.fixture
    def schema_2_in_abc_bar(self, schema_1_in_foo_bar):
        return factories.create_avro_schema(
            schema_json={"type": "enum", "symbols": ["a"], "name": "enum2"},
            namespace='abc',
            source='bar',
            topic_name='topic_2',
            created_at=schema_1_in_foo_bar.created_at + 1,
            updated_at=schema_1_in_foo_bar.updated_at + 1
        )

    @pytest.fixture
    def schema_3_in_foo_baz(self, schema_2_in_abc_bar):
        return factories.create_avro_schema(
            schema_json={"type": "enum", "symbols": ["a"], "name": "enum3"},
            namespace='foo',
            source='baz',
            topic_name='topic_3',
            created_at=schema_2_in_abc_bar.created_at + 1,
            updated_at=schema_2_in_abc_bar.updated_at + 1
        )

    @pytest.fixture
    def disabled_schema_4_in_foo_baz(self, schema_3_in_foo_baz):
        return factories.create_avro_schema(
            schema_json={"type": "enum", "symbols": ["a"], "name": "enum4"},
            namespace='foo',
            source='bar',
            topic_name='topic_4',
            status=models.AvroSchemaStatus.DISABLED,
            created_at=schema_3_in_foo_baz.created_at + 1,
            updated_at=schema_3_in_foo_baz.updated_at + 1
        )

    def test_get_schemas_by_namespace_name(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(namespace_name='foo')
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_1_in_foo_bar, schema_3_in_foo_baz],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_by_source_name(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(source_name='bar')
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_1_in_foo_bar, schema_2_in_abc_bar],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_by_namespace_and_source_name(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            namespace_name='foo',
            source_name='bar'
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_1_in_foo_bar],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_created_after_given_timestamp(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            created_after=schema_2_in_abc_bar.created_at
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_2_in_abc_bar, schema_3_in_foo_baz],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_including_disabled_ones(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(include_disabled=True)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_1_in_foo_bar, schema_2_in_abc_bar,
                           schema_3_in_foo_baz, disabled_schema_4_in_foo_baz],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_with_id_greater_than_min_id(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            page_info=PageInfo(min_id=schema_2_in_abc_bar.id)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_2_in_abc_bar, schema_3_in_foo_baz],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_only_max_number_of_schemas(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            page_info=PageInfo(count=1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[schema_1_in_foo_bar],
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_with_nonexistant_namespace_or_source(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(namespace_name='dummy')
        assert actual == []

        actual = schema_repo.get_schemas_by_criteria(
            namespace_name='foo',
            source_name="this_source_does_not_exist"
        )
        assert actual == []

    def test_get_no_more_newer_schemas(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            created_after=disabled_schema_4_in_foo_baz.created_at + 1
        )
        assert actual == []

    def test_min_id_exceeds_schema_max_id(
        self,
        schema_1_in_foo_bar,
        schema_2_in_abc_bar,
        schema_3_in_foo_baz,
        disabled_schema_4_in_foo_baz
    ):
        actual = schema_repo.get_schemas_by_criteria(
            page_info=PageInfo(min_id=disabled_schema_4_in_foo_baz.id + 1)
        )
        assert actual == []


class TestGetLatestTopicBySourceId(DBTestCase):

    @pytest.fixture
    def source(self):
        return factories.create_source(namespace_name='foo', source_name='bar')

    @pytest.fixture
    def topics_in_source(self, source):
        return [
            factories.create_topic(
                topic_name='topic_1',
                namespace_name=source.namespace.name,
                source_name=source.name
            ),
            factories.create_topic(
                topic_name='topic_2',
                namespace_name=source.namespace.name,
                source_name=source.name
            ),
        ]

    def test_get_latest_topic_of_source_id(self, topics_in_source, source):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        asserts.assert_equal_topic(actual, expected=topics_in_source[-1])

    def test_if_source_has_no_topic(self, source):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        assert actual is None

    def test_get_latest_topic_with_bad_source_id(self):
        actual = schema_repo.get_latest_topic_of_source_id(0)
        assert actual is None
