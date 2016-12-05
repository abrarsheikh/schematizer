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

import copy
import datetime
import time
from collections import defaultdict

import mock
import pytest

from schematizer import models
from schematizer.components import converters
from schematizer.logic import exceptions as sch_exc
from schematizer.logic import schema_repository as schema_repo
from schematizer.models import Namespace
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.page_info import PageInfo
from schematizer.models.schema_meta_attribute_mapping import (
    SchemaMetaAttributeMapping)
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.logic.meta_attribute_mappers_test import GetMetaAttributeBaseTest
from tests.models.testing_db import DBTestCase


class TestSchemaRepository(DBTestCase):

    @property
    def namespace_name(self):
        return 'foo'

    @property
    def transformed_namespace_name(self):
        return 'foo_transformed'

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

    @property
    def topic_name(self):
        return 'topic_one'

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            topic_name=self.topic_name,
            namespace_name=self.namespace_name,
            source_name=self.source_name,
            created_at=self.some_datetime + datetime.timedelta(seconds=1)
        )

    @property
    def transformed_topic_name(self):
        return 'topic_one_transformed'

    @pytest.fixture
    def transformed_topic(self):
        return factories.create_topic(
            self.transformed_topic_name,
            self.transformed_namespace_name,
            self.source_name
        )

    @property
    def source_id(self):
        return factories.fake_default_id

    @property
    def offset(self):
        return 0

    @property
    def batch_size(self):
        return 100

    @property
    def priority(self):
        return 50

    @property
    def filter_condition(self):
        return 'user=test_user'

    @property
    def avg_rows_per_second_cap(self):
        return 1000

    @property
    def status(self):
        return 'SUCCESS'

    @property
    def status_value(self):
        return 3

    @pytest.fixture
    def refresh(self, source):
        return factories.create_refresh(
            source_id=source.id,
            offset=self.offset,
            batch_size=self.batch_size,
            priority=self.priority,
            filter_condition=self.filter_condition,
            avg_rows_per_second_cap=self.avg_rows_per_second_cap
        )

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

    @pytest.fixture
    def sorted_schemas(self, rw_schema, another_rw_schema, user_schema):
        return sorted(
            [rw_schema, another_rw_schema, user_schema],
            key=lambda schema: schema.id
        )

    @property
    def rw_transformed_schema_json(self):
        schema_json = copy.deepcopy(self.rw_schema_json)
        schema_json['namespace'] = self.transformed_namespace_name
        schema_json['fields'].append(
            {"name": "bar_str", "type": "string", "doc": "bar_str"}
        )
        return schema_json

    @property
    def rw_transformed_schema_elements(self):
        return self._build_elements(self.rw_transformed_schema_json)

    @pytest.fixture
    def rw_transformed_schema(self, transformed_topic, rw_schema):
        return factories.create_avro_schema(
            self.rw_transformed_schema_json,
            self.rw_transformed_schema_elements,
            topic_name=transformed_topic.name,
            base_schema_id=rw_schema.id
        )

    @property
    def rw_transformed_schema_v2_json(self):
        schema_json = copy.deepcopy(self.rw_transformed_schema_json)
        schema_json['fields'].append(
            {
                "name": "bar_double",
                "type": "double",
                "doc": "bar_double",
                "default": 0.0
            }
        )
        return schema_json

    @property
    def rw_transformed_schema_v2_elements(self):
        return self._build_elements(self.rw_transformed_schema_v2_json)

    @pytest.fixture
    def rw_transformed_v2_schema(self, transformed_topic, rw_schema):
        """Represents an upgrade to the ASTs (v2) which produces a different
        (but compatible) transformed schema from the same base
        """
        return factories.create_avro_schema(
            self.rw_transformed_schema_v2_json,
            self.rw_transformed_schema_v2_elements,
            topic_name=transformed_topic.name,
            base_schema_id=rw_schema.id
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
    def another_rw_transformed_schema_json(self):
        schema_json = copy.deepcopy(self.another_rw_schema_json)
        schema_json['namespace'] = self.transformed_namespace_name
        schema_json['fields'].append(
            {"name": "baz_str", "type": "string", "doc": "baz_str"}
        )
        return schema_json

    @property
    def another_rw_transformed_schema_elements(self):
        return self._build_elements(self.another_rw_transformed_schema_json)

    @pytest.fixture
    def another_rw_transformed_schema(
            self,
            transformed_topic,
            another_rw_schema
    ):
        return factories.create_avro_schema(
            self.another_rw_transformed_schema_json,
            self.another_rw_transformed_schema_elements,
            topic_name=transformed_topic.name,
            base_schema_id=another_rw_schema.id
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

    @property
    def pkey_schema_json(self):
        return {
            "type": "record",
            "name": "table_pkey",
            "namespace": self.namespace_name,
            "fields": [
                {
                    "name": "field_1",
                    "type": "int",
                    "doc": "field_1",
                    "pkey": 1
                },
                {
                    "name": "field_2",
                    "type": "int",
                    "doc": "field_2",
                    "pkey": 2
                },
            ],
            "doc": "I have a pkey!"
        }

    @property
    def added_pkey_schema_json(self):
        return {
            "type": "record",
            "name": "table_pkey",
            "namespace": self.namespace_name,
            "fields": [
                {
                    "name": "field_1",
                    "type": "int",
                    "doc": "field_1",
                    "pkey": 1
                },
                {
                    "name": "field_2",
                    "type": "int",
                    "doc": "field_2",
                    "pkey": 2
                },
                {
                    "name": "field_3",
                    "type": "int",
                    "doc": "field_3",
                    "pkey": 3
                }
            ],
            "doc": "I have a pkey!"
        }

    @property
    def yet_another_pkey_schema_json(self):
        return {
            "type": "record",
            "name": "table_pkey",
            "namespace": self.namespace_name,
            "fields": [
                {
                    "name": "field_1",
                    "type": "int",
                    "doc": "field_1",
                    "pkey": 1
                },
                {
                    "name": "field_3",
                    "type": "int",
                    "doc": "field_3",
                    "pkey": 2
                }
            ],
            "doc": "I have a pkey!"
        }

    @property
    def another_pkey_schema_json(self):
        return {
            "type": "record",
            "name": "table_pkey",
            "namespace": self.namespace_name,
            "fields": [
                {
                    "name": "field_1",
                    "type": "int",
                    "doc": "field_1",
                    "pkey": 1
                },
                {
                    "name": "field_2",
                    "type": "int",
                    "doc": "field_2",
                    "pkey": 2
                },
                {
                    "name": "field_new",
                    "type": "int",
                    "doc": "field_new",
                    "default": 123
                }
            ],
            "doc": "I have a pkey!"
        }

    @pytest.yield_fixture
    def mock_compatible_func(self):
        with mock.patch(
            'schematizer.logic.schema_repository.'
            'SchemaCompatibilityValidator.is_backward_compatible'
        ) as mock_func:
            yield mock_func

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

    def test_registering_from_avro_json_with_new_schema(self, namespace):
        expected_base_schema_id = 100
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.rw_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type,
            base_schema_id=expected_base_schema_id
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            base_schema_id=expected_base_schema_id,
            avro_schema_elements=self.rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)

        actual_source = session.query(models.Source).filter(
            models.Source.id == actual_schema.topic.source.id
        ).one()
        expected_source = models.Source(
            namespace_id=namespace.id,
            name=self.source_name,
            owner_email=self.source_owner_email
        )
        self.assert_equal_source_partial(expected_source, actual_source)

    def test_register_schema_after_meta_attribute_mapping_changes(
        self,
        namespace,
        source,
        meta_attr_schema
    ):
        """ This test registers a schema json and then registers a namespace
        meta attribute mapping. Then it registers the same schema json again
        and verifies that a new schema gets registered in a new topic and meta
        attributes are enforced on the schema..
        """
        schema1 = schema_repo.register_avro_schema_from_avro_json(
            self.rw_schema_json,
            namespace.name,
            source.name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            models.Namespace.__name__,
            namespace.id
        )

        schema2 = schema_repo.register_avro_schema_from_avro_json(
            self.rw_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        actual = schema_repo.get_meta_attributes_by_schema_id(schema2.id)
        expected = [meta_attr_schema.id]

        assert schema1.id != schema2.id
        assert schema1.topic_id != schema2.topic_id
        assert actual == expected

    @pytest.mark.parametrize("cluster_type", ['datapipe', 'scribe'])
    def test_registering_from_avro_json_with_cluster_types(self, cluster_type):
        expected_base_schema_id = 100
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.rw_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=cluster_type,
            base_schema_id=expected_base_schema_id
        )
        actual_topic = session.query(models.Topic).filter(
            models.Topic.id == actual_schema.topic.id
        ).one()
        expected_topic = models.Topic(
            name=actual_schema.topic.name,
            source_id=actual_schema.topic.source_id,
            contains_pii=actual_schema.topic.contains_pii,
            cluster_type=cluster_type
        )
        self.assert_equal_topic_partial(expected_topic, actual_topic)

    def test_registering_from_avro_json_without_cluster_type(self):
        expected_base_schema_id = 100
        with pytest.raises(TypeError):
            schema_repo.register_avro_schema_from_avro_json(
                self.rw_schema_json,
                self.namespace_name,
                self.source_name,
                self.source_owner_email,
                contains_pii=False,
                base_schema_id=expected_base_schema_id
            )

    def test_registering_from_avro_json_with_pkey_added(self):
        actual_schema1 = schema_repo.register_avro_schema_from_avro_json(
            self.pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )

        actual_schema2 = schema_repo.register_avro_schema_from_avro_json(
            self.added_pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        assert actual_schema1.topic.id != actual_schema2.topic.id

    def test_registering_from_avro_json_with_pkey_changed(self):
        actual_schema1 = schema_repo.register_avro_schema_from_avro_json(
            self.pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )

        actual_schema2 = schema_repo.register_avro_schema_from_avro_json(
            self.yet_another_pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        assert actual_schema1.topic.id != actual_schema2.topic.id

    def test_registering_from_avro_json_with_pkey_unchanged(self):
        actual_schema1 = schema_repo.register_avro_schema_from_avro_json(
            self.pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )

        actual_schema2 = schema_repo.register_avro_schema_from_avro_json(
            self.another_pkey_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        assert actual_schema1.topic.id == actual_schema2.topic.id

    @pytest.mark.usefixtures('rw_schema')
    def test_registering_from_avro_json_with_compatible_schema(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True

        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.another_rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.another_rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)
        assert topic.id == actual_schema.topic_id

    @pytest.mark.parametrize("email", [(None), (' ')])
    def test_register_invalid_schema_email(
        self,
        email,
        rw_transformed_schema
    ):
        avro_schema = rw_transformed_schema
        with pytest.raises(ValueError) as e:
            schema_repo.register_avro_schema_from_avro_json(
                avro_schema.avro_schema_json,
                avro_schema.topic.source.namespace.name,
                avro_schema.topic.source.name,
                email,
                contains_pii=False,
                cluster_type=self.cluster_type,
                base_schema_id=avro_schema.base_schema_id
            )
        assert str(e.value) == "Source owner email must be non-empty."

    @pytest.mark.parametrize("src_name", [(None), (' ')])
    def test_register_invalid_schema_src_name(
        self,
        src_name,
        rw_transformed_schema
    ):
        avro_schema = rw_transformed_schema
        with pytest.raises(ValueError) as e:
            schema_repo.register_avro_schema_from_avro_json(
                avro_schema.avro_schema_json,
                avro_schema.topic.source.namespace.name,
                src_name,
                avro_schema.topic.source.owner_email,
                contains_pii=False,
                cluster_type=self.cluster_type,
                base_schema_id=avro_schema.base_schema_id
            )
        assert str(e.value) == "Source name must be non-empty."

    def assert_new_topic_created_after_schema_register(
        self,
        topic,
        contains_pii,
        cluster_type
    ):
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii,
            cluster_type
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.another_rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.another_rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)

        # new topic should be created
        assert topic.id != actual_schema.topic_id
        assert topic.name != actual_schema.topic.name

        # the new topic should still be under the same source
        assert topic.source_id == actual_schema.topic.source_id

    @pytest.fixture(params=[{
        "name": "foo",
        "doc": "test_doc",
        "type": "record",
        "namespace": "test_namespace",
        "fields": [
            {"type": "int", "name": "col", "doc": "test_doc"}
        ]},
        {"name": "color", "doc": "test_d", "type": "enum", "symbols": ["red"]}
    ])
    def avro_schema_with_docs(self, request):
        return request.param

    def test_register_avro_schema_with_docs_require_doc(
        self,
        topic,
        avro_schema_with_docs
    ):
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            avro_schema_with_docs,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type,
            docs_required=True
        )
        assert actual_schema.avro_schema_json == avro_schema_with_docs

    def test_register_avro_schema_with_docs_dont_require_doc(
        self,
        topic,
        avro_schema_with_docs
    ):
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            avro_schema_with_docs,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type,
            docs_required=False
        )
        assert actual_schema.avro_schema_json == avro_schema_with_docs

    @pytest.fixture(params=[{
        "name": "foo",
        "doc": " ",
        "type": "record",
        "namespace": "test_namespace",
        "fields": [
            {"type": "int", "name": "col"}
        ]},
        {"name": "color", "type": "enum", "symbols": ["red"]}
    ])
    def avro_schema_without_docs(self, request):
        return request.param

    def test_register_avro_schema_without_docs_require_doc(
        self,
        topic,
        avro_schema_without_docs
    ):
        with pytest.raises(ValueError):
            schema_repo.register_avro_schema_from_avro_json(
                avro_schema_without_docs,
                topic.source.namespace.name,
                topic.source.name,
                topic.source.owner_email,
                contains_pii=False,
                cluster_type=self.cluster_type
            )

    def test_register_avro_schema_without_docs_dont_require_doc(
        self,
        topic,
        avro_schema_without_docs
    ):
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            avro_schema_without_docs,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type,
            docs_required=False
        )
        assert actual_schema.avro_schema_json == avro_schema_without_docs

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_incompatible_schema(
        self,
        topic,
        mock_compatible_func
    ):
        mock_compatible_func.return_value = False
        self.assert_new_topic_created_after_schema_register(
            topic=topic,
            contains_pii=topic.contains_pii,
            cluster_type=topic.cluster_type
        )

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_different_pii(
        self,
        topic,
        mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        self.assert_new_topic_created_after_schema_register(
            topic=topic,
            contains_pii=not topic.contains_pii,
            cluster_type=topic.cluster_type
        )

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_different_cluster_type(
        self,
        topic,
        mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        assert topic.cluster_type != 'scribe'
        self.assert_new_topic_created_after_schema_register(
            topic=topic,
            contains_pii=topic.contains_pii,
            cluster_type='scribe'
        )

    def _register_avro_schema(self, avro_schema):
        return schema_repo.register_avro_schema_from_avro_json(
            avro_schema.avro_schema_json,
            avro_schema.topic.source.namespace.name,
            avro_schema.topic.source.name,
            avro_schema.topic.source.owner_email,
            contains_pii=avro_schema.topic.contains_pii,
            cluster_type=avro_schema.topic.cluster_type,
            base_schema_id=avro_schema.base_schema_id
        )

    def test_registering_from_avro_json_with_same_schema(
        self,
        rw_schema,
        mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        actual = self._register_avro_schema(rw_schema)
        assert rw_schema.id == actual.id

    def test_registering_same_transformed_schema_is_same(
        self,
        rw_transformed_schema
    ):
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        self.assert_equal_avro_schema_partial(result_a1, result_a2)

    def test_add_different_transformed_schemas_with_same_base_schema(
        self,
        rw_transformed_schema,
        another_rw_transformed_schema
    ):
        # Registering a different transformed schema should result in a
        # different schema/topic
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_b = self._register_avro_schema(another_rw_transformed_schema)
        assert result_a1.id != result_b.id
        assert result_a1.topic.id != result_b.topic.id
        assert result_a1.base_schema_id != result_b.base_schema_id

        # Re-registering the original transformed schema will should
        # result in the original's schema/topic
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        assert result_a1.id == result_a2.id
        assert result_a1.topic.id == result_a2.topic.id
        assert result_a1.base_schema_id == result_a2.base_schema_id

    def test_reregistering_compatible_transformed_schema_stays_in_topic(
        self,
        rw_transformed_schema,
        rw_transformed_v2_schema
    ):
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_b = self._register_avro_schema(rw_transformed_v2_schema)
        assert result_a1.id != result_b.id
        assert result_a1.topic.id == result_b.topic.id
        assert result_a1.base_schema_id == result_b.base_schema_id
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        assert result_a1.id != result_a2.id
        assert result_a1.topic.id == result_a2.topic.id
        assert result_a1.base_schema_id == result_a2.base_schema_id

    def test_registering_same_schema_twice(
        self,
        topic,
        rw_schema
    ):
        result_a = self._register_avro_schema(rw_schema)
        result_b = self._register_avro_schema(rw_schema)

        # new schema should be created for the same topic
        assert rw_schema.id == result_a.id
        assert topic.id == result_a.topic_id
        assert rw_schema.id == result_b.id
        assert topic.id == result_b.topic_id
        expected = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.rw_schema_elements,
            base_schema_id=rw_schema.base_schema_id
        )
        self.assert_equal_avro_schema_partial(expected, result_a)
        self.assert_equal_avro_schema_partial(expected, result_b)

    def test_registering_from_avro_json_with_diff_base_schema(
        self,
        topic,
        rw_schema,
        mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        expected_base_schema_id = 100

        actual = schema_repo.register_avro_schema_from_avro_json(
            rw_schema.avro_schema_json,
            rw_schema.topic.source.namespace.name,
            rw_schema.topic.source.name,
            rw_schema.topic.source.owner_email,
            contains_pii=False,
            cluster_type=self.cluster_type,
            base_schema_id=expected_base_schema_id
        )

        # new schema should be created for a new topic
        assert rw_schema.id != actual.id
        assert topic.id != actual.topic_id
        expected = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.rw_schema_elements,
            base_schema_id=expected_base_schema_id
        )
        self.assert_equal_avro_schema_partial(expected, actual)

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
        self.assert_equal_topic(topic, actual)
        new_topic = factories.create_topic(
            topic_name='new_topic',
            namespace_name=source.namespace.name,
            source_name=source.name
        )
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        self.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_source_id(self, source, topic):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.assert_equal_topic(topic, actual)

        new_topic = factories.create_topic(
            topic_name='new_topic',
            namespace_name=source.namespace.name,
            source_name=source.name
        )
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.assert_equal_topic(new_topic, actual)

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
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_latest_topic_of_namespace_source('foo', 'bar')

    def test_get_latest_topic_of_source_id_with_no_topic(self, source):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        assert actual is None

    def test_get_latest_topic_of_source_id_with_nonexistent_source(self):
        actual = schema_repo.get_latest_topic_of_source_id(0)
        assert actual is None

    @pytest.mark.usefixtures('source', 'rw_schema', 'disabled_schema')
    @pytest.mark.parametrize(
        "is_compatible, meta_attributes_for_schema_id, "
        "meta_attributes_for_source, expected_output", [
            (True, [10, 20], [10, 20], True),
            (True, [10, 20], [10, 30], False),
            (False, [10, 20], [10, 20], False),
            (False, [10, 20], [10, 30], False),
        ])
    def test_is_schema_compatible_in_topic(
        self,
        topic,
        mock_compatible_func,
        is_compatible,
        meta_attributes_for_schema_id,
        meta_attributes_for_source,
        expected_output
    ):
        with mock.patch(
            'schematizer.logic.schema_repository.'
            'meta_attr_repo.get_meta_attributes_by_source',
            return_value=meta_attributes_for_source
        ), mock.patch(
            'schematizer.logic.schema_repository.'
            'get_meta_attributes_by_schema_id',
            return_value=meta_attributes_for_schema_id
        ):
            mock_compatible_func.return_value = is_compatible

            actual = schema_repo.is_schema_compatible_in_topic(
                self.rw_schema_json,
                topic
            )
            assert actual == expected_output

    @pytest.mark.usefixtures('disabled_schema')
    def test_is_schema_compatible_in_topic_with_no_enabled_schema(self, topic):
        actual = schema_repo.is_schema_compatible_in_topic('int', topic)
        assert actual is True

    def test_get_topic_by_name(self, topic):
        actual = schema_repo.get_topic_by_name(self.topic_name)
        self.assert_equal_topic(topic, actual)

    def test_get_topic_by_name_with_nonexistent_topic(self):
        actual = schema_repo.get_topic_by_name('foo')
        assert actual is None

    def test_get_source_by_fullname(self, source):
        actual = schema_repo.get_source_by_fullname(
            self.namespace_name,
            self.source_name
        )
        self.assert_equal_source(source, actual)

    def test_get_source_by_fullname_with_nonexistent_source(self):
        actual = schema_repo.get_source_by_fullname('foo', 'bar')
        assert actual is None

    def test_get_schema_by_id(self, rw_schema):
        actual = schema_repo.get_schema_by_id(rw_schema.id)
        self.assert_equal_avro_schema(rw_schema, actual)

    def test_get_schema_by_id_with_nonexistent_schema(self):
        actual = schema_repo.get_schema_by_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        self.assert_equal_avro_schema(rw_schema, actual)

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
        self.assert_equal_avro_schema(rw_schema, actual)

    def test_get_latest_schema_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_latest_schema_by_topic_name('_bad.topic')

    @pytest.mark.usefixtures('rw_schema', 'disabled_schema')
    @pytest.mark.parametrize("is_compatible", [True, False])
    def test_is_schema_compatible(self, mock_compatible_func, is_compatible):
        mock_compatible_func.return_value = is_compatible
        target_schema = 'avro schema to be validated'
        actual = schema_repo.is_schema_compatible(
            target_schema,
            self.namespace_name,
            self.source_name
        )
        expected = mock_compatible_func.return_value
        assert expected == actual

    def test_is_schema_compatible_with_nonexistent_source(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.is_schema_compatible('avro schema', 'foo', 'bar')

    def test_get_schemas_by_topic_name(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_name(topic.name)
        assert 1 == len(actual)
        self.assert_equal_avro_schema(rw_schema, actual[0])

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
            assert_func=self.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_schemas_by_topic_name('foo')

    def test_get_schemas_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_id(topic.id)
        assert 1 == len(actual)
        self.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_after_given_timestamp_excluding_disabled_schemas(
        self,
        sorted_schemas,
        disabled_schema
    ):
        expected = sorted_schemas[1:]
        after_dt = expected[0].created_at
        actual = schema_repo.get_schemas_created_after(created_after=after_dt)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_after_given_timestamp_including_disabled_schemas(
        self,
        rw_schema,
        another_rw_schema,
        user_schema,
        disabled_schema
    ):
        expected = sorted(
            [rw_schema, disabled_schema, another_rw_schema, user_schema],
            key=lambda schema: schema.id
        )
        after_dt = expected[0].created_at
        actual = schema_repo.get_schemas_created_after(
            created_after=after_dt,
            include_disabled=True
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_filter_by_count(
        self,
        sorted_schemas
    ):
        expected = [sorted_schemas[1]]
        after_dt = expected[0].created_at
        actual = schema_repo.get_schemas_created_after(
            created_after=after_dt,
            page_info=PageInfo(count=1, min_id=None)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_get_schemas_filter_by_min_id(self, sorted_schemas):
        min_id = sorted_schemas[1].id
        created_dt = sorted_schemas[0].created_at
        expected = [schema for schema in sorted_schemas if schema.id >= min_id]
        actual = schema_repo.get_schemas_created_after(
            created_after=created_dt,
            page_info=PageInfo(count=None, min_id=min_id)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_avro_schema
        )

    def test_no_newer_schema(self, sorted_schemas):
        last_schema = sorted_schemas[-1]
        after_dt = last_schema.created_at + datetime.timedelta(seconds=1)

        actual = schema_repo.get_schemas_created_after(created_after=after_dt)
        assert actual == []

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
            assert_func=self.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_id_with_nonexistent_topic(self):
        actual = schema_repo.get_schemas_by_topic_id(0)
        assert [] == actual

    def test_get_schemas_by_namespace_name(
        self,
        rw_schema
    ):
        actual = schema_repo.get_schemas_by_criteria(self.namespace_name)
        assert len(actual) == 1
        self.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_by_namespace_and_source_name(
        self,
        rw_schema
    ):
        actual = schema_repo.get_schemas_by_criteria(
            self.namespace_name,
            source_name=self.source_name
        )
        assert len(actual) == 1
        self.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_by_namespace_and_nonexistant_source_name(self):
        actual = schema_repo.get_schemas_by_criteria(
            self.namespace_name,
            source_name="this_source_does_not_exist"
        )
        assert not actual

    def test_get_schemas_by_nonexistant_namespace(self):
        actual = schema_repo.get_schemas_by_criteria(
            "this_namespace_doesnt_exist"
        )
        assert not actual

    def test_mark_schema_disabled(self, rw_schema):
        schema_repo.mark_schema_disabled(rw_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.DISABLED == actual.status

    def test_mark_schema_disabled_with_nonexisted_schema(self, rw_schema):
        # nothing should happen
        schema_repo.mark_schema_disabled(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_mark_schema_readonly(self, rw_schema):
        schema_repo.mark_schema_readonly(rw_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_ONLY == actual.status

    def test_mark_schema_readonly_with_nonexisted_schema(self, rw_schema):
        # nothing should happen
        schema_repo.mark_schema_readonly(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_get_topics_by_source_id(self, source, topic):
        actual = schema_repo.get_topics_by_source_id(source.id)
        assert 1 == len(actual)
        self.assert_equal_topic(topic, actual[0])

    def test_available_converters(self):
        expected = {
            (models.SchemaKindEnum.MySQL, models.SchemaKindEnum.Avro):
            converters.MySQLToAvroConverter,
            (models.SchemaKindEnum.Avro, models.SchemaKindEnum.Redshift):
            converters.AvroToRedshiftConverter
        }
        for key, value in expected.iteritems():
            actual = schema_repo.converters[key]
            source_type, target_type = key
            assert source_type == actual.source_type
            assert target_type == actual.target_type
            assert value == actual

    def test_convert_schema(self):
        with mock.patch.object(
            converters.MySQLToAvroConverter,
            'convert'
        ) as mock_converter:
            schema_repo.convert_schema(
                models.SchemaKindEnum.MySQL,
                models.SchemaKindEnum.Avro,
                self.rw_schema_json
            )
            mock_converter.assert_called_once_with(self.rw_schema_json)

    def test_convert_schema_with_no_suitable_converter(self):
        with pytest.raises(Exception):
            schema_repo.convert_schema(
                mock.Mock(),
                mock.Mock(),
                self.rw_schema_json
            )

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
        actual_refresh = schema_repo.create_refresh(
            self.source_id,
            self.offset,
            self.batch_size,
            self.priority,
            self.filter_condition,
            self.avg_rows_per_second_cap
        )
        expected_refresh = models.Refresh(
            source_id=self.source_id,
            status=models.RefreshStatus.NOT_STARTED.value,
            offset=self.offset,
            batch_size=self.batch_size,
            priority=self.priority,
            filter_condition=self.filter_condition,
            avg_rows_per_second_cap=self.avg_rows_per_second_cap
        )
        self.assert_equal_refresh_partial(expected_refresh, actual_refresh)

    def test_get_refresh_by_id(self, refresh):
        actual_refresh = schema_repo.get_refresh_by_id(refresh.id)
        self.assert_equal_refresh(refresh, actual_refresh)

    def test_update_refresh(self, refresh):
        new_status = models.RefreshStatus.IN_PROGRESS
        new_offset = 500
        schema_repo.update_refresh(
            refresh.id,
            new_status.name,
            new_offset
        )
        actual_refresh = schema_repo.get_refresh_by_id(refresh.id)
        expected_refresh = models.Refresh(
            source_id=refresh.source_id,
            status=new_status.value,
            offset=new_offset,
            batch_size=refresh.batch_size,
            priority=refresh.priority,
            filter_condition=refresh.filter_condition
        )
        self.assert_equal_refresh_partial(expected_refresh, actual_refresh)

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

    def assert_equal_namespace(self, expected, actual):
        assert expected.id == actual.id
        assert expected.name == actual.name
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def assert_equal_source_partial(self, expected, actual):
        assert expected.namespace_id == actual.namespace_id
        assert expected.name == actual.name
        assert expected.owner_email == actual.owner_email

    def assert_equal_source(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_source_partial(expected, actual)

    def assert_equal_topic_partial(self, expected, actual):
        assert expected.name == actual.name
        assert expected.cluster_type == actual.cluster_type

    def assert_equal_topic(self, expected, actual):
        assert expected.id == actual.id
        assert expected.source_id == actual.source_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_topic_partial(expected, actual)

    def assert_equal_avro_schema_partial(self, expected, actual):
        assert expected.avro_schema == actual.avro_schema
        assert expected.base_schema_id == actual.base_schema_id
        assert expected.status == actual.status
        self.assert_equal_entities(
            expected.avro_schema_elements,
            actual.avro_schema_elements,
            self.assert_equal_avro_schema_element_partial,
            filter_key='key'
        )

    def assert_equal_avro_schema(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema == actual.avro_schema
        assert expected.topic_id == actual.topic_id
        assert expected.base_schema_id == actual.base_schema_id
        assert expected.status == actual.status
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_entities(
            expected.avro_schema_elements,
            actual.avro_schema_elements,
            self.assert_equal_avro_schema_element
        )

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


@pytest.mark.usefixtures('sorted_topics', 'sorted_refreshes')
class TestByCriteria(DBTestCase):

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

    @property
    def avg_rows_per_second_cap(self):
        return 1000

    @property
    def priority(self):
        return 50

    @pytest.fixture
    def biz_refresh(self, biz_source):
        return factories.create_refresh(
            source_id=biz_source.id,
            offset=factories.fake_offset,
            batch_size=factories.fake_batch_size,
            priority=self.priority,
            filter_condition=factories.fake_filter_condition,
            avg_rows_per_second_cap=self.avg_rows_per_second_cap
        )

    @pytest.fixture
    def user_refresh(self, user_source):
        return factories.create_refresh(
            source_id=user_source.id,
            offset=factories.fake_offset,
            batch_size=factories.fake_batch_size,
            priority=self.priority,
            filter_condition=factories.fake_filter_condition,
            avg_rows_per_second_cap=self.avg_rows_per_second_cap
        )

    @pytest.fixture
    def cta_refresh(self, cta_source):
        return factories.create_refresh(
            source_id=cta_source.id,
            offset=factories.fake_offset,
            batch_size=factories.fake_batch_size,
            priority=self.priority,
            filter_condition=factories.fake_filter_condition,
            avg_rows_per_second_cap=self.avg_rows_per_second_cap
        )

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
        after_dt = last_refresh.created_at + datetime.timedelta(seconds=1)
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

    @property
    def namespace_foo(self):
        return 'foo'

    @property
    def source_bar(self):
        return 'bar'

    @property
    def source_baz(self):
        return 'baz'

    @property
    def namespace_abc(self):
        return 'abc'

    @pytest.fixture
    def topic_foo_bar(self):
        return factories.create_topic(
            topic_name='topic_foo_bar',
            namespace_name=self.namespace_foo,
            source_name=self.source_bar
        )

    @pytest.fixture
    def topic_foo_baz(self, topic_foo_bar):
        # reference topic_foo_bar fixture to make sure it's created first.
        time.sleep(1)
        return factories.create_topic(
            topic_name='topic_foo_baz',
            namespace_name=self.namespace_foo,
            source_name=self.source_baz
        )

    @pytest.fixture
    def topic_abc_bar(self, topic_foo_baz):
        time.sleep(1)
        return factories.create_topic(
            topic_name='topic_abc_bar',
            namespace_name=self.namespace_abc,
            source_name=self.source_bar
        )

    @pytest.fixture(autouse=True)
    def sorted_topics(self, topic_foo_bar, topic_foo_baz, topic_abc_bar):
        return [topic_foo_bar, topic_foo_baz, topic_abc_bar]

    def test_get_all_topics(self, sorted_topics):
        actual = schema_repo.get_topics_by_criteria()
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=sorted_topics,
            assert_func=asserts.assert_equal_topic
        )

    def test_get_topics_after_given_timestamp(self, sorted_topics):
        expected = sorted_topics[1:]
        after_dt = expected[0].created_at

        actual = schema_repo.get_topics_by_criteria(created_after=after_dt)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_topic
        )

    def test_no_newer_topic(self, sorted_topics):
        last_topic = sorted_topics[-1]
        after_dt = last_topic.created_at + datetime.timedelta(seconds=1)
        actual = schema_repo.get_topics_by_criteria(created_after=after_dt)
        assert actual == []

    def test_filter_topics_by_source(self, topic_foo_bar, topic_abc_bar):
        actual = schema_repo.get_topics_by_criteria(source=self.source_bar)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_foo_bar, topic_abc_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_filter_topics_by_namespace(self, topic_foo_bar, topic_foo_baz):
        actual = schema_repo.get_topics_by_criteria(
            namespace=self.namespace_foo
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_foo_bar, topic_foo_baz],
            assert_func=asserts.assert_equal_topic
        )

    def test_filter_topics_by_namespace_and_source(
        self,
        sorted_topics,
        topic_foo_bar
    ):
        actual = schema_repo.get_topics_by_criteria(
            namespace=self.namespace_foo,
            source=self.source_bar
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[topic_foo_bar],
            assert_func=asserts.assert_equal_topic
        )

    def test_get_only_one_topic(self, sorted_topics):
        actual = schema_repo.get_topics_by_criteria(
            page_info=PageInfo(count=1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[sorted_topics[0]],
            assert_func=asserts.assert_equal_topic
        )

    def test_get_topics_with_id_greater_than_min_id(self, sorted_topics):
        expected = sorted_topics[1:]
        min_id = expected[0].id
        actual = schema_repo.get_topics_by_criteria(
            page_info=PageInfo(min_id=min_id)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_topic
        )


@pytest.mark.usefixtures(
    'namespace_meta_attr_mapping',
    'source_meta_attr_mapping',
)
class TestAddToSchemaMetaAttributeMapping(GetMetaAttributeBaseTest):

    @property
    def cluster_type(self):
        return 'datapipe'

    @pytest.fixture
    def sample_schema_json(self):
        return {
            "name": "dummy_schema_for_meta_attr",
            "type": "record",
            "fields": [
                {"name": "id", "type": "int", "doc": "id", "default": 0},
                {"name": "name", "type": "string", "doc": "name"}
            ],
            "doc": "Sample Schema to test MetaAttrMappings"
        }

    def _get_meta_attr_mappings(self, schema_id):
        result = session.query(SchemaMetaAttributeMapping).filter(
            SchemaMetaAttributeMapping.schema_id == schema_id
        ).all()
        mappings_dict = defaultdict(set)
        for m in result:
            mappings_dict[m.schema_id].add(m.meta_attr_schema_id)
        return mappings_dict

    def test_register_same_schema_with_same_meta_attr_mappings(
        self,
        sample_schema_json,
        dummy_src,
        namespace_meta_attr,
        source_meta_attr
    ):
        actual_schema_1 = schema_repo.register_avro_schema_from_avro_json(
            sample_schema_json,
            dummy_src.namespace.name,
            dummy_src.name,
            source_owner_email='dexter@morgan.com',
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        expected = {
            actual_schema_1.id: {
                namespace_meta_attr.id,
                source_meta_attr.id,
            }
        }
        assert self._get_meta_attr_mappings(actual_schema_1.id) == expected
        actual_schema_2 = schema_repo.register_avro_schema_from_avro_json(
            sample_schema_json,
            dummy_src.namespace.name,
            dummy_src.name,
            source_owner_email='dexter@morgan.com',
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        asserts.assert_equal_avro_schema(actual_schema_1, actual_schema_2)

    def test_add_duplicate_mappings(
        self,
        dummy_namespace,
        sample_schema_json,
        dummy_src,
        namespace_meta_attr,
        source_meta_attr,
    ):
        factories.create_meta_attribute_mapping(
            source_meta_attr.id,
            Namespace.__name__,
            dummy_namespace.id
        )
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            sample_schema_json,
            dummy_src.namespace.name,
            dummy_src.name,
            'dexter@morgan.com',
            contains_pii=False,
            cluster_type=self.cluster_type
        )
        expected = {
            actual_schema.id: {
                namespace_meta_attr.id,
                source_meta_attr.id,
            }
        }
        assert expected == self._get_meta_attr_mappings(actual_schema.id)

    def test_handle_non_existing_mappings(
        self,
        biz_topic, biz_schema_json, biz_schema_elements
    ):
        actual = factories.create_avro_schema(
            biz_schema_json,
            biz_schema_elements,
            topic_name=biz_topic.name,
            namespace=biz_topic.source.namespace.name,
            source=biz_topic.source.name
        )
        assert not self._get_meta_attr_mappings(actual.id)
