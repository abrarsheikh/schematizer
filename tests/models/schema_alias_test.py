# -*- coding: utf-8 -*-
# Copyright 2017 Yelp Inc.
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
from sqlalchemy.exc import IntegrityError

from schematizer.models.schema_alias import SchemaAlias
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests
from tests.models.testing_db import DBTestCase


class TestGetSchemaAlias(GetModelsBasicTests):

    entity_cls = SchemaAlias

    def create_entity_func(self):
        alias = factories.generate_name()
        schema_id = factories.create_avro_schema(
            {
                "name": "biz",
                "type": "record",
                "fields": [
                    {"name": "id", "type": "int", "doc": "id", "default": 0}
                ],
                "doc": "biz table"
            }
        ).id

        return factories.create_schema_alias(schema_id, alias)

    def assert_func(self, actual, expected):
        return asserts.assert_equal_schema_alias(actual, expected)


class TestSchemaAliasConstraints(DBTestCase):

    def test_happy(self, biz_schema_json):
        # We create two distinct names that we'll reuse for namespace,
        # source, and alias.
        first_name = factories.generate_name('first')
        different_name = factories.generate_name('diff')

        # We'll register the first schema with the first_name as the
        # namespace, source, and alias
        schema_id = factories.create_avro_schema(
            biz_schema_json,
            namespace=first_name,
            source=first_name,
            topic_name=factories.generate_name('first'),
        ).id

        factories.create_schema_alias(schema_id, first_name)

        # If we register that same schema with a different alias, we're happy
        factories.create_schema_alias(schema_id, different_name)

        # If we register the same alias with a different source, we're happy
        diff_source_schema_id = factories.create_avro_schema(
            biz_schema_json,
            namespace=first_name,
            source=different_name,
            topic_name=factories.generate_name('diff_src'),
        ).id
        factories.create_schema_alias(diff_source_schema_id, first_name)

        # If we register the same source and alias with a different
        # namespace, we're happy
        diff_namespace_schema_id = factories.create_avro_schema(
            biz_schema_json,
            namespace=different_name,
            source=first_name,
            topic_name=factories.generate_name('diff_ns'),
        ).id
        factories.create_schema_alias(diff_namespace_schema_id, first_name)

    def test_duplicate_aliases(self, biz_schema):
        alias = 'duplicate_alias'

        factories.create_schema_alias(biz_schema.id, alias)

        with pytest.raises(IntegrityError):
            factories.create_schema_alias(biz_schema.id, alias)
