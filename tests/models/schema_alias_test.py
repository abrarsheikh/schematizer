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


def sample_schema_id(
    namespace_name='default_namespace',
    source_name='default_source',
):
    schema = factories.create_avro_schema(
        schema_json={
            "type": "fixed",
            "size": 16,
            "name": factories.generate_name("fixed_type")
        },
        namespace=namespace_name,
        source=source_name,
    )
    return schema.id


class TestGetSchemaAlias(GetModelsBasicTests):

    entity_cls = SchemaAlias

    def create_entity_func(self):
        schema_id = sample_schema_id()
        alias = factories.generate_name()
        return factories.create_schema_alias(schema_id, alias)

    def assert_func(self, actual, expected):
        return asserts.assert_equal_schema_alias(actual, expected)


class TestSchemaAliasConstraints(DBTestCase):

    def test_duplicate_aliases(self):
        alias = 'duplicate_alias'
        schema_id = sample_schema_id()

        factories.create_schema_alias(schema_id, alias)

        with pytest.raises(IntegrityError):
            factories.create_schema_alias(schema_id, alias)
