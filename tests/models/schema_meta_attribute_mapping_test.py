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

from schematizer.models.schema_meta_attribute_mapping \
    import SchemaMetaAttributeMapping
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests


class TestGetSchemaMetaAttrMappings(GetModelsBasicTests):

    def create_meta_attr_mapping(self):
        schema = factories.create_avro_schema(
            schema_json={
                "type": "enum",
                "name": factories.generate_name("some_enum"),
                "symbols": ["a"]
            }
        )
        meta_attr = factories.create_avro_schema(schema_json={"type": "int"})
        return factories.create_schema_meta_attr_mapping(
            schema_id=schema.id,
            meta_attr_id=meta_attr.id
        )

    entity_cls = SchemaMetaAttributeMapping
    create_entity_func = create_meta_attr_mapping

    def get_assert_func(self):
        return asserts.assert_equal_schema_meta_attr_mapping
