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

from schematizer.models.data_source_target_mapping import (
    DataSourceTargetMapping
)
from schematizer.models.namespace import Namespace
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests


class TestGetDataSourceTargetMappings(GetModelsBasicTests):

    def create_source_target_mapping(self):
        namespace = factories.create_namespace(
            namespace_name=factories.generate_name('namespace')
        )
        data_target = factories.get_or_create_data_target(
            name='example_data_target',
            target_type='redshift',
            destination='some redshift cluster'
        )
        return factories.create_data_source_target_mapping(
            source_type=Namespace.__name__,
            source_id=namespace.id,
            target_id=data_target.id
        )

    entity_cls = DataSourceTargetMapping
    create_entity_func = create_source_target_mapping

    def get_assert_func(self):
        return asserts.assert_equal_data_source_target_mapping
