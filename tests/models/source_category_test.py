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

from schematizer.models.source_category import SourceCategory
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests


class TestGetSourceCategories(GetModelsBasicTests):

    entity_cls = SourceCategory

    def create_entity_func(self):
        source_bar = factories.get_or_create_source(
            namespace_name='foo',
            source_name=factories.generate_name("source_bar"),
            owner_email='test.dev@example.com'
        )
        return factories.create_source_category(
            source_id=source_bar.id,
            category=factories.generate_name("some_category")
        )

    def assert_func(self, actual, expected):
        return asserts.assert_equal_source_category(actual, expected)
