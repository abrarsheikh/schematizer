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

from schematizer.models.data_target import DataTarget
from schematizer.models.exceptions import EntityNotFoundError
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests
from tests.models.testing_db import DBTestCase


class TestGetDataTargets(GetModelsBasicTests):

    entity_cls = DataTarget

    def create_entity_func(self):
        return factories.create_data_target(
            name=factories.generate_name('data_target'),
            target_type='my_target_type',
            destination='some_destination'
        )

    def assert_func(self, actual, expected):
        return asserts.assert_equal_data_target(actual, expected)


class TestGetDataTargetByName(DBTestCase):

    @pytest.fixture
    def data_target_foo(self):
        return factories.create_data_target(
            'foo',
            'redshift',
            'foo_destination'
        )

    def test_happy_case(self, data_target_foo):
        actual = DataTarget.get_by_name(data_target_foo.name)
        asserts.assert_equal_data_target(actual, expected=data_target_foo)

    def test_non_existed_data_target(self):
        with pytest.raises(EntityNotFoundError):
            DataTarget.get_by_name(name='bad data target')
