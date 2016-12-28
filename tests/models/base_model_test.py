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
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.page_info import PageInfo
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.testing_db import DBTestCase


class GetModelsBasicTests(DBTestCase):

    entity_cls = None
    create_entity_func = None

    def get_assert_func(self):
        raise NotImplementedError()

    @pytest.fixture
    def entities(self):
        return [self.create_entity_func() for _ in range(3)]

    def test_get_all_entities(self, entities):
        actual = self.entity_cls.get_all()
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=entities,
            assert_func=self.get_assert_func()
        )

    def test_when_no_entity_exists(self):
        actual = self.entity_cls.get_all()
        assert actual == []

    def test_get_max_count_of_entities(self, entities):
        actual = self.entity_cls.get_all(PageInfo(count=1))
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=entities[0:1],
            assert_func=self.get_assert_func()
        )

    def test_filter_by_min_entity_id(self, entities):
        entity_1 = entities[0]
        actual = self.entity_cls.get_all(PageInfo(min_id=entity_1.id + 1))
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=entities[1:],
            assert_func=self.get_assert_func()
        )

    def test_get_only_one_entity_with_id_greater_than_min_id(self, entities):
        entity_1 = entities[0]
        actual = self.entity_cls.get_all(
            PageInfo(count=1, min_id=entity_1.id + 1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=entities[1:2],
            assert_func=self.get_assert_func()
        )

    def test_get_single_entity_by_id(self):
        entity = self.create_entity_func()
        actual = self.entity_cls.get_by_id(entity.id)
        self.get_assert_func()(actual, expected=entity)

    def test_get_nonexistent_entity(self):
        with pytest.raises(EntityNotFoundError):
            self.entity_cls.get_by_id(obj_id=0)


class TestGetModelById(DBTestCase):

    @pytest.fixture
    def dw_data_target(self):
        return factories.create_data_target(
            name='yelp_redshift',
            target_type='redshift',
            destination='example.org'
        )

    @pytest.fixture
    def dw_consumer_group(self, dw_data_target):
        return factories.create_consumer_group('dw', dw_data_target)

    @pytest.fixture
    def dw_consumer_group_data_source(self, dw_consumer_group, biz_source):
        return factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=biz_source.id
        )

    def test_get_consumer_group_by_id(self, dw_consumer_group):
        actual = models.ConsumerGroup.get_by_id(dw_consumer_group.id)
        asserts.assert_equal_consumer_group(actual, dw_consumer_group)

    def test_get_consumer_group_data_source_by_id(
        self,
        dw_consumer_group_data_source
    ):
        actual = models.ConsumerGroupDataSource.get_by_id(
            dw_consumer_group_data_source.id
        )
        asserts.assert_equal_consumer_group_data_source(
            actual,
            dw_consumer_group_data_source
        )

    @pytest.mark.parametrize('model_cls', [
        models.AvroSchemaElement,
        models.ConsumerGroup,
        models.ConsumerGroupDataSource
    ])
    def test_get_invalid_id(self, model_cls):
        with pytest.raises(EntityNotFoundError):
            model_cls.get_by_id(0)


class TestCreateModel(DBTestCase):

    def test_create_data_target(self):
        actual = models.DataTarget.create(
            session,
            name='yelp_redshift',
            target_type='foo',
            destination='bar'
        )
        expected = models.DataTarget.get_by_id(actual.id)
        asserts.assert_equal_data_target(actual, expected)
        assert actual.target_type == 'foo'
        assert actual.destination == 'bar'

    def test_create_consumer_group(self, dw_data_target):
        actual = models.ConsumerGroup.create(
            session,
            group_name='foo',
            data_target=dw_data_target
        )
        expected = models.ConsumerGroup.get_by_id(actual.id)
        asserts.assert_equal_consumer_group(actual, expected)
        assert actual.group_name == 'foo'
        assert actual.data_target_id == dw_data_target.id

    def test_create_consumer_group_data_source(
        self,
        yelp_namespace,
        dw_consumer_group
    ):
        actual = models.ConsumerGroupDataSource.create(
            session,
            data_source_type=models.DataSourceTypeEnum.NAMESPACE,
            data_source_id=yelp_namespace.id,
            consumer_group=dw_consumer_group
        )
        expected = models.ConsumerGroupDataSource.get_by_id(actual.id)
        asserts.assert_equal_consumer_group_data_source(actual, expected)
        assert actual.data_source_type == models.DataSourceTypeEnum.NAMESPACE
        assert actual.data_source_id == yelp_namespace.id
        assert actual.consumer_group_id == dw_consumer_group.id
