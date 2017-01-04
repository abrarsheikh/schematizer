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

from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.topic import Topic
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests
from tests.models.testing_db import DBTestCase


class TestGetTopics(GetModelsBasicTests):

    entity_cls = Topic

    def create_entity_func(self):
        return factories.create_topic(
            topic_name=factories.generate_name('topic'),
            namespace_name='foo',
            source_name='bar'
        )

    def assert_func(self, actual, expected):
        return asserts.assert_equal_topic(actual, expected)


class TestGetTopicByName(DBTestCase):

    @pytest.fixture
    def topic_one(self):
        return factories.create_topic(
            topic_name='topic_one',
            namespace_name='foo',
            source_name='bar'
        )

    def test_happy_case(self, topic_one):
        actual = Topic.get_by_name(topic_one.name)
        asserts.assert_equal_topic(actual, expected=topic_one)

    def test_nonexistent_topic(self):
        with pytest.raises(EntityNotFoundError):
            Topic.get_by_name(name='bad topic')
