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
from sqlalchemy.exc import IntegrityError

from schematizer.models.source import Topic
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetModelsBasicTests
from tests.models.testing_db import DBTestCase


class TestGetTopics(GetModelsBasicTests):

    def create_topic(self):
        return factories.create_topic(
            topic_name=factories.generate_name('topic'),
            namespace_name='foo',
            source_name='bar'
        )

    entity_cls = Topic
    create_entity_func = create_topic

    def get_assert_func(self):
        return asserts.assert_equal_topic


class TestTopicModel(DBTestCase):

    def test_valid_cluster_type(self, biz_source):
        cluster_type = 'scribe'
        topic = factories.create_topic(
            topic_name='yelp.biz_test.1',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name,
            cluster_type=cluster_type
        )
        assert topic.cluster_type == cluster_type

    def test_empty_cluster_type(self, biz_source):
        with pytest.raises(IntegrityError):
            factories.create_topic(
                topic_name='yelp.biz_test.1',
                namespace_name=biz_source.namespace.name,
                source_name=biz_source.name,
                cluster_type=None
            )
