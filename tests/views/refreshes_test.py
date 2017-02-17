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

from schematizer.views import refreshes as refresh_views
from schematizer_testing import factories
from tests.views.api_test_base import ApiTestBase


class RefreshTestBase(ApiTestBase):

    @property
    def namespace_foo(self):
        return 'foo'

    @property
    def source_bar(self):
        return 'bar'

    @pytest.fixture
    def source(self):
        return factories.create_source(self.namespace_foo, self.source_bar)

    @pytest.fixture
    def source_bar_refresh(self, source):
        return factories.create_refresh(source_id=source.id)


class TestGetRefreshByID(RefreshTestBase):

    def test_happy_case(self, mock_request, source_bar_refresh):
        mock_request.matchdict = {'refresh_id': str(source_bar_refresh.id)}
        actual = refresh_views.get_refresh_by_id(mock_request)
        expected = self.get_expected_src_refresh_resp(source_bar_refresh.id)
        assert actual == expected

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'refresh_id': '0'}
            refresh_views.get_refresh_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Refresh id 0 not found."


class TestUpdateRefresh(RefreshTestBase):

    @property
    def update_request(self):
        return {
            'status': 'IN_PROGRESS',
            'offset': 100,
            'max_primary_key': 1000
        }

    def test_update_refresh(self, mock_request, source_bar_refresh):
        mock_request.json_body = self.update_request
        mock_request.matchdict = {'refresh_id': str(source_bar_refresh.id)}
        actual = refresh_views.update_refresh(mock_request)

        expected = self.get_expected_src_refresh_resp(
            source_bar_refresh.id,
            status='IN_PROGRESS',
            offset=100,
            max_primary_key=1000
        )
        assert actual == expected

    def test_non_existing_refresh_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'refresh_id': '0'}
            mock_request.json_body = self.update_request
            refresh_views.update_refresh(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Refresh id 0 not found."


class TestGetRefreshesByCriteria(RefreshTestBase):

    @pytest.fixture
    def refresh_one(self, source):
        return factories.create_refresh(source_id=source.id)

    @pytest.fixture
    def refresh_two(self, source, refresh_one):
        return factories.create_refresh(
            source_id=source.id,
            status='IN_PROGRESS',
            created_at=refresh_one.created_at + 2,  # created 2 seconds later
            updated_at=refresh_one.updated_at + 5
        )

    @pytest.mark.usefixtures('refresh_one', 'refresh_two')
    def test_non_existing_namespace(self, mock_request):
        mock_request.params = {'namespace': 'missing'}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    @pytest.mark.usefixtures('refresh_one', 'refresh_two')
    def test_no_matching_refreshes(self, mock_request):
        mock_request.params = {
            'namespace': self.namespace_foo,
            'status': 'FAILED'
        }
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    def test_filter_by_namespace(self, mock_request, refresh_one, refresh_two):
        mock_request.params = {'namespace': self.namespace_foo}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        expected = [self.get_expected_src_refresh_resp(refresh_one.id),
                    self.get_expected_src_refresh_resp(refresh_two.id)]
        assert actual == expected

    def test_filter_by_status(self, mock_request, refresh_one, refresh_two):
        mock_request.params = {'status': 'NOT_STARTED'}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        expected = [self.get_expected_src_refresh_resp(refresh_one.id)]
        assert actual == expected

    def test_get_refreshes_created_after_timestamp(
        self, mock_request, refresh_one, refresh_two
    ):
        mock_request.params = {'created_after': refresh_one.created_at}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        expected = [self.get_expected_src_refresh_resp(refresh_one.id),
                    self.get_expected_src_refresh_resp(refresh_two.id)]
        assert actual == expected

    def test_get_refreshes_updated_after_timestamp(
        self, mock_request, refresh_one, refresh_two
    ):
        mock_request.params = {'updated_after': refresh_one.updated_at + 1}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        expected = [self.get_expected_src_refresh_resp(refresh_two.id)]
        assert actual == expected
