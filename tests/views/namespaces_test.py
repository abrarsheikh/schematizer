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

from schematizer.views import namespaces as namespace_views
from schematizer_testing import factories
from tests.views.api_test_base import ApiTestBase


class TestListSourcesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'namespace': 'foo'}
            mock_request.params = {}
            namespace_views.list_sources_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Namespace name `foo` not found."

    def test_happy_case(self, mock_request, yelp_namespace, biz_source):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        expected = [self.get_expected_src_resp(biz_source.id)]
        assert actual == expected

    def test_with_min_id(self, mock_request, yelp_namespace, biz_source):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {'min_id': biz_source.id + 1}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        assert actual == []

    def test_with_count(
            self,
            mock_request,
            yelp_namespace,
            biz_source,
            another_biz_source
    ):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {'count': 1}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        expected = [self.get_expected_src_resp(biz_source.id)]
        assert actual == expected


class TestListNamespaces(ApiTestBase):

    def test_no_namespaces(self, mock_request):
        actual = namespace_views.list_namespaces(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, yelp_namespace):
        actual = namespace_views.list_namespaces(mock_request)
        expected = [self.get_expected_namespace_resp(yelp_namespace.id)]
        assert actual == expected


class TestListRefreshesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'namespace': 'foo'}
            namespace_views.list_refreshes_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Namespace name `foo` not found."

    def test_happy_case(self, mock_request, yelp_namespace, biz_src_refresh):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        actual = namespace_views.list_refreshes_by_namespace(mock_request)
        expected = [self.get_expected_src_refresh_resp(biz_src_refresh.id)]
        assert actual == expected


class TestGetSchemaFromAlias(ApiTestBase):

    def test_missing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'namespace': 'foo',
                'source': 'bar',
                'alias': 'baz',
            }
            namespace_views.get_schema_from_alias(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            "SchemaAlias namespace `foo`, "
            "source `bar`, alias `baz` not found."
        )

    def test_missing_source(self, mock_request, yelp_namespace):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'namespace': yelp_namespace.name,
                'source': 'bar',
                'alias': 'baz',
            }
            namespace_views.get_schema_from_alias(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            "SchemaAlias namespace `{}`, "
            "source `bar`, alias `baz` not found.".format(
                yelp_namespace.name,
            )
        )

    def test_missing_alias(self, mock_request, yelp_namespace, biz_source):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'namespace': yelp_namespace.name,
                'source': biz_source.name,
                'alias': 'baz',
            }
            namespace_views.get_schema_from_alias(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            "SchemaAlias namespace `{}`, "
            "source `{}`, alias `baz` not found.".format(
                biz_source.namespace.name,
                biz_source.name,
            )
        )

    def test_happy_case(
        self,
        mock_request,
        yelp_namespace,
        biz_source,
        biz_schema
    ):
        schema_alias = 'baz'
        factories.create_schema_alias(biz_schema.id, schema_alias)
        mock_request.matchdict = {
            'namespace': yelp_namespace.name,
            'source': biz_source.name,
            'alias': schema_alias,
        }

        actual = namespace_views.get_schema_from_alias(mock_request)
        expected = self.get_expected_schema_resp(biz_schema.id)
        assert actual == expected


class TestGetSchemaAndAliasFromNamespace(ApiTestBase):

    def test_namespace_does_not_exist(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception):
            mock_request.matchdict = {'namespace_name': 'some_name'}
            namespace_views.get_schema_and_alias_from_namespace_name(
                mock_request
            )

    def test_no_alias_for_namespace(
        self,
        mock_request,
        yelp_namespace
    ):
        mock_request.matchdict = {'namespace_name': yelp_namespace.name}
        response = namespace_views.get_schema_and_alias_from_namespace_name(
            mock_request
        )
        assert not response

    def test_happy_case(
        self,
        mock_request,
        yelp_namespace,
        biz_schema,
        biz_source
    ):
        aliases = ['one', 'two', 'three']
        for alias in aliases:
            factories.create_schema_alias(biz_schema.id, alias)

        mock_request.matchdict = {'namespace_name': yelp_namespace.name}
        actual = namespace_views.get_schema_and_alias_from_namespace_name(
            mock_request
        )
        for element in actual:
            assert element['alias'] in aliases
            assert element['source_name'] == biz_source.name
            assert element['schema_id'] == biz_schema.id
            aliases.remove(element['alias'])
