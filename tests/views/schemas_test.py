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

import time

import mock
import pytest
import simplejson

from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.helpers.formatting import _format_datetime
from schematizer.views import schemas as schema_views
from schematizer_testing import factories
from tests.views.api_test_base import ApiTestBase


class TestGetSchemaByID(ApiTestBase):

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_schema_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_get_schema_by_id(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)
        expected = self.get_expected_schema_resp(biz_schema.id)
        assert actual == expected

    def test_get_schema_with_base_schema(self, mock_request, biz_schema):
        biz_schema.base_schema_id = 2
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)

        expected = self.get_expected_schema_resp(
            biz_schema.id,
            base_schema_id=2
        )
        assert actual == expected

    def test_schema_with_pkey(self, mock_request, biz_pkey_schema):
        mock_request.matchdict = {'schema_id': str(biz_pkey_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)
        expected = self.get_expected_schema_resp(biz_pkey_schema.id)
        assert actual == expected


class TestGetSchemasByCriteria(ApiTestBase):

    # TODO [clin|DATAPIPE-2024] add more tests
    @pytest.fixture
    def created_timestamp(self):
        return int(time.time())

    @pytest.fixture
    def disabled_schema_0(self, created_timestamp):
        return factories.create_avro_schema(
            schema_json={"type": "array", "items": "int"},
            created_at=created_timestamp,
            status=models.AvroSchemaStatus.DISABLED
        )

    @pytest.fixture
    def ro_schema_1(self, created_timestamp):
        return factories.create_avro_schema(
            schema_json={"type": "array", "items": "int"},
            created_at=created_timestamp + 1
        )

    @pytest.fixture
    def rw_schema_2(self, created_timestamp):
        return factories.create_avro_schema(
            schema_json={"type": "array", "items": "int"},
            created_at=created_timestamp + 2
        )

    def test_get_schemas_created_after_given_timestamp(
        self, mock_request, disabled_schema_0, ro_schema_1, rw_schema_2
    ):
        mock_request.params = {
            'created_after': disabled_schema_0.created_at
        }
        actual = schema_views.get_schemas_created_after(mock_request)
        expected = [
            self.get_expected_schema_resp(schema.id) for schema in
            [ro_schema_1, rw_schema_2]
        ]
        assert actual == expected

        mock_request.params = {'created_after': ro_schema_1.created_at + 1}
        actual = schema_views.get_schemas_created_after(mock_request)
        expected = [self.get_expected_schema_resp(rw_schema_2.id)]
        assert actual == expected

    def test_limit_schemas_by_count(
        self, mock_request, disabled_schema_0, ro_schema_1, rw_schema_2
    ):
        mock_request.params = {'created_after': 0, 'count': 1}
        actual = schema_views.get_schemas_created_after(mock_request)
        expected = [self.get_expected_schema_resp(ro_schema_1.id)]
        assert actual == expected

    def test_limit_schemas_by_min_id(
        self, mock_request, disabled_schema_0, ro_schema_1, rw_schema_2
    ):
        mock_request.params = {'created_after': 0, 'min_id': rw_schema_2.id}
        actual = schema_views.get_schemas_created_after(mock_request)
        expected = [self.get_expected_schema_resp(rw_schema_2.id)]
        assert actual == expected


class RegisterSchemaTestBase(ApiTestBase):

    def _assert_equal_schema_response(self, actual, request_json):
        expected_vals = {}
        if 'base_schema_id' in request_json:
            expected_vals = {'base_schema_id': request_json['base_schema_id']}
        expected = self.get_expected_schema_resp(
            actual['schema_id'],
            **expected_vals
        )
        assert actual == expected

        # verify to ensure the source is correct.
        actual_src_name = actual['topic']['source']['name']
        assert actual_src_name == request_json['source']

        actual_namespace_name = actual['topic']['source']['namespace']['name']
        assert actual_namespace_name == request_json['namespace']


class TestRegisterSchema(RegisterSchemaTestBase):

    @pytest.fixture
    def request_json(self, biz_schema_json, biz_source):
        return {
            "schema": simplejson.dumps(biz_schema_json),
            "namespace": biz_source.namespace.name,
            "source": biz_source.name,
            "source_owner_email": 'biz.user@yelp.com',
            'contains_pii': False
        }

    def test_register_schema(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_create_schema_with_base_schema(self, mock_request, request_json):
        request_json['base_schema_id'] = 2
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_invalid_schema_json(self, mock_request, request_json):
        request_json['schema'] = 'Not valid json!%#!#$#'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Error "Expecting value: line 1 column 1 (char 0)" encountered '
            'decoding JSON: "Not valid json!%#!#$#"'
        )

    def test_register_invalid_avro_format(self, mock_request, request_json):
        request_json['schema'] = '{"type": "record", "name": "A"}'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert "Invalid Avro schema JSON." in str(e.value)

    @pytest.mark.parametrize("biz_schema_without_doc_json", [
        {
            "name": "biz",
            "type": "record",
            "fields": [{
                "name": "id",
                "type": "int",
                "doc": "id",
                "default": 0
            }],
        },
        {
            "name": "biz",
            "type": "record",
            "fields": [{"name": "id",
                        "type": "int",
                        "doc": "id",
                        "default": 0
                        }],
            "doc": ""
        },
        {
            "name": "biz",
            "type": "record",
            "fields": [{
                "name": "id",
                "type": "int",
                "default": 0
            }],
            "doc": "doc"
        },
        {
            "name": "biz",
            "type": "record",
            "fields": [{"name": "id",
                        "type": "int",
                        "doc": "   ",
                        "default": 0
                        }],
            "doc": "doc"
        },
    ])
    def test_register_missing_doc_schema(
        self,
        mock_request,
        request_json,
        biz_schema_without_doc_json
    ):
        request_json['schema'] = simplejson.dumps(biz_schema_without_doc_json)
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert "Missing `doc` " in str(e.value)

    @property
    def biz_wl_schema_json(self):
        return {
            "name": "biz_wl",
            "type": "record",
            "fields": [{"name": "id", "type": "int", "default": 0}],
            "doc": ""
        }

    def test_register_missing_doc_schema_NS_whitelisted(
        self,
        mock_request,
        request_json
    ):
        request_json['schema'] = simplejson.dumps(self.biz_wl_schema_json)
        request_json['namespace'] = 'yelp_wl'
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_invalid_namespace_name(self, mock_request, request_json):
        request_json['namespace'] = 'yelp|main'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Source name or Namespace name should not contain the '
            'restricted character: |'
        )

    def test_register_invalid_numeric_src_name(
        self,
        mock_request,
        request_json
    ):
        request_json['source'] = '12345'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'Source or Namespace name should not be numeric'

    @pytest.mark.parametrize("src_name", [(None), (' ')])
    def test_register_empty_src_name(
        self,
        src_name,
        mock_request,
        request_json
    ):
        request_json['source'] = src_name
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Source name must be non-empty."

    @pytest.mark.parametrize("email", [(None), (' ')])
    def test_register_empty_args(
        self,
        email,
        mock_request,
        request_json
    ):
        request_json['source_owner_email'] = email
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Source owner email must be non-empty."

    def test_register_schema_defaults_to_datapipe_cluster_type(
        self,
        mock_request,
        request_json
    ):
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_schema_with_cluster_type(
        self,
        mock_request,
        request_json
    ):
        request_json['cluster_type'] = 'scribe'
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)


class TestRegisterSchemaFromMySQL(RegisterSchemaTestBase):

    @property
    def new_create_table_stmt(self):
        return 'create table `biz` (`id` int(11), `name` varchar(10));'

    @property
    def old_create_table_stmt(self):
        return 'create table `biz` (`id` int(11));'

    @property
    def alter_table_stmt(self):
        return 'alter table `biz` add column `name` varchar(10);'

    @pytest.fixture
    def request_json(self, biz_source):
        return {
            "new_create_table_stmt": self.new_create_table_stmt,
            "namespace": biz_source.namespace.name,
            "source": biz_source.name,
            "source_owner_email": "biz.test@yelp.com",
            "contains_pii": False
        }

    def test_register_new_table(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = schema_views.register_schema_from_mysql_stmts(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_updated_table(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        request_json["alter_table_stmt"] = self.alter_table_stmt
        mock_request.json_body = request_json

        actual = schema_views.register_schema_from_mysql_stmts(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_invalid_sql_table_stmt(self, mock_request, request_json):
        request_json["new_create_table_stmt"] = 'create table biz ();'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'No column exists in the table.' in str(e.value)

    def test_register_table_with_unsupported_avro_type(
        self,
        mock_request,
        request_json
    ):
        request_json["new_create_table_stmt"] = ('create table dummy '
                                                 '(foo bar);')
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'Unknown MySQL column type' in str(e.value)

    def test_register_invalid_avro_schema(self, mock_request, request_json):
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with mock.patch.object(
            models.AvroSchema,
            'verify_avro_schema',
            return_value=(False, 'oops')
        ), pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'Invalid Avro schema JSON.' in str(e.value)

    def test_invalid_register_request(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        expected_error = (
            'Both old_create_table_stmt and alter_table_stmt must be provided.'
        )

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == expected_error


class TestGetSchemaElements(ApiTestBase):

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_schema_elements_by_schema_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_get_schema_elements(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_schema_elements_by_schema_id(mock_request)
        assert actual == self._get_expected_elements_response(biz_schema)

    def _get_expected_elements_response(self, biz_schema):
        response = []
        for element in biz_schema.avro_schema_elements:
            response.append(
                {
                    'id': element.id,
                    'schema_id': biz_schema.id,
                    'element_type': element.element_type,
                    'key': element.key,
                    'doc': element.doc,
                    'created_at': _format_datetime(
                        element.created_at
                    ),
                    'updated_at': _format_datetime(
                        element.updated_at
                    )
                }
            )

        return response


@pytest.mark.usefixtures('create_biz_src_meta_attr_mapping')
class TestGetMetaAttrBySchemaId(ApiTestBase):

    @pytest.fixture
    def create_biz_src_meta_attr_mapping(self, meta_attr_schema, biz_source):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            models.Source.__name__,
            biz_source.id
        )

    @pytest.fixture
    def new_biz_schema_json(self):
        return {
            "name": "biz",
            "type": "record",
            "fields": [
                {"name": "id", "type": "int", "doc": "id", "default": 0},
                {"name": "name", "type": "string", "doc": "biz name"}
            ],
            "doc": "biz table"
        }

    @pytest.fixture
    def request_json(self, new_biz_schema_json, biz_source):
        return {
            "schema": simplejson.dumps(new_biz_schema_json),
            "namespace": biz_source.namespace.name,
            "source": biz_source.name,
            "source_owner_email": 'biz.user@yelp.com',
            'contains_pii': False
        }

    @pytest.fixture
    def new_biz_schema_id(self, mock_request, request_json):
        mock_request.json_body = request_json
        new_biz_schema = schema_views.register_schema(mock_request)
        return new_biz_schema['schema_id']

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_meta_attributes_by_schema_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'AvroSchema id 0 not found.'

    def test_get_meta_attr_by_new_schema_id(
        self,
        mock_request,
        new_biz_schema_id,
        meta_attr_schema
    ):
        mock_request.matchdict = {'schema_id': str(new_biz_schema_id)}
        actual = schema_views.get_meta_attributes_by_schema_id(mock_request)
        expected = [meta_attr_schema.id]
        assert actual == expected

    def test_get_meta_attr_by_old_schema_id(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_meta_attributes_by_schema_id(mock_request)
        expected = []
        assert actual == expected


class TestGetDataTaragetsBySchemaID(ApiTestBase):

    def test_get_data_targets_by_schema_id(
        self,
        mock_request,
        biz_schema,
        dw_data_target,
        dw_consumer_group_source_data_src
    ):
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_data_targets_by_schema_id(mock_request)

        assert actual == [
            self.get_expected_data_target_resp(dw_data_target.id)
        ]

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_schema_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'Schema is not found.'
