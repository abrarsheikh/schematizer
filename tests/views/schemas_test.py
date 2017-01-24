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
import staticconf.testing

from schematizer import models
from schematizer.api.requests.requests_v1 import DEFAULT_KAFKA_CLUSTER_TYPE
from schematizer.helpers.formatting import _format_timestamp
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
        assert str(e.value) == 'AvroSchema id 0 not found.'

    def test_get_schema_by_id(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': str(biz_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)
        expected = self.get_expected_schema_resp(biz_schema.id)
        assert mock_request.response.cache_control == 'max-age=86400'
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

    @property
    def avro_schema(self):
        return {"type": "map", "values": "int"}

    @pytest.fixture
    def request_json(self):
        return {
            "schema": simplejson.dumps(self.avro_schema),
            "namespace": 'foo',
            "source": 'bar',
            "source_owner_email": 'test@example.com',
            "contains_pii": False,
            "cluster_type": 'example_cluster_type'
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

    @pytest.mark.parametrize("schema_without_doc", [
        {
            "name": "record_without_doc",
            "type": "record",
            "fields": [{"name": "id", "type": "int", "doc": "id"}],
        },
        {
            "name": "record_with_empty_doc",
            "type": "record",
            "fields": [{"name": "id", "type": "int", "doc": "id"}],
            "doc": ""
        },
        {
            "name": "record_field_without_doc",
            "type": "record",
            "fields": [{"name": "id", "type": "int"}],
            "doc": "doc"
        },
        {
            "name": "record_field_with_empty_doc",
            "type": "record",
            "fields": [{"name": "id", "type": "int", "doc": "  "}],
            "doc": "doc"
        },
    ])
    def test_register_missing_doc_schema(
        self,
        mock_request,
        request_json,
        schema_without_doc
    ):
        request_json['schema'] = simplejson.dumps(schema_without_doc)
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert "Missing `doc` " in str(e.value)

    @property
    def whitelisted_namespace(self):
        return 'whitelisted_namespace'

    @pytest.yield_fixture(autouse=True, scope='module')
    def mock_namespace_whitelist(self):
        with staticconf.testing.MockConfiguration(
            {'namespace_no_doc_required': [self.whitelisted_namespace]}
        ):
            yield

    def test_register_schema_without_doc_in_whitelisted_namespace(
        self,
        mock_request,
        request_json
    ):
        request_json['schema'] = simplejson.dumps({
            "type": "record",
            "name": "foo",
            "fields": [{"name": "bar", "type": "int"}]
        })
        request_json['namespace'] = self.whitelisted_namespace
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    @pytest.mark.parametrize(
        "invalid_name, expected_error",
        [(None, "Namespace name must be non-empty."),
         (' ', "Namespace name must be non-empty."),
         ('123', "Namespace name must not be numeric."),
         ('a|b', "Namespace name must not contain restricted character |")]
    )
    def test_register_schema_with_invalid_namespace_name(
        self,
        mock_request,
        request_json,
        invalid_name,
        expected_error
    ):
        request_json['namespace'] = invalid_name
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == expected_error

    @pytest.mark.parametrize(
        "invalid_name, expected_error",
        [(None, "Source name must be non-empty."),
         (' ', "Source name must be non-empty."),
         ('123', "Source name must not be numeric."),
         ('a|b', "Source name must not contain restricted character |")]
    )
    def test_register_schema_with_invalid_source_name(
        self,
        mock_request,
        request_json,
        invalid_name,
        expected_error
    ):
        request_json['source'] = invalid_name
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == expected_error

    @pytest.mark.parametrize("invalid_email", [None, ' '])
    def test_register_schema_with_empty_owner_email(
        self,
        invalid_email,
        mock_request,
        request_json
    ):
        request_json['source_owner_email'] = invalid_email
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
        request_json.pop('cluster_type', None)
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)
        assert actual['topic']['cluster_type'] == DEFAULT_KAFKA_CLUSTER_TYPE

    def test_register_schema_with_cluster_type(
        self,
        mock_request,
        request_json
    ):
        request_json['cluster_type'] = 'scribe'
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_schema_with_meta_attr_mapping(
        self,
        mock_request,
        request_json,
        meta_attr_schema
    ):
        mock_request.json_body = request_json
        schema_without_meta_attr = schema_views.register_schema(mock_request)
        expected = self.get_expected_schema_resp(
            schema_without_meta_attr['schema_id']
        )
        assert schema_without_meta_attr == expected
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            models.Source.__name__,
            schema_without_meta_attr['topic']['source']['source_id']
        )
        schema_with_meta_attr = schema_views.register_schema(mock_request)
        expected = self.get_expected_schema_resp(
            schema_with_meta_attr['schema_id'],
            required_meta_attr_schema_ids=[meta_attr_schema.id]
        )
        assert schema_with_meta_attr == expected


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
        assert str(e.value) == 'AvroSchema id 0 not found.'

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
                    'created_at': _format_timestamp(element.created_at),
                    'updated_at': _format_timestamp(element.updated_at)
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
        assert str(e.value) == 'AvroSchema id 0 not found.'
