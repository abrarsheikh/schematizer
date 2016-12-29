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

import simplejson
from cached_property import cached_property

from schematizer.models.page_info import PageInfo

DEFAULT_KAFKA_CLUSTER_TYPE = 'datapipe'


# TODO [clin|DATAPIPE-1433] remove these request classes and only keep common
# helper functions to reduce extra development work. Please do not add more
# such classes when adding new api endpoints.
class RequestBase(object):
    pass


class RegisterSchemaFromMySqlRequest(RequestBase):

    def __init__(
        self,
        new_create_table_stmt,
        namespace,
        source,
        source_owner_email,
        contains_pii=False,
        old_create_table_stmt=None,
        alter_table_stmt=None
    ):
        super(RegisterSchemaFromMySqlRequest, self).__init__()
        self.new_create_table_stmt = new_create_table_stmt
        self.old_create_table_stmt = old_create_table_stmt
        self.alter_table_stmt = alter_table_stmt
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email
        self.contains_pii = contains_pii
        self.cluster_type = DEFAULT_KAFKA_CLUSTER_TYPE


class AvroSchemaCompatibilityRequest(RequestBase):

    def __init__(self, schema, namespace, source):
        super(AvroSchemaCompatibilityRequest, self).__init__()
        self.schema = schema
        self.namespace = namespace
        self.source = source

    @cached_property
    def schema_json(self):
        return simplejson.loads(self.schema) if self.schema else None


class MysqlSchemaCompatibilityRequest(RequestBase):

    def __init__(
        self,
        new_create_table_stmt,
        namespace,
        source,
        old_create_table_stmt=None,
        alter_table_stmt=None
    ):
        super(MysqlSchemaCompatibilityRequest, self).__init__()
        self.new_create_table_stmt = new_create_table_stmt
        self.old_create_table_stmt = old_create_table_stmt
        self.alter_table_stmt = alter_table_stmt
        self.namespace = namespace
        self.source = source


class CreateConsumerGroupRequest(RequestBase):

    def __init__(self, group_name):
        super(CreateConsumerGroupRequest, self).__init__()
        self.group_name = group_name


class CreateConsumerGroupDataSourceRequest(RequestBase):

    def __init__(self, data_source_type, data_source_id):
        super(CreateConsumerGroupDataSourceRequest, self).__init__()
        self.data_source_type = data_source_type
        self.data_source_id = data_source_id


def get_pagination_info(query_params):
    return PageInfo(
        query_params.get('count', 0),
        query_params.get('min_id', 0)
    )
