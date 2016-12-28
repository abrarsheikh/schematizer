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
"""
This module contains the internal data structure to hold the information
of parsed SQL schemas.
"""
from __future__ import absolute_import
from __future__ import unicode_literals


class SQLTable(object):
    """Internal data structure that represents a general sql table.
    """

    def __init__(
        self,
        table_name,
        columns=None,
        doc=None,
        schema_name=None,
        **metadata
    ):
        self.name = table_name
        self.columns = columns or []
        self.doc = doc
        self.schema_name = schema_name
        # any additional metadata that does not belong to sql table
        # definition but would like to be tracked.
        self.metadata = metadata

    def __eq__(self, other):
        return all([
            isinstance(other, SQLTable),
            self.name == other.name,
            self.columns == other.columns,
            self.metadata == other.metadata,
            self.schema_name == other.schema_name
        ])

    @property
    def full_name(self):
        if self.schema_name:
            return '{0}.{1}'.format(self.schema_name, self.name)
        return self.name

    @property
    def primary_keys(self):
        return sorted(
            (col for col in self.columns if col.primary_key_order),
            key=lambda c: c.primary_key_order
        )


class SQLColumn(object):
    """Internal data structure that represents a general sql column.
    It is intended to support sql column definition in general. The
    column type could be database specific.
    """

    def __init__(self, column_name, column_type, primary_key_order=None,
                 is_nullable=True, default_value=None,
                 attributes=None, doc=None, **metadata):
        self.name = column_name
        self.type = column_type
        self.primary_key_order = primary_key_order
        self.is_nullable = is_nullable
        self.default_value = default_value
        self.doc = doc
        # attributes contain column settings except default value and nullable
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.name, attr)
                                       for attr in self.attributes)
        # any additional metadata that does not belong to sql column
        # definition but would like to be tracked, such as alias
        self.metadata = metadata

    def get_attribute(self, key):
        return self._attributes_lookup.get(key)

    def __eq__(self, other):
        return (isinstance(other, SQLColumn) and
                self.name == other.name and
                self.type == other.type and
                self.primary_key_order == other.primary_key_order and
                self.is_nullable == other.is_nullable and
                self.default_value == other.default_value and
                self.attributes == other.attributes and
                self.metadata == other.metadata)


class SQLAttribute(object):
    """Class that holds the sql attributes in the table/column definitions,
    such as column default value, nullable property, character set, etc.
    """

    def __init__(self, name):
        self.name = name
        self.value = None
        self.has_value = False

    @classmethod
    def create_with_value(cls, name, value):
        attribute = SQLAttribute(name)
        attribute.name = name
        attribute.value = value
        attribute.has_value = True
        return attribute

    def __eq__(self, other):
        return (isinstance(other, SQLAttribute) and
                self.name == other.name and
                self.value == other.value and
                self.has_value == other.has_value)

    def __hash__(self):
        return hash((self.name, self.value, self.has_value))


class SQLColumnDataType(object):
    """Internal data structure that contains column data type information.
    """

    type_name = None

    def __init__(self, attributes=None):
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.name, attr)
                                       for attr in self.attributes)

    def attribute_exists(self, name):
        return name in self._attributes_lookup

    def get_attribute(self, name):
        return self._attributes_lookup.get(name)

    def __eq__(self, other):
        return all([
            isinstance(other, SQLColumnDataType),
            self.attributes == other.attributes,
            self.type_name == other.type_name
        ])

    def convert_str_to_type_val(self, val_string):
        """Convert the given string representation of the value to the value
        of this data type.  Each data type is responsible for converting the
        string to the value of correct type.  It returns `None` if the given
        string is missing or `null`.  Otherwise, it returns the original value
        string by default.
        """
        if self._is_null_string(val_string):
            return None
        else:
            return self._convert_str_to_type_val(val_string)

    def _convert_str_to_type_val(self, val_string):
        """Convert the given string representation of the value to the value
        of this data type.  Each data type is responsible for converting the
        string to the value of correct type.
        """
        raise NotImplementedError('Must be implemented by subclasses')

    def _is_null_string(self, val_string):
        return val_string is None or val_string.lower() == 'null'


class MetaDataKey(object):
    """Key of metadata attributes"""

    NAMESPACE = 'namespace'
    ALIASES = 'aliases'
    PERMISSION = 'permission'


class DbPermission(object):

    def __init__(
        self,
        object_name,
        user_or_group_name,
        permission,
        for_group=False
    ):
        self.object_name = object_name
        self.user_or_group_name = user_or_group_name
        self.permission = permission
        self.for_group = for_group

    def __eq__(self, other):
        return (isinstance(other, DbPermission) and
                self.object_name == other.object_name and
                self.user_or_group_name == other.user_or_group_name and
                self.permission == other.permission and
                self.for_group == other.for_group)
