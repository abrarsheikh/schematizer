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

from itertools import izip

import pytest

from schematizer.components.handlers import mysql_handler
from schematizer.components.handlers import sql_handler_base
from schematizer.models import mysql_data_types as data_types
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestMySQLHandler(object):

    @pytest.fixture
    def handler(self):
        return mysql_handler.MySQLHandler()

    @property
    def table_name(self):
        return 'foo'

    @property
    def create_table_sql(self):
        return 'CREATE TABLE `{0}` (`id` int(11) not null);'.format(
            self.table_name
        )

    @property
    def alter_table_sql(self):
        return 'ALTER TABLE `{0}` add `color` varchar(16);'.format(
            self.table_name
        )

    @property
    def create_table_foo_sql(self):
        return ('CREATE TABLE `foo` ('
                '`id` int(11) auto_increment not null, '
                'name varchar(255),'
                'amount decimal(10, 2) default 0.0 unsigned,'
                'age int default NULL,'
                "bar enum (\"a1\", \"a2\", 'a3') CHARACTER SET latin1,"
                "bar_one set (\"a1\", 'a2', 'a3') CHARACTER SET latin1,"
                'primary key (id) '
                ');')

    @property
    def expected_sql_table_foo(self):
        col_id = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            primary_key_order=1,
            is_nullable=False
        )
        col_name = SQLColumn('name', data_types.MySQLVarChar(255))
        col_amount = SQLColumn(
            'amount',
            data_types.MySQLDecimal(10, 2, unsigned=True),
            default_value=0.0
        )
        col_age = SQLColumn('age', data_types.MySQLInt(None))
        col_bar = SQLColumn(
            'bar',
            data_types.MySQLEnum(
                values=['a1', 'a2', 'a3'],
                char_set="latin1"
            )
        )
        col_bar_one = SQLColumn(
            'bar_one',
            data_types.MySQLSet(
                values=['a1', 'a2', 'a3'],
                char_set="latin1"
            )
        )
        return SQLTable(
            'foo',
            [col_id, col_name, col_amount, col_age, col_bar, col_bar_one]
        )

    @property
    def create_table_date_and_time_sql(self):
        return ('CREATE TABLE `date_and_time` ('
                '`id` int(11) auto_increment not null, '
                'dttm datetime default \'1000-01-01 00:00:00\' not null, '
                'tstmp timestamp default \'1970-01-01 00:00:01\' not null, '
                'tm time default \'11:12:00\' not null, '
                'yr year default \'2000\' not null, '
                'dt date default \'1000-01-01\' not null, '
                'primary key (id) '
                ');')

    @property
    def expected_sql_table_date_and_time(self):
        col_id = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            primary_key_order=1,
            is_nullable=False
        )
        col_dttm = SQLColumn(
            'dttm',
            data_types.MySQLDateTime(),
            default_value='1000-01-01 00:00:00',
            is_nullable=False
        )
        col_tstmp = SQLColumn(
            'tstmp',
            data_types.MySQLTimestamp(),
            default_value='1970-01-01 00:00:01',
            is_nullable=False
        )
        col_tm = SQLColumn(
            'tm',
            data_types.MySQLTime(),
            default_value='11:12:00',
            is_nullable=False
        )
        col_yr = SQLColumn(
            'yr',
            data_types.MySQLYear(),
            default_value='2000',
            is_nullable=False
        )
        col_dt = SQLColumn(
            'dt',
            data_types.MySQLDate(),
            default_value='1000-01-01',
            is_nullable=False
        )
        return SQLTable(
            'date_and_time',
            [col_id, col_dttm, col_tstmp, col_tm, col_yr, col_dt]
        )

    def test_create_sql_table_from_sql_stmts(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.create_table_foo_sql]
        )
        assert self.expected_sql_table_foo == sql_table

    def test_create_sql_table_from_sql_stmts_date_types(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.create_table_date_and_time_sql]
        )
        assert self.expected_sql_table_date_and_time == sql_table

    def assert_sql_table_equal_with_create_defs(
        self,
        handler,
        create_definitions,
        expected_columns
    ):
        sql = self._build_create_table_sql(create_definitions)
        actual_table = handler.create_sql_table_from_sql_stmts([sql])
        expected_table = SQLTable(self.table_name, expected_columns)
        assert_equal_sql_table(expected_table, actual_table)

    def _build_create_table_sql(self, create_definitions):
        return 'CREATE TABLE `{table}` ({definitions});'.format(
            table=self.table_name,
            definitions=','.join(create_definitions)
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` bit(4) not null',
         SQLColumn(
             'bar',
             data_types.MySQLBit(4),
             is_nullable=False
         )),
        ('bar bit null',
         SQLColumn('bar', data_types.MySQLBit(None))),
        ('bar bit default 0b0101',
         SQLColumn(
             'bar',
             data_types.MySQLBit(None),
             default_value=5
         )),
        ('bar bit default b\'101\'',
         SQLColumn(
             'bar',
             data_types.MySQLBit(None),
             default_value=5
         )),
    ])
    def test_create_sql_table_from_sql_stmts_with_bit_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` int(4) not null unsigned',
         SQLColumn(
             'bar',
             data_types.MySQLInt(4, unsigned=True),
             is_nullable=False
         )),
        ('`bar` int(4) default 10',
         SQLColumn('bar', data_types.MySQLInt(4), default_value=10)),
        ('bar tinyint null',
         SQLColumn('bar', data_types.MySQLTinyInt(None))),
        ('bar smallint null',
         SQLColumn('bar', data_types.MySQLSmallInt(None))),
        ('bar bigint null',
         SQLColumn('bar', data_types.MySQLBigInt(None))),
        ('bar integer null',
         SQLColumn('bar', data_types.MySQLInteger(None))),
    ])
    def test_create_sql_table_from_sql_stmts_with_integer_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` bool not null',
         SQLColumn(
             'bar',
             data_types.MySQLBool(),
             is_nullable=False
         )),
        ('bar bool null',
         SQLColumn('bar', data_types.MySQLBool())),
        ('bar bool default TRUE',
         SQLColumn('bar', data_types.MySQLBool(), default_value=True)),
        ('bar bool default true',
         SQLColumn('bar', data_types.MySQLBool(), default_value=True)),
        ('bar bool default 1',
         SQLColumn('bar', data_types.MySQLBool(), default_value=True)),
        ('bar bool default FALSE',
         SQLColumn('bar', data_types.MySQLBool(), default_value=False)),
        ('bar bool default false',
         SQLColumn('bar', data_types.MySQLBool(), default_value=False)),
        ('bar bool default 0',
         SQLColumn('bar', data_types.MySQLBool(), default_value=False)),
        ('bar boolean null',
         SQLColumn('bar', data_types.MySQLBoolean())),
    ])
    def test_create_sql_table_from_sql_stmts_with_boolean_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar decimal(10, 2) default 0.0 unsigned',
         SQLColumn(
             'bar',
             data_types.MySQLDecimal(10, 2, unsigned=True),
             default_value=0.0
         )),
        ('bar double NULL',
         SQLColumn('bar', data_types.MySQLDouble(None, None))),
        ('bar numeric(10) null',
         SQLColumn('bar', data_types.MySQLNumeric(10, None))),
    ])
    def test_create_sql_table_from_sql_stmts_with_real_num_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar char(3) not null',
         SQLColumn('bar', data_types.MySQLChar(3), is_nullable=False)),
        ('bar char not null',
         SQLColumn('bar', data_types.MySQLChar(3), is_nullable=False)),
        ('bar varchar(255) null',
         SQLColumn('bar', data_types.MySQLVarChar(255))),
        ('bar text CHARACTER SET latin1 COLLATE latin1_german1_ci',
         SQLColumn('bar', data_types.MySQLText(
             char_set='latin1',
             collate='latin1_german1_ci'
         ))),
        ('bar char(42) default \'Luke\'',
         SQLColumn('bar', data_types.MySQLChar(42), default_value='Luke')),
        ('bar varchar(42) default \'use\'',
         SQLColumn('bar', data_types.MySQLVarChar(42), default_value='use')),
        ('bar tinytext default \'force!\'',
         SQLColumn('bar', data_types.MySQLTinyText(), default_value='force!')),
        # Note that MySQL BLOB and TEXT cannot have default value so we are
        # intentionally excluding them from such tests. For more information
        # See http://dev.mysql.com/doc/refman/5.5/en/data-type-defaults.html
        # or the discussion on r/119051/
    ])
    def test_create_sql_table_from_sql_stmts_with_string_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar date default \'1000-01-01\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLDate(),
             default_value='1000-01-01',
             is_nullable=False
         )),
        ('bar date null', SQLColumn('bar', data_types.MySQLDate())),
    ])
    def test_create_sql_table_from_sql_stmts_with_date_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar year default \'2000\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLYear(),
             default_value='2000',
             is_nullable=False
         )),
        ('bar year null', SQLColumn('bar', data_types.MySQLYear())),
    ])
    def test_create_sql_table_from_sql_stmts_with_year_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar time default \'11:12:00\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLTime(),
             default_value='11:12:00',
             is_nullable=False
         )),
        ('bar time(2) default \'11:12:00\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLTime(2),
             default_value='11:12:00',
             is_nullable=False
         )),
        ('bar time null', SQLColumn('bar', data_types.MySQLTime())),
    ])
    def test_create_sql_table_from_sql_stmts_with_time_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar timestamp default \'1970-01-01 00:00:01\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLTimestamp(),
             default_value='1970-01-01 00:00:01',
             is_nullable=False
         )),
        ('bar timestamp(2) default \'1970-01-01 00:00:01\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLTimestamp(2),
             default_value='1970-01-01 00:00:01',
             is_nullable=False
         )),
        ('bar timestamp null', SQLColumn('bar', data_types.MySQLTimestamp())),

    ])
    def test_create_sql_table_from_sql_stmts_with_timestamp_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar datetime default \'1000-01-01 00:00:00\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLDateTime(),
             default_value='1000-01-01 00:00:00',
             is_nullable=False
         )),
        ('bar datetime(2) default \'1000-01-01 00:00:00\' not null',
         SQLColumn(
             'bar',
             data_types.MySQLDateTime(2),
             default_value='1000-01-01 00:00:00',
             is_nullable=False
         )),
        ('bar datetime null', SQLColumn('bar', data_types.MySQLDateTime())),
    ])
    def test_create_sql_table_from_sql_stmts_with_datetime_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar binary(64)', SQLColumn('bar', data_types.MySQLBinary(64))),
        ('bar binary', SQLColumn('bar', data_types.MySQLBinary())),
        ('bar varbinary(64)', SQLColumn('bar', data_types.MySQLVarBinary(64))),
        ('bar blob null', SQLColumn('bar', data_types.MySQLBlob())),
        ('bar tinyblob', SQLColumn('bar', data_types.MySQLTinyBlob())),
        ('bar mediumblob', SQLColumn('bar', data_types.MySQLMediumBlob())),
        ('bar longblob', SQLColumn('bar', data_types.MySQLLongBlob())),
        ('bar binary(16) default \'The powerglove\'',
         SQLColumn(
             'bar',
             data_types.MySQLBinary(16),
             default_value='The powerglove'
         )),
        # Note that MySQL BLOB and TEXT cannot have default value so we are
        # intentionally excluding them from such tests. For more information
        # See http://dev.mysql.com/doc/refman/5.5/en/data-type-defaults.html
        # or the discussion on r/119051/
    ])
    def test_create_sql_table_from_sql_stmts_with_binary_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ("bar enum ('a1', \"a2\", 'a3') CHARACTER SET latin1",
         SQLColumn(
             'bar',
             data_types.MySQLEnum(
                 values=['a1', 'a2', 'a3'],
                 char_set="latin1"
             )
         )),
        ("bar enum ('a1', 'a2', 'a3') "
         "CHARACTER SET latin1 COLLATE latin1_german1_ci not null",
         SQLColumn(
             'bar',
             data_types.MySQLEnum(
                 values=['a1', 'a2', 'a3'],
                 char_set="latin1",
                 collate="latin1_german1_ci"
             ),
             is_nullable=False
         )),
        ("bar enum (\"a1\", 'a2', 'a3') default a1",
         SQLColumn(
             'bar',
             data_types.MySQLEnum(['a1', 'a2', 'a3']),
             default_value='a1'
         )),
    ])
    def test_create_sql_table_from_sql_stmts_with_enum_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ("bar set (\"a1\", 'a2', 'a3') CHARACTER SET latin1",
         SQLColumn(
             'bar',
             data_types.MySQLSet(
                 values=['a1', 'a2', 'a3'],
                 char_set="latin1"
             )
         )),
        ("bar set ('a1', 'a2', \"a3\") "
         " CHARACTER SET latin1 COLLATE latin1_german1_ci not null",
         SQLColumn(
             'bar',
             data_types.MySQLSet(
                 values=['a1', 'a2', 'a3'],
                 char_set="latin1",
                 collate="latin1_german1_ci"
             ),
             is_nullable=False
         )),
        ("bar set ('a1', \"a2\", 'a3') default a2",
         SQLColumn(
             'bar',
             data_types.MySQLSet(['a1', 'a2', 'a3']),
             default_value='a2'
         )),
    ])
    def test_create_sql_table_from_sql_stmts_with_set_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_primary_keys(self, handler):
        create_definitions = [
            'id int(11) not null',
            'pid int(11) not null',
            'tag char(3)',
            'primary key(id, pid)'
        ]
        expected_columns = [
            SQLColumn(
                'id',
                data_types.MySQLInt(11),
                is_nullable=False,
                primary_key_order=1
            ),
            SQLColumn(
                'pid',
                data_types.MySQLInt(11),
                is_nullable=False,
                primary_key_order=2
            ),
            SQLColumn('tag', data_types.MySQLChar(3))
        ]
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
        )

    @pytest.mark.parametrize('primary_key_identifier', ['`id`', '"id"'])
    def test_create_sql_table_stmt_with_quoted_primary_keys(
        self,
        handler,
        primary_key_identifier
    ):
        create_definitions = [
            'id int(11) not null',
            '`name` varchar(8)',
            'primary key({})'.format(primary_key_identifier)
        ]
        expected_columns = [
            SQLColumn(
                'id',
                data_types.MySQLInt(11),
                is_nullable=False,
                primary_key_order=1
            ),
            SQLColumn('name', data_types.MySQLVarChar(8))
        ]
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
        )

    def test_create_sql_table_column_types_are_case_insensitive(self, handler):
        create_definitions = [
            'lows int(11)',
            'CAPS INT(11)',
            'MiXeD INt(11)'
        ]
        expected_columns = [
            SQLColumn(
                'lows',
                data_types.MySQLInt(11)
            ),
            SQLColumn(
                'CAPS',
                data_types.MySQLInt(11)
            ),
            SQLColumn(
                'MiXeD',
                data_types.MySQLInt(11)
            ),
        ]
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
        )

    def test_create_sql_table_from_sql_stmts_with_multi_sqls(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.alter_table_sql, self.create_table_sql]
        )
        expected_column = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            is_nullable=False
        )
        expected_table = SQLTable(self.table_name, [expected_column])
        assert expected_table == sql_table

    def test_create_sql_table_from_sql_stmts_with_non_create_table_sql(
        self,
        handler
    ):
        with pytest.raises(ValueError) as e:
            handler.create_sql_table_from_sql_stmts(
                [self.create_table_sql, self.alter_table_sql]
            )
        assert str(e.value).startswith(
            'parsed_stmt should be a create-table statement'
        )

    def test_create_sql_table_from_sql_stmts_with_bad_sql(self, handler):
        sql = 'CREATE TABLE `foo_tbl` `id` int(11) auto_increment;'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert str(e.value).startswith('No column exists in the table.')

    def test_create_sql_table_from_sql_stmts_with_bad_col_type(self, handler):
        sql = 'CREATE TABLE `foo_tbl` (id basscannon(11));'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert 'Unknown MySQL column type basscannon.' == str(e.value)

    def test_create_sql_table_from_sql_stmts_with_no_stmts(self, handler):
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([])
        assert 'Unable to process MySQL statements [].' == str(e.value)

    def test_identifiers_with_quotes(self, handler):
        sql = 'CREATE TABLE `Fo``o` (`ba"r` int, `"ba""z"` int);'
        actual = handler.create_sql_table_from_sql_stmts([sql])
        assert actual.name == 'Fo`o'
        assert actual.columns[0].name == 'ba"r'
        assert actual.columns[1].name == '"ba""z"'

    def test_create_temp_table(self, handler):
        sql = 'create temporary table `foo` (bar int(11));'
        actual = handler.create_sql_table_from_sql_stmts([sql])
        expected = SQLTable('foo', [SQLColumn('bar', data_types.MySQLInt(11))])
        assert actual == expected


def assert_equal_sql_table(self, other):
    """ This exists to aid in debugging test failures, as a simple
    ``self == other`` doesn't give enough information as to _what_
    was different.
    """
    assert self.name == other.name
    for my_column, other_column in izip(self.columns, other.columns):
        assert_equal_sql_column(my_column, other_column)
    assert self.metadata == other.metadata


def assert_equal_sql_column(self, other):
    """ This exists to aid in debugging test failures, as a simple
    ``self == other`` doesn't give enough information as to _what_
    was different.
    """
    assert self.name == other.name
    assert self.type == other.type
    assert self.primary_key_order == other.primary_key_order
    assert self.is_nullable == other.is_nullable
    assert self.default_value == other.default_value
    for my_attribute, other_attribute in izip(
        sorted(self.attributes),
        sorted(other.attributes)
    ):
        assert_equal_sql_attribute(my_attribute, other_attribute)
    assert self.metadata == other.metadata


def assert_equal_sql_attribute(self, other):
    """ This exists to aid in debugging test failures, as a simple
    ``self == other`` doesn't give enough information as to _what_
    was different.
    """
    assert self.name == other.name
    assert self.value == other.value
    assert self.has_value == other.has_value
