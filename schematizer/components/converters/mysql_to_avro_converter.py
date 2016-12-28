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

from data_pipeline_avro_util.avro_builder import AvroSchemaBuilder
from data_pipeline_avro_util.data_pipeline.avro_meta_data \
    import AvroMetaDataKeys

from schematizer.components.converters.converter_base import BaseConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import mysql_data_types as mysql_types
from schematizer.models import SchemaKindEnum
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLTable


class MySQLToAvroConverter(BaseConverter):
    """Converter that converts MySQL table schema to Avro schema.
    """

    source_type = SchemaKindEnum.MySQL
    target_type = SchemaKindEnum.Avro

    def __init__(self):
        self._builder = AvroSchemaBuilder()

    def convert(self, src_schema):
        """The src_schema is the SQLTable object that represents a MySQL table.
        It returns the Avro schema json object.
        """
        if not src_schema:
            return None
        if not isinstance(src_schema, SQLTable):
            raise SchemaConversionException('SQLTable is expected.')

        return self._create_avro_record_json(src_schema)

    def _create_avro_record_json(self, table):
        namespace = table.metadata.get(MetaDataKey.NAMESPACE, '')
        aliases = table.metadata.get(MetaDataKey.ALIASES)
        metadata = self._get_table_metadata(table)

        self._builder.begin_record(
            table.name,
            namespace=namespace,
            aliases=aliases,
            doc=table.doc,
            **metadata
        )
        for column in table.columns:
            self._create_avro_field(column)
        record_json = self._builder.end()
        return record_json

    def _get_table_metadata(self, table):
        metadata = {}

        primary_keys = [c.name for c in table.primary_keys]
        if primary_keys:
            metadata[AvroMetaDataKeys.PRIMARY_KEY] = primary_keys

        return metadata

    def _create_avro_field(self, column):
        field_type, field_metadata = self._create_avro_field_type(column)
        if column.is_nullable:
            field_type = self._builder.begin_nullable_type(
                field_type,
                column.default_value
            ).end()

        self._builder.add_field(
            column.name,
            field_type,
            has_default=column.default_value is not None or column.is_nullable,
            default_value=column.default_value,
            aliases=column.metadata.get(MetaDataKey.ALIASES),
            doc=column.doc,
            **field_metadata
        )

    def _create_avro_field_type(self, column):
        type_cls = column.type.__class__
        convert_func = self._type_converters.get(type_cls)
        if convert_func:
            return convert_func(column)

        raise UnsupportedTypeException(
            "Unable to convert MySQL data type {} to Avro schema type."
            .format(column.type)
        )

    @property
    def _type_converters(self):
        return {
            mysql_types.MySQLTinyInt: self._convert_small_integer_type,
            mysql_types.MySQLSmallInt: self._convert_small_integer_type,
            mysql_types.MySQLMediumInt: self._convert_small_integer_type,
            mysql_types.MySQLInt: self._convert_integer_type,
            mysql_types.MySQLInteger: self._convert_integer_type,
            mysql_types.MySQLBigInt: self._convert_bigint_type,

            mysql_types.MySQLBit: self._convert_bit_type,

            mysql_types.MySQLBool: self._convert_boolean_type,
            mysql_types.MySQLBoolean: self._convert_boolean_type,

            mysql_types.MySQLFloat: self._convert_float_type,
            mysql_types.MySQLDouble: self._convert_double_type,
            mysql_types.MySQLReal: self._convert_double_type,
            mysql_types.MySQLDecimal: self._convert_decimal_type,
            mysql_types.MySQLNumeric: self._convert_decimal_type,

            mysql_types.MySQLChar: self._convert_char_type,
            mysql_types.MySQLVarChar: self._convert_varchar_type,
            mysql_types.MySQLTinyText: self._convert_tinytext_type,
            mysql_types.MySQLText: self._convert_text_type,
            mysql_types.MySQLMediumText: self._convert_mediumtext_type,
            mysql_types.MySQLLongText: self._convert_longtext_type,

            mysql_types.MySQLDate: self._convert_date_type,
            mysql_types.MySQLDateTime: self._convert_datetime_type,
            mysql_types.MySQLTime: self._convert_time_type,
            mysql_types.MySQLYear: self._convert_year_type,
            mysql_types.MySQLTimestamp: self._convert_timestamp_type,
            mysql_types.MySQLEnum: self._convert_enum_type,

            mysql_types.MySQLBlob: self._convert_blob_type,
            mysql_types.MySQLTinyBlob: self._convert_tinyblob_type,
            mysql_types.MySQLMediumBlob: self._convert_mediumblob_type,
            mysql_types.MySQLLongBlob: self._convert_longblob_type,

            mysql_types.MySQLBinary: self._convert_binary_type,
            mysql_types.MySQLVarBinary: self._convert_varbinary_type,

            mysql_types.MySQLSet: self._convert_set_type,
        }

    def _convert_small_integer_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_int(), metadata

    def _convert_integer_type(self, column):
        # Avro int type is 4-byte signed integer. For MySQL unsigned int type,
        # convert it to Avro long type, which is 8-type signed integer.
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        avro_type = (self._builder.create_long() if column.type.is_unsigned
                     else self._builder.create_int())
        return avro_type, metadata

    def _get_primary_key_metadata(self, primary_key_order):
        return ({AvroMetaDataKeys.PRIMARY_KEY: primary_key_order}
                if primary_key_order else {})

    def _get_unsigned_metadata(self, is_unsigned):
        return ({AvroMetaDataKeys.UNSIGNED: is_unsigned}
                if is_unsigned else {})

    def _convert_bigint_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_long(), metadata

    def _convert_bit_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_bitlen_metadata(column.type.length))
        return self._builder.create_int(), metadata

    def _get_bitlen_metadata(self, bit_length):
        return ({AvroMetaDataKeys.BIT_LEN: bit_length}
                if bit_length else {})

    def _convert_boolean_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return self._builder.create_boolean(), metadata

    def _convert_double_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_double(), metadata

    def _get_precision_metadata(self, column):
        return {
            AvroMetaDataKeys.PRECISION: column.type.precision,
            AvroMetaDataKeys.SCALE: column.type.scale,
        }

    def _convert_float_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_float(), metadata

    def _convert_decimal_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        precision = int(column.type.precision)
        scale = int(column.type.scale)
        return self._builder.begin_decimal_bytes(
            precision,
            scale
        ).end(), metadata

    def _convert_string_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return self._builder.create_string(), metadata

    def _convert_char_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.FIX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _convert_varchar_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _get_fsp_metadata(self, fsp):
        return {
            AvroMetaDataKeys.FSP: fsp
        } if fsp else {}

    def _convert_tinytext_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLTinyText().length
        return self._builder.create_string(), metadata

    def _convert_text_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLText().length
        return self._builder.create_string(), metadata

    def _convert_mediumtext_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = \
            mysql_types.MySQLMediumText().length
        return self._builder.create_string(), metadata

    def _convert_longtext_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLLongText().length
        return self._builder.create_string(), metadata

    def _convert_date_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return self._builder.begin_date().end(), metadata

    def _convert_datetime_type(self, column):
        """The Avro timestamp logical type is used to represent
        MySQL datetime type, and it chooses timestamp-millis or
        timestamp-micros depending on the fsp value.
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.DATETIME] = True
        fsp = column.type.fsp
        metadata.update(self._get_fsp_metadata(fsp))

        return self._builder.create_string(), metadata

    def _convert_time_type(self, column):
        """the long type is used instead of Avro time logical type
        is because the MySQL time type may represent a time interval
        in addition to a time of day. Also, because the value could
        contain fractional part, it expects the value to be converted
        to milli-seconds.
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.TIME] = True
        metadata.update(self._get_fsp_metadata(column.type.fsp))

        return self._builder.create_long(), metadata

    def _convert_year_type(self, column):
        """Avro currently doesn't support year, so map the
        year sql column type to long (year number)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.YEAR] = True
        return self._builder.create_long(), metadata

    def _convert_timestamp_type(self, column):
        """We map to micros over millis for safety
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)

        fsp = column.type.fsp
        metadata.update(self._get_fsp_metadata(fsp))

        if fsp is None or fsp <= 3:
            return self._builder.begin_timestamp_millis().end(), metadata
        else:
            return self._builder.begin_timestamp_micros().end(), metadata

    def _convert_enum_type(self, column):
        return self._builder.begin_enum(
            self.get_enum_type_name(column),
            column.type.values
        ).end(), {}

    def _convert_tinyblob_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLTinyBlob().length
        return self._builder.create_bytes(), metadata

    def _convert_blob_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLBlob().length
        return self._builder.create_bytes(), metadata

    def _convert_mediumblob_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = \
            mysql_types.MySQLMediumBlob().length
        return self._builder.create_bytes(), metadata

    def _convert_longblob_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = mysql_types.MySQLLongBlob().length
        return self._builder.create_bytes(), metadata

    def _convert_binary_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.FIX_LEN] = column.type.length
        return self._builder.create_bytes(), metadata

    def _convert_varbinary_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeys.MAX_LEN] = column.type.length
        return self._builder.create_bytes(), metadata

    def _convert_set_type(self, column):
        schema = self._builder.begin_array(
            self._builder.begin_enum(
                column.name,
                column.type.values
            ).end()
        ).end()
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return schema, metadata

    @classmethod
    def get_enum_type_name(cls, column):
        return column.name + '_enum'
