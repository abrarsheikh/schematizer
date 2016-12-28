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

from avro import schema
from data_pipeline_avro_util.data_pipeline.avro_meta_data \
    import AvroMetaDataKeys

from schematizer.components.converters.converter_base import BaseConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import redshift_data_types
from schematizer.models import SchemaKindEnum
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class AvroToRedshiftConverter(BaseConverter):
    """Converter that converts Avro json schema to Redshift table schema.
    """

    source_type = SchemaKindEnum.Avro
    target_type = SchemaKindEnum.Redshift

    def convert(self, src_schema):
        """The src_schema is the Avro schema json object. It returns the
        SQLTable object that represents the Redshift table schema.

        Note that Redshift does not support unsigned column type. For now,
        `unsigned` metadata will be ignored and a signed column type will
        be used instead.
        """
        # TODO[clin|DATAPIPE-101] adding sortkey/distkey

        if not src_schema:
            return None
        try:
            avro_record = schema.make_avsc_object(src_schema)
        except:
            raise SchemaConversionException('Invalid Avro record schema.')

        if not self._is_record_schema(avro_record):
            raise SchemaConversionException('Invalid Avro record schema.')

        return self._create_redshift_table(avro_record)

    def _is_record_schema(self, avro_schema):
        return isinstance(avro_schema, schema.RecordSchema)

    def _create_redshift_table(self, record_schema):
        cols = [self._create_column(field) for field in record_schema.fields]
        table_metadata = self._get_table_metadata(record_schema)
        return SQLTable(
            record_schema.name,
            columns=cols,
            doc=record_schema.doc,
            # TODO(chohan|DATAPIPE-1133): Define this property in
            # AvroMetaDataKeys in data_pipeline_avro_util and update
            # this line accordingly.
            schema_name=record_schema.get_prop('schema_name'),
            **table_metadata
        )

    def _create_column(self, field):
        column_type = self._create_column_type(field)
        is_nullable = self._is_column_nullable(field)
        metadata = self._get_column_metadata(field)
        return SQLColumn(
            field.name,
            column_type,
            primary_key_order=self._get_primary_key_order(field),
            is_nullable=is_nullable,
            default_value=field.default if field.has_default else None,
            attributes=None,
            doc=field.doc,
            **metadata
        )

    def _create_column_type(self, field):
        field_type = self._get_field_type(field)
        column_type = self._convert_field_type(field_type, field)
        return column_type

    def _get_field_type(self, field):
        if self._is_union_schema(field.type):
            return next((sub_type for sub_type in field.type.schemas
                         if not self._is_null_type(sub_type)), None)
        return field.type

    def _is_column_nullable(self, field):
        types_to_exam = (field.type.schemas
                         if self._is_union_schema(field.type)
                         else (field.type,))
        return any(self._is_null_type(typ) for typ in types_to_exam)

    def _is_null_type(self, avro_schema):
        return (self._is_primitive_schema(avro_schema) and
                avro_schema.fullname == 'null')

    def _is_primitive_schema(self, avro_schema):
        return isinstance(avro_schema, schema.PrimitiveSchema)

    def _is_union_schema(self, avro_schema):
        return isinstance(avro_schema, schema.UnionSchema)

    def _is_logical_schema(self, avro_schema):
        return isinstance(avro_schema, schema.LogicalSchema)

    def _is_complex_schema(self, avro_schema):
        # The RecordSchema type is excluded because the Redshift converter
        # doesn't support nested table schemas.
        return isinstance(
            avro_schema, (
                schema.ArraySchema,
                schema.EnumSchema,
                schema.FixedSchema,
                schema.MapSchema,
                schema.UnionSchema
            )
        )

    def _convert_field_type(self, field_type, field):
        # TODO(chohan|DATAPIPE-1999): Revisit the conversion logic here to
        # handle avro schemas in a more general way.
        is_complex = False
        if self._is_primitive_schema(field_type):
            typ = field_type.fullname
        elif self._is_complex_schema(field_type):
            typ = field_type.type
            is_complex = True
        else:
            typ = field_type

        converter_func = self._type_converters.get(typ)

        if self._is_logical_schema(field_type):
            logical_converter_func = self._logical_type_converters.get(
                field_type.props.get('logicalType')
            )
            if logical_converter_func:
                return logical_converter_func(field_type)

        if converter_func:
            return converter_func(
                field_type if is_complex else field
            )

        raise UnsupportedTypeException(
            "Unable to convert field {0} type {1} to Redshift column type."
            .format(field.name, field_type)
        )

    @property
    def _type_converters(self):
        return {
            'null': self._convert_null_type,
            'int': self._convert_int_type,
            'long': self._convert_long_type,
            'float': self._convert_float_type,
            'double': self._convert_double_type,
            'string': self._convert_string_type,
            'boolean': self._convert_boolean_type,
            'enum': self._convert_enum_type,
            'bytes': self._convert_bytes_type,
        }

    def _convert_null_type(self, field):
        raise SchemaConversionException(
            "Redshift column type cannot be `null`."
        )

    def _convert_int_type(self, field):
        return redshift_data_types.RedshiftInteger()

    def _convert_long_type(self, field):
        is_timestamp = AvroMetaDataKeys.TIMESTAMP in field.props
        return (redshift_data_types.RedshiftTimestamp() if is_timestamp
                else redshift_data_types.RedshiftBigInt())

    def _convert_float_type(self, field):
        return redshift_data_types.RedshiftReal()

    def _convert_double_type(self, field):
        is_fixed_point = AvroMetaDataKeys.FIXED_POINT in field.props
        if is_fixed_point:
            length, decimal = self._get_precision_metadata(field)
            return redshift_data_types.RedshiftDecimal(length, decimal)
        return redshift_data_types.RedshiftDouble()

    def _get_precision_metadata(self, field):
        return (field.props.get(AvroMetaDataKeys.PRECISION),
                field.props.get(AvroMetaDataKeys.SCALE))

    # 2 bytes per char is currently chosen as the trade-off between
    # support multi-byte char in Redshift and performance/space usage.
    # It is also the current settings used in the datawarehouse.
    CHAR_BYTES = 2

    # http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html
    MAX_VARCHAR_BYTES = 65535

    def _convert_string_type(self, field, char_bytes=CHAR_BYTES):
        """Only supports char and varchar. If neither fix_len nor max_len
        is specified, an exception is thrown.
        """
        fix_len = field.props.get(AvroMetaDataKeys.FIX_LEN)
        if fix_len:
            # Columns with a CHAR data type only accept single-byte UTF-8
            # characters. This means we can't support char columns since they
            # will not be able to accept multibyte chars. We can instead use
            # VARCHAR columns, which accept multibyte UTF-8 characters.
            return redshift_data_types.RedshiftVarChar(fix_len)

        max_len = field.props.get(AvroMetaDataKeys.MAX_LEN)
        if max_len:
            return redshift_data_types.RedshiftVarChar(
                min(int(max_len) * char_bytes, self.MAX_VARCHAR_BYTES)
            )

        raise SchemaConversionException(
            "Unable to convert `string` type without metadata {0} or {1}."
            .format(AvroMetaDataKeys.FIX_LEN, AvroMetaDataKeys.MAX_LEN)
        )

    def _convert_bytes_type(self, field):
        return self._convert_string_type(field, char_bytes=1)

    def _convert_boolean_type(self, field):
        return redshift_data_types.RedshiftBoolean()

    def _convert_enum_type(self, field):
        max_symbol_len = max(len(symbol) for symbol in field.symbols)
        return redshift_data_types.RedshiftVarChar(
            min(max_symbol_len, self.MAX_VARCHAR_BYTES)
        )

    @property
    def _logical_type_converters(self):
        return {
            'date': self._convert_date_type,
            'decimal': self._convert_decimal_type,
            'timestamp-millis': self._convert_timestamp_millis_type
        }

    def _convert_date_type(self, field):
        return redshift_data_types.RedshiftDate()

    def _convert_decimal_type(self, field):
        precision, scale = self._get_precision_metadata(field)
        return redshift_data_types.RedshiftDecimal(precision, scale)

    def _convert_timestamp_millis_type(self, field):
        return redshift_data_types.RedshiftTimestampTz()

    def _get_table_metadata(self, record_schema):
        table_metadata = ({MetaDataKey.NAMESPACE: record_schema.namespace}
                          if record_schema.namespace is not None else {})
        table_metadata.update(self._get_aliases_metadata(record_schema.props))
        return table_metadata

    def _get_aliases_metadata(self, props):
        return ({MetaDataKey.ALIASES: props.get(MetaDataKey.ALIASES)}
                if props.get('aliases') else {})

    def _get_column_metadata(self, field):
        return self._get_aliases_metadata(field.props)

    def _get_primary_key_order(self, field):
        return field.props.get(AvroMetaDataKeys.PRIMARY_KEY)
