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

from collections import deque

import simplejson
from avro import schema
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from schematizer.models.avro_schema_element import AvroSchemaElement
from schematizer.models.base_model import BaseModel
from schematizer.models.consumer import Consumer
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.note import Note
from schematizer.models.note import ReferenceTypeEnum
from schematizer.models.producer import Producer


class AvroSchemaStatus(object):

    READ_AND_WRITE = 'RW'
    READ_ONLY = 'R'
    DISABLED = 'Disabled'


class AvroSchema(Base, BaseModel):

    __tablename__ = 'avro_schema'

    id = Column(Integer, primary_key=True)

    # The JSON string representation of the avro schema.
    avro_schema = Column('avro_schema', Text, nullable=False)

    # Id of the topic that the schema is associated to.
    # It is a foreign key to Topic table.
    topic_id = Column(Integer, ForeignKey('topic.id'), nullable=False)

    # The schema_id where this schema is derived from.
    base_schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    # alias of the schema. (namespace, source, alias) combination uniquely
    # identifies a schema.
    alias = Column(String, default=None)

    # Schema status: RW (read/write), R (read-only), Disabled
    status = Column(
        Enum(
            AvroSchemaStatus.READ_AND_WRITE,
            AvroSchemaStatus.READ_ONLY,
            AvroSchemaStatus.DISABLED,
            name='status'
        ),
        default=AvroSchemaStatus.READ_AND_WRITE,
        nullable=False
    )

    # Timestamp when the entry is created
    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    # Timestamp when the entry is last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )

    producers = relationship(Producer, backref="avro_schema")

    consumers = relationship(Consumer, backref="avro_schema")

    avro_schema_elements = relationship(
        AvroSchemaElement,
        backref="avro_schema"
    )

    @property
    def note(self):
        note = session.query(
            Note
        ).filter(
            Note.reference_type == ReferenceTypeEnum.SCHEMA,
            Note.reference_id == self.id
        ).first()
        return note

    @property
    def avro_schema_json(self):
        return simplejson.loads(self.avro_schema)

    @avro_schema_json.setter
    def avro_schema_json(self, schema_json):
        self.avro_schema = simplejson.dumps(schema_json, sort_keys=True)

    @property
    def avro_schema_with_doc(self):
        """Get the JSON representation of the Avro schema with the
        documentation and element Id of each doc-eligible element.
        """
        key_to_element_map = dict(
            (o.key, o) for o in self.avro_schema_elements
        )
        avro_schema_obj = schema.make_avsc_object(self.avro_schema_json)

        schema_elements = deque([(avro_schema_obj, None)])
        while len(schema_elements) > 0:
            schema_obj, parent_key = schema_elements.popleft()
            element_cls = _schema_to_element_map.get(schema_obj.__class__)
            if not element_cls:
                continue
            _schema_element = element_cls(schema_obj, parent_key)
            self._add_doc_to_schema(_schema_element, key_to_element_map)

            parent_key = _schema_element.key
            for nested_schema in _schema_element.nested_schema_objects:
                schema_elements.append((nested_schema, parent_key))

        return avro_schema_obj.to_json()

    ELEMENT_ID_ATTR = 'element_id'
    DOC_ATTR = 'doc'

    def _add_doc_to_schema(self, schema_element, key_to_element_map):
        element = key_to_element_map.get(schema_element.key)
        if not element:
            return

        schema_element.schema_obj.set_prop(self.DOC_ATTR, element.doc)
        schema_element.schema_obj.set_prop(self.ELEMENT_ID_ATTR, element.id)

    @classmethod
    def create_schema_elements_from_json(cls, avro_schema_json):
        """Get all the schema elements that exist in the given schema JSON.
        :param avro_schema_json: JSON representation of an Avro schema
        :return: List of AvroSchemaElement objects
        """

        avro_schema_elements = []
        elements = cls._create_schema_elements_from_json(avro_schema_json)
        for _schema_element, schema_obj in elements:
            avro_schema_element = AvroSchemaElement(
                key=_schema_element.key,
                element_type=_schema_element.element_type,
                doc=schema_obj.get_prop('doc')
            )
            avro_schema_elements.append(avro_schema_element)

        return avro_schema_elements

    @classmethod
    def _create_schema_elements_from_json(cls, avro_schema_json):
        avro_schema_obj = schema.make_avsc_object(avro_schema_json)
        schema_elements = []
        schema_elements_queue = deque([(avro_schema_obj, None)])
        while schema_elements_queue:
            schema_obj, parent_key = schema_elements_queue.popleft()
            element_cls = _schema_to_element_map.get(schema_obj.__class__)
            if not element_cls:
                continue

            _schema_element = element_cls(schema_obj, parent_key)
            schema_elements.append((_schema_element, schema_obj))
            parent_key = _schema_element.key
            for nested_schema in _schema_element.nested_schema_objects:
                schema_elements_queue.append((nested_schema, parent_key))
        return schema_elements

    @classmethod
    def verify_avro_schema(cls, avro_schema_json):
        """Verify whether the given JSON representation is a valid Avro schema.

        :param avro_schema_json: JSON representation of the Avro schema
        :return: A tuple (is_valid, error) in which the first element
        indicates whether the given JSON is a valid Avro schema, and the
        second element is the error if it is not valid.
        """
        try:
            schema.make_avsc_object(avro_schema_json)
            return True, None
        except Exception as e:
            return False, repr(e)

    @classmethod
    def verify_avro_schema_has_docs(cls, avro_schema_json):
        """ Verify if the given Avro schema has docs.
        According to avro spec `doc` is supported by `record` type,
        all fields within the `record` and `enum`.

        :param avro_schema_json: JSON representation of the Avro schema

        :raises ValueError: avro_schema_json with missing docs
        :raises Exception: invalid avro_schema_json
        """
        elements = cls._create_schema_elements_from_json(
            avro_schema_json
        )
        schema_elements_missing_doc = [
            schema_element.key
            for schema_element, schema_obj in elements
            if not schema_element.has_docs_if_supported
        ]

        if schema_elements_missing_doc:
            # TODO DATAPIPE-970  implement better exception response during
            # registering avro schema with missing docs
            raise ValueError("Missing `doc` for Schema Elements(s) {}".format(
                ', '.join(schema_elements_missing_doc)
            ))


class _SchemaElement(object):
    """Helper class that wraps the avro schema object and its corresponding
    element type.
    """

    target_schema_type = None
    element_type = None
    support_doc = None

    def __init__(self, schema_obj, parent_key):
        if not isinstance(schema_obj, self.target_schema_type):
            raise ValueError("schema_obj must be {0}. Value: {1}".format(
                self.target_schema_type.__class__.__name__,
                schema_obj
            ))
        self.schema_obj = schema_obj
        self.parent_key = parent_key

    @property
    def key(self):
        raise NotImplementedError()

    @property
    def nested_schema_objects(self):
        return []

    @property
    def has_docs_if_supported(self):
        if not self.support_doc:
            return True
        doc = self.schema_obj.get_prop('doc')
        if doc and doc.strip():
            return True
        return False


class _RecordSchemaElement(_SchemaElement):

    target_schema_type = schema.RecordSchema
    element_type = 'record'
    support_doc = True

    @property
    def key(self):
        return self.schema_obj.fullname

    @property
    def nested_schema_objects(self):
        return self.schema_obj.fields


class _FieldElement(_SchemaElement):

    target_schema_type = schema.Field
    element_type = 'field'
    support_doc = True

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.schema_obj.name
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.type]


class _EnumSchemaElement(_SchemaElement):

    target_schema_type = schema.EnumSchema
    element_type = 'enum'
    support_doc = True

    @property
    def key(self):
        return self.schema_obj.fullname


class _FixedSchemaElement(_SchemaElement):

    target_schema_type = schema.FixedSchema
    element_type = 'fixed'
    support_doc = False

    @property
    def key(self):
        return self.schema_obj.fullname


class _ArraySchemaElement(_SchemaElement):

    target_schema_type = schema.ArraySchema
    element_type = 'array'
    support_doc = False

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.element_type
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.items]


class _MapSchemaElement(_SchemaElement):

    target_schema_type = schema.MapSchema
    element_type = 'map'
    support_doc = False

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.element_type
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.values]


_schema_to_element_map = dict(
    (o.target_schema_type, o) for o in _SchemaElement.__subclasses__()
)
