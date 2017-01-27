# -*- coding: utf-8 -*-
# Copyright 2017 Yelp Inc.
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

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.avro_schema import AvroSchema
from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.namespace import Namespace
from schematizer.models.source import Source


class SchemaAlias(Base, BaseModel):

    __tablename__ = 'schema_alias'
    __table_args__ = (
        UniqueConstraint(
            'source_id',
            'alias',
            name='source_id_alias_unique_constraint',
        ),
    )

    id = Column(Integer, primary_key=True)

    # (namespace_name, source_name, alias) map to a single schema_id
    # and since source_id maps to a single (namespace_name, source_name),
    # we use (source_id, alias) here
    source_id = Column(Integer, ForeignKey('source.id'), nullable=False)
    alias = Column(String, nullable=False)

    schema_id = Column(Integer, ForeignKey('avro_schema.id'), nullable=False)
    schema = relationship(AvroSchema, uselist=False)

    # Timestamp when the entry is created
    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    # Timestamp when the entry is last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )

    @classmethod
    def get_by_ns_src_alias(cls, namespace_name, source_name, alias):
        schema_alias = session.query(
            SchemaAlias
        ).join(
            Source
        ).join(
            Namespace
        ).filter(
            Namespace.name == namespace_name,
            Source.name == source_name,
            SchemaAlias.alias == alias
        ).one_or_none()

        if schema_alias:
            return schema_alias

        raise EntityNotFoundError(
            entity_desc='{} namespace `{}`, source `{}`, alias `{}`'.format(
                cls.__name__,
                namespace_name,
                source_name,
                alias,
            )
        )
