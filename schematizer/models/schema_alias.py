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

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base


class SchemaAlias(Base, BaseModel):

    __tablename__ = 'schema_alias'
    __table_args__ = (
        UniqueConstraint(
            'namespace_id',
            'source_id',
            'alias',
            name='namespace_id_source_id_alias_unique_constraint',
        ),
    )

    id = Column(Integer, primary_key=True)

    # The (namespace, source, alias) combination maps to a single schema_id
    namespace_id = Column(Integer, ForeignKey('namespace.id'), nullable=False)
    source_id = Column(Integer, ForeignKey('source.id'), nullable=False)
    alias = Column(String, nullable=False)

    schema_id = Column(Integer, ForeignKey('avro_schema.id'), nullable=False)

    # Timestamp when the entry is created
    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    # Timestamp when the entry is last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )
