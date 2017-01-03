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

from data_pipeline_avro_util.data_pipeline.avro_meta_data \
    import AvroMetaDataKeys
from sqlalchemy import Boolean
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


class Topic(Base, BaseModel):

    __tablename__ = 'topic'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='topic_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Topic name.
    name = Column(String, nullable=False)

    # The associated source_id for this topic.
    source_id = Column(Integer, ForeignKey('source.id'), nullable=False)

    avro_schemas = relationship(AvroSchema, backref="topic")

    # Since mysql doesn't have boolean type, sqlalchemy converts the boolean
    # value to integer 0/1 when storing into the db.
    # (http://docs.sqlalchemy.org/en/latest/core/type_basics.html#
    # sqlalchemy.types.Boolean)
    contains_pii = Column(Boolean, nullable=False)

    cluster_type = Column(String, nullable=False)

    @property
    def primary_keys(self):
        if not self.avro_schemas:
            return []

        return self.avro_schemas[0].avro_schema_json.get(
            AvroMetaDataKeys.PRIMARY_KEY,
            []
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

    @classmethod
    def get_by_name(cls, name):
        obj = session.query(cls).filter(cls.name == name).one_or_none()
        if obj:
            return obj
        raise EntityNotFoundError(
            entity_desc='{} name `{}`'.format(cls.__name__, name)
        )
