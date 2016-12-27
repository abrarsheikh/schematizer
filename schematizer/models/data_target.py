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

from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.consumer_group import ConsumerGroup
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError


class DataTarget(Base, BaseModel):

    __tablename__ = 'data_target'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='name_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)

    target_type = Column(String, nullable=False)
    destination = Column(String, nullable=False)

    consumer_groups = relationship(ConsumerGroup, backref="data_target")

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
        obj = session.query(DataTarget).filter(cls.name == name).one_or_none()
        if obj:
            return obj
        raise EntityNotFoundError(
            entity_desc='{} name `{}`'.format(cls.__name__, name)
        )
