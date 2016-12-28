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
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.source import Source


class Namespace(Base, BaseModel):

    __tablename__ = 'namespace'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='namespace_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Namespace, such as "yelpmain.db", etc
    name = Column(String, nullable=False)

    sources = relationship(Source, backref="namespace")

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
        obj = session.query(Namespace).filter(cls.name == name).one_or_none()
        if obj:
            return obj
        raise EntityNotFoundError(
            entity_desc='{} name `{}`'.format(cls.__name__, name)
        )

    def get_sources(self, page_info=None):
        qry = session.query(
            Source
        ).filter(Source.namespace_id == self.id)
        if page_info and page_info.min_id:
            qry = qry.filter(
                Source.id >= page_info.min_id
            )
        qry = qry.order_by(Source.id)
        if page_info and page_info.count:
            qry = qry.limit(page_info.count)
        return qry.all()
