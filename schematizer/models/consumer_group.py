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
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.consumer_group_data_source \
    import ConsumerGroupDataSource
from schematizer.models.database import Base


class ConsumerGroup(Base, BaseModel):

    __tablename__ = 'consumer_group'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False, unique=True)
    data_target_id = Column(
        Integer,
        ForeignKey('data_target.id'),
        nullable=False
    )

    data_sources = relationship(
        ConsumerGroupDataSource,
        backref="consumer_group"
    )

    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )
