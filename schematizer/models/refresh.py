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

from enum import Enum
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base


class Priority(Enum):

    LOW = 25
    MEDIUM = 50
    HIGH = 75
    MAX = 100


class RefreshStatus(Enum):

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    PAUSED = "PAUSED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class Refresh(Base, BaseModel):

    __tablename__ = 'refresh'

    id = Column(Integer, primary_key=True)

    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    status = Column(
        String,
        default=RefreshStatus.NOT_STARTED.value,
        nullable=False
    )

    # Represents the last known position that has been refreshed.
    offset = Column(Integer, default=0, nullable=False)

    max_primary_key = Column(Integer, default=None)

    batch_size = Column(Integer, default=100, nullable=False)

    priority = Column(
        Integer,
        default=Priority.MEDIUM.value,
        nullable=False
    )

    # This field contains the expression used to filter the records
    # that must be refreshed. E.g. It may be a MySQL where clause
    # if the source of the refresh is a MySQL table.
    filter_condition = Column(String, default=None)

    avg_rows_per_second_cap = Column(
        Integer,
        default=None
    )

    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )
