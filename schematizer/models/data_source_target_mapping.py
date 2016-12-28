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

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base


class DataSourceTargetMapping(Base, BaseModel):
    __tablename__ = 'data_source_target_mapping'

    id = Column(Integer, primary_key=True)

    # Id of the data source that maps to one or more data targets
    data_source_id = Column(Integer, nullable=False)

    # The data source type (Namespace, Source, Schema)
    data_source_type = Column(String, nullable=False)

    # Id of the data target that maps to one or more data sources
    data_target_id = Column(Integer, nullable=False)

    # Timestamp when the entry was created
    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    # Timestamp when the entry was last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )
