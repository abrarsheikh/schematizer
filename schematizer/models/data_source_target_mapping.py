# -*- coding: utf-8 -*-
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

    # Timestamp when the entry is created
    created_at = Column(Integer, nullable=False, default=func.unix_timestamp())

    # Timestamp when the entry is last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=func.unix_timestamp(),
        onupdate=func.unix_timestamp()
    )
