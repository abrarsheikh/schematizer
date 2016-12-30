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

import uuid

from schematizer import models
from schematizer.models.avro_schema import AvroSchema
from schematizer.models.data_source_target_mapping import (
    DataSourceTargetMapping
)
from schematizer.models.data_target import DataTarget
from schematizer.models.database import session
from schematizer.models.meta_attribute_mapping_store import (
    MetaAttributeMappingStore
)
from schematizer.models.namespace import Namespace
from schematizer.models.refresh import Priority
from schematizer.models.refresh import Refresh
from schematizer.models.refresh import RefreshStatus
from schematizer.models.schema_meta_attribute_mapping import (
    SchemaMetaAttributeMapping
)
from schematizer.models.source import Source
from schematizer.models.topic import Topic


def _create_entity(session, entity):
    session.add(entity)
    session.flush()
    return entity


def create_namespace(namespace_name):
    return _create_entity(session, Namespace(name=namespace_name))


def get_or_create_namespace(namespace_name):
    namespace = session.query(Namespace).filter(
        Namespace.name == namespace_name
    ).first()
    return namespace or create_namespace(namespace_name)


def create_source(namespace_name, source_name, owner_email=None):
    namespace = get_or_create_namespace(namespace_name)
    return _create_entity(
        session,
        entity=Source(
            namespace_id=namespace.id,
            name=source_name,
            owner_email=owner_email or 'test@example.com'
        )
    )


def get_or_create_source(namespace_name, source_name, owner_email=None):
    source = session.query(Source).join(Namespace).filter(
        models.Namespace.name == namespace_name,
        models.Source.name == source_name
    ).first()
    return source or create_source(namespace_name, source_name, owner_email)


def create_topic(topic_name, namespace_name, source_name, **overrides):
    """Create a topic with specified topic name in the Topic table.  For topic
    attributes to override, see :class:schematizer.models.topic.Topic.
    """
    source = get_or_create_source(namespace_name, source_name)
    params = {
        'name': topic_name,
        'source_id': source.id,
        'contains_pii': False,
        'cluster_type': 'datapipe'
    }
    params.update(overrides)
    return _create_entity(session, Topic(**params))


def get_or_create_topic(topic_name, namespace_name=None, source_name=None):
    topic = session.query(Topic).filter(Topic.name == topic_name).first()
    return topic or create_topic(topic_name, namespace_name, source_name)


def create_avro_schema(
    schema_json,
    schema_elements=None,
    topic_name='default_topic_name',
    namespace='default_namespace',
    source="default_source",
    **overrides
):
    topic = get_or_create_topic(topic_name, namespace, source)

    params = {
        'avro_schema_json': schema_json,
        'topic_id': topic.id,
        'status': models.AvroSchemaStatus.READ_AND_WRITE,
        'base_schema_id': None
    }
    if overrides:
        params.update(overrides)
    avro_schema = _create_entity(session, AvroSchema(**params))

    schema_elements = (
        schema_elements or
        AvroSchema.create_schema_elements_from_json(schema_json)
    )
    for schema_element in schema_elements:
        schema_element.avro_schema_id = avro_schema.id
        session.add(schema_element)
    session.flush()

    return avro_schema


def create_note(reference_type, reference_id, note_text, last_updated_by):
    return _create_entity(
        session,
        models.Note(
            reference_type=reference_type,
            reference_id=reference_id,
            note=note_text,
            last_updated_by=last_updated_by
        )
    )


def create_refresh(source_id, **overrides):
    params = {
        'source_id': source_id,
        'offset': 0,
        'batch_size': 100,
        'priority': Priority.MEDIUM.value,
        'status': RefreshStatus.NOT_STARTED.value,
        'filter_condition': None,
        'avg_rows_per_second_cap': 200
    }
    if overrides:
        params.update(overrides)
    return _create_entity(session, Refresh(**params))


def create_source_category(source_id, category):
    return _create_entity(
        session,
        models.SourceCategory(source_id=source_id, category=category)
    )


def create_data_target(name, target_type, destination):
    return _create_entity(
        session,
        DataTarget(name=name, target_type=target_type, destination=destination)
    )


def get_or_create_data_target(name, target_type, destination):
    entity = session.query(DataTarget).filter(DataTarget.name == name).first()
    return entity or create_data_target(name, target_type, destination)


def create_data_source_target_mapping(source_type, source_id, target_id):
    return _create_entity(
        session,
        DataSourceTargetMapping(
            data_source_type=source_type,
            data_source_id=source_id,
            data_target_id=target_id
        )
    )


def create_consumer_group(group_name, data_target):
    return _create_entity(
        session,
        models.ConsumerGroup(
            group_name=group_name,
            data_target_id=data_target.id
        )
    )


def create_consumer_group_data_source(
    consumer_group,
    data_src_type,
    data_src_id
):
    return _create_entity(
        session,
        models.ConsumerGroupDataSource(
            consumer_group_id=consumer_group.id,
            data_source_type=data_src_type,
            data_source_id=data_src_id
        )
    )


def create_meta_attribute_mapping(
    meta_attr_schema_id,
    entity_type,
    entity_id
):
    return _create_entity(
        session,
        MetaAttributeMappingStore(
            entity_type=entity_type,
            entity_id=entity_id,
            meta_attr_schema_id=meta_attr_schema_id
        )
    )


def create_schema_meta_attr_mapping(schema_id, meta_attr_id):
    return _create_entity(
        session,
        SchemaMetaAttributeMapping(
            schema_id=schema_id,
            meta_attr_schema_id=meta_attr_id
        )
    )


def generate_name(prefix=None):
    return '{}{}'.format(prefix or '', uuid.uuid4().hex)
