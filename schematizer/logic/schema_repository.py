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

import re
import uuid

import simplejson
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy.orm import exc as orm_exc

from schematizer import models
from schematizer.config import log
from schematizer.logic import meta_attribute_mappers as meta_attr_repo
from schematizer.logic.schema_resolution import SchemaCompatibilityValidator
from schematizer.logic.validators import verify_entity_exists
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.schema_meta_attribute_mapping import (
    SchemaMetaAttributeMapping
)


try:
    from yelp_conn.mysqldb import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError


def is_backward_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified old_schema_json can be
    deserialized using specified new_schema_json.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        old_schema_json,
        new_schema_json
    )


def is_forward_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified new_schema_json can be
    deserialized using specified old_schema_json.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        new_schema_json,
        old_schema_json
    )


def is_full_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified old_schema_json can be
    deserialized using specified new_schema_json, and vice versa.
    """
    return (is_backward_compatible(old_schema_json, new_schema_json) and
            is_forward_compatible(old_schema_json, new_schema_json))


def register_avro_schema_from_avro_json(
    avro_schema_json,
    namespace_name,
    source_name,
    source_owner_email,
    contains_pii,
    cluster_type,
    status=models.AvroSchemaStatus.READ_AND_WRITE,
    base_schema_id=None,
    docs_required=True
):
    """Add an Avro schema of given schema json object into schema store.
    The steps from checking compatibility to create new topic should be atomic.

    Args:
        avro_schema_json: JSON representation of Avro schema
        namespace_name (str): namespace string
        source_name (str): source name string
        source_owner_email (str): email of the schema owner
        cluster_type (str): Type of kafka cluster Ex: datapipe, scribe, etc.
            See http://y/datapipe_cluster_types for more info on cluster_types.
        status (AvroStatusEnum, optional): RW/R/Disabled
        base_schema_id (int, optional): Id of the Avro schema from which the
            new schema is derived from
        docs_required (bool, optional): whether to-be-registered schema must
            contain doc strings

    Return:
        New created AvroSchema object.
    """
    namespace_name = _strip_if_not_none(namespace_name)
    source_name = _strip_if_not_none(source_name)
    source_owner_email = _strip_if_not_none(source_owner_email)

    _assert_non_empty_name(namespace_name, "Namespace name")
    _assert_non_empty_name(source_name, "Source name")
    _assert_non_empty_name(source_owner_email, "Source owner email")
    _assert_valid_name(namespace_name, "Namespace name")
    _assert_valid_name(source_name, "Source name")

    is_valid, error = models.AvroSchema.verify_avro_schema(avro_schema_json)
    if not is_valid:
        raise ValueError("Invalid Avro schema JSON. Value: {}. Error: {}"
                         .format(avro_schema_json, error))

    if docs_required:
        models.AvroSchema.verify_avro_schema_has_docs(avro_schema_json)

    # This may have false negative, i.e. the schema may be created in the
    # master db but not yet in the slave db. In such situation, the flow
    # then performs the same checking in the master db. The expectation is
    # checking schema in slave db should catch most of the cases when the
    # schema already exists.
    # The `_switch_to_read_only_connection` returns the read-only connection
    # if it is supported, and `None` if not. If the read-only connection is
    # available, the logic flow checks the slave db (read-only) first, and will
    # check the existing schema again in the master db (read-write) for
    # false-negative case. If the read-only db is not supported, the logic flow
    # will check in the regular db directly.
    # TODO [clin|DATAPIPE-2165] nice to have test to verify right db is used.
    read_only_conn = _switch_to_read_only_connection()
    if read_only_conn:
        with read_only_conn:
            source_id, topic_candidates = _get_source_id_and_topic_candidates(
                namespace_name,
                source_name,
                base_schema_id,
                contains_pii,
                cluster_type
            )
            the_schema = _get_schema_if_exists(
                avro_schema_json, topic_candidates, source_id
            )
            if the_schema:
                return the_schema

    # TODO [DATAPIPE-1852]: the table locking doesn't seem to work correctly.
    # The race condition still occurs.
    namespace = _get_namespace_or_create(namespace_name)
    _lock_namespace(namespace)

    source = _get_source_or_create(
        namespace.id,
        source_name.strip(),
        source_owner_email.strip()
    )
    _lock_source(source)

    # If the connection is switched to read-only one above, it still needs to
    # checks again in the master db to catch false-negative cases.
    source_id, topic_candidates = _get_source_id_and_topic_candidates(
        namespace_name,
        source_name,
        base_schema_id,
        contains_pii,
        cluster_type
    )
    the_schema = _get_schema_if_exists(
        avro_schema_json, topic_candidates, source_id, lock=True
    )
    if the_schema:
        return the_schema

    most_recent_topic = topic_candidates[0] if topic_candidates else None
    if not _is_candidate_topic_compatible(
        topic=most_recent_topic,
        avro_schema_json=avro_schema_json,
        contains_pii=contains_pii
    ):
        most_recent_topic = _create_topic_for_source(
            namespace_name=namespace_name,
            source=source,
            contains_pii=contains_pii,
            cluster_type=cluster_type
        )
    return _create_avro_schema(
        avro_schema_json=avro_schema_json,
        source_id=source.id,
        topic_id=most_recent_topic.id,
        status=status,
        base_schema_id=base_schema_id
    )


def register_schema_alias(schema_id, alias):
    """Add an alias to a registered schema.

    Args:
        schema_id (int): The schema id to associate the alias with
        alias (string): The name of the alias

    Returns:
        :class:models.schema_alias.SchemaAlias:
            the newly created object.

    Raises:
        :class:schematizer.models.exceptions.EntityNotFoundError: If the given
            schema id is invalid.
    """
    verify_entity_exists(session, models.AvroSchema, schema_id)

    returned_schema = session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).first()

    topic_id = returned_schema.topic_id
    source_id = get_source_id_by_topic_id(topic_id)

    return models.SchemaAlias.create(
        session,
        source_id=source_id,
        schema_id=schema_id,
        alias=alias
    )


def _strip_if_not_none(original_str):
    if not original_str:
        return original_str
    return original_str.strip()


def _switch_to_read_only_connection():
    try:
        return session.slave_connection_set
    except AttributeError:
        return None


def _get_source_id_and_topic_candidates(
    namespace_name,
    source_name,
    base_schema_id,
    contains_pii,
    cluster_type
):
    # This currently will exclude the topics that only have disabled schemas.
    source = get_source_by_fullname(namespace_name, source_name)
    if not source:
        return None, []

    query = session.query(models.Topic).join(models.AvroSchema).filter(
        models.Topic.source_id == source.id,
        models.Topic.cluster_type == cluster_type,
        models.Topic.contains_pii == contains_pii,
        models.AvroSchema.base_schema_id == base_schema_id,
        models.Topic.id == models.AvroSchema.topic_id,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(
        models.Topic.id.desc()
    )
    if not base_schema_id:
        query = query.limit(1)
    return source.id, query.all()


def _get_schema_if_exists(
    new_schema_json, topic_candidates, source_id, lock=False
):
    if not topic_candidates:
        return None

    meta_attr_mappings = {
        o for o in meta_attr_repo.get_meta_attributes_by_source(source_id)
    }

    db_type = "Master" if lock else "Slave"
    for topic in topic_candidates:
        if lock:
            _lock_topic_and_schemas(topic.id)
        avro_schemas = _get_schemas_by_topic_id(topic.id)
        log.info('[{}] Checking {} schemas in topic {} for {}.'.format(
            db_type, len(avro_schemas), topic.name, new_schema_json
        ))
        for (schema_id, avro_schema) in avro_schemas:
            is_same_schema = _is_same_schema(
                schema_id, avro_schema, new_schema_json, meta_attr_mappings
            )
            if is_same_schema:
                log.info(
                    '[{}] Found existing schema {} in topic {} for {}.'.format(
                        db_type, schema_id, topic.name, new_schema_json
                    )
                )
                return models.AvroSchema.get_by_id(schema_id)

    log.info('[{}] Cannot find existing schema in source {} for {}.'.format(
        db_type, source_id, new_schema_json
    ))
    return None


def _get_schemas_by_topic_id(topic_id):
    """Get id and avro schema of all the active schemas in the given topic.
    Note that this function only returns schema id and schema itself instead
    of entire AvroSchema object.
    """
    results = session.query(
        models.AvroSchema.id,
        models.AvroSchema.avro_schema
    ).filter(
        models.AvroSchema.topic_id == topic_id,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(models.AvroSchema.id.desc()).all()
    return [(result[0], simplejson.loads(result[1])) for result in results]


def _is_same_schema(
    existing_schema_id,
    existing_schema_json,
    new_schema_json,
    meta_attr_mappings
):
    if existing_schema_json != new_schema_json:
        return False

    schema_meta_attrs = session.query(
        SchemaMetaAttributeMapping.meta_attr_schema_id
    ).filter(
        SchemaMetaAttributeMapping.schema_id == existing_schema_id
    ).all()

    return meta_attr_mappings == {o[0] for o in schema_meta_attrs}


def _are_meta_attr_mappings_same(schema_id, source_id):
    return (set(get_meta_attributes_by_schema_id(schema_id)) ==
            set(meta_attr_repo.get_meta_attributes_by_source(source_id)))


def _is_candidate_topic_compatible(topic, avro_schema_json, contains_pii):
    return (topic and
            topic.contains_pii == contains_pii and
            is_schema_compatible_in_topic(avro_schema_json, topic) and
            _is_pkey_identical(avro_schema_json, topic.name))


def _create_topic_for_source(
    namespace_name,
    source,
    contains_pii,
    cluster_type
):
    # Note that creating duplicate topic names will throw a sqlalchemy
    # IntegrityError exception. When it occurs, it indicates the uuid
    # is generating the same value (rarely) and we'd like to know it.
    # Per SEC-5079, sqlalchemy IntegrityError now is replaced with yelp-conn
    # IntegrityError.
    topic_name = _construct_topic_name(namespace_name, source.name)
    return _create_topic(topic_name, source.id, contains_pii, cluster_type)


def _construct_topic_name(namespace, source):
    topic_name = '__'.join((namespace, source, uuid.uuid4().hex))
    return re.sub('[^\w-]', '_', topic_name)


def _create_topic(topic_name, source_id, contains_pii, cluster_type):
    """Create a topic named `topic_name` in the given source.
    It returns a newly created topic. If a topic with the same
    name already exists, an exception is thrown
    """
    topic = models.Topic(
        name=topic_name,
        source_id=source_id,
        contains_pii=contains_pii,
        cluster_type=cluster_type
    )
    session.add(topic)
    session.flush()
    return topic


def _get_namespace_or_create(namespace_name):
    try:
        return session.query(
            models.Namespace
        ).filter(
            models.Namespace.name == namespace_name
        ).one()
    except orm_exc.NoResultFound:
        return _create_namespace_if_not_exist(namespace_name)


def _get_source_or_create(namespace_id, source_name, owner_email):
    try:
        return session.query(
            models.Source
        ).filter(
            models.Source.namespace_id == namespace_id,
            models.Source.name == source_name
        ).one()
    except orm_exc.NoResultFound:
        return _create_source_if_not_exist(
            namespace_id,
            source_name,
            owner_email
        )


def _create_namespace_if_not_exist(namespace_name):
    try:
        # Create a savepoint before trying to create new namespace so that
        # in the case which the IntegrityError occurs, the session will
        # rollback to savepoint. Upon exiting the nested Context, commit/
        # rollback is automatically issued and no need to add it explicitly
        with session.begin_nested():
            new_namespace = models.Namespace(name=namespace_name)
            session.add(new_namespace)
    except (IntegrityError, exc.IntegrityError):
        # Ignore this error due to trying to create a duplicate namespace
        # TODO [clin|DATAPIPE-1471] see if there is a way to only handle one
        # exception or the other.
        new_namespace = get_namespace_by_name(namespace_name)
    return new_namespace


def _create_source_if_not_exist(namespace_id, source_name, owner_email):
    try:
        # Create a savepoint before trying to create new source so that
        # in the case which the IntegrityError occurs, the session will
        # rollback to savepoint. Upon exiting the nested Context, commit/
        # rollback is automatically issued and no need to add it explicitly
        with session.begin_nested():
            new_source = models.Source(
                namespace_id=namespace_id,
                name=source_name,
                owner_email=owner_email
            )
            session.add(new_source)
    except (IntegrityError, exc.IntegrityError):
        # Ignore this error due to trying to create a duplicate source
        # TODO [clin|DATAPIPE-1471] see if there is a way to only handle one
        # exception or the other.
        new_source = _get_source_by_namespace_id_and_src_name(
            namespace_id,
            source_name
        )
    return new_source


def _assert_non_empty_name(name, name_type):
    if not name:
        raise ValueError('{} must be non-empty.'.format(name_type))


def _assert_valid_name(name, name_type):
    if not name:
        return
    if '|' in name:
        # Restrict '|' to avoid ambiguity when parsing input of
        # data_pipeline tailer. One of the tailer arguments is topic
        # and optional offset separated by '|'.
        raise ValueError(
            '{} must not contain restricted character |'.format(name_type)
        )
    if name.isdigit():
        raise ValueError('{} must not be numeric.'.format(name_type))


def _get_source_by_namespace_id_and_src_name(namespace_id, source):
    return session.query(
        models.Source
    ).filter(
        models.Source.namespace_id == namespace_id,
        models.Source.name == source
    ).first()


def _lock_namespace(namespace):
    session.query(
        models.Namespace
    ).filter(
        models.Namespace.id == namespace.id
    ).with_for_update()


def _lock_source(source):
    session.query(
        models.Source
    ).filter(
        models.Source.id == source.id
    ).with_for_update()


def _lock_topic_and_schemas(topic_id):
    if not topic_id:
        return
    session.query(
        models.Topic
    ).filter(
        models.Topic.id == topic_id
    ).with_for_update()
    session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic_id
    ).with_for_update()


def get_latest_topic_of_namespace_source(namespace_name, source_name):
    source = get_source_by_fullname(namespace_name, source_name)
    if not source:
        raise EntityNotFoundError(
            entity_cls=models.Source,
            entity_desc='namespace {} source {}'.format(
                namespace_name,
                source_name
            )
        )
    return session.query(models.Topic).filter(
        models.Topic.source_id == source.id
    ).order_by(models.Topic.id.desc()).first()


def is_schema_compatible_in_topic(target_schema, topic):
    """Check whether given schema is a valid Avro schema and compatible
    with existing schemas in the specified topic. Note that target_schema
    is the avro json object.
    """
    enabled_schemas = get_schemas_by_topic_name(topic.name)
    for enabled_schema in enabled_schemas:
        schema_json = simplejson.loads(enabled_schema.avro_schema)
        if (not is_full_compatible(schema_json, target_schema) or
            not _are_meta_attr_mappings_same(
                enabled_schema.id,
                topic.source_id
        )):
            return False
    return True


def _is_pkey_identical(new_schema_json, topic_name):
    """Check whether given schema has not mutated any primary key.
    """
    old_schema_json = get_latest_schema_by_topic_name(
        topic_name
    ).avro_schema_json
    old_pkey_set = set(
        (old_field['name'], old_field['pkey'])
        for old_field in old_schema_json.get('fields', [])
        if old_field.get('pkey')
    )
    new_pkey_set = set(
        (new_field['name'], new_field['pkey'])
        for new_field in new_schema_json.get('fields', [])
        if new_field.get('pkey')
    )
    return old_pkey_set == new_pkey_set


def get_namespace_by_name(namespace):
    return session.query(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace
    ).first()


def get_source_by_fullname(namespace_name, source_name):
    return session.query(
        models.Source
    ).join(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace_name,
        models.Source.name == source_name
    ).first()


def _create_avro_schema(
    avro_schema_json,
    source_id,
    topic_id,
    status=models.AvroSchemaStatus.READ_AND_WRITE,
    base_schema_id=None
):
    avro_schema_elements = models.AvroSchema.create_schema_elements_from_json(
        avro_schema_json
    )

    avro_schema = models.AvroSchema(
        avro_schema_json=avro_schema_json,
        topic_id=topic_id,
        status=status,
        base_schema_id=base_schema_id
    )
    session.add(avro_schema)
    session.flush()

    for avro_schema_element in avro_schema_elements:
        avro_schema_element.avro_schema_id = avro_schema.id
        session.add(avro_schema_element)

    session.flush()
    _add_meta_attribute_mappings(avro_schema.id, source_id)
    return avro_schema


def get_latest_schema_by_topic_id(topic_id):
    """Get the latest enabled (Read-Write or Read-Only) schema of given topic.
    It returns None if no such schema can be found.
    """
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic_id,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(
        models.AvroSchema.id.desc()
    ).first()


def get_latest_schema_by_topic_name(topic_name):
    """Get the latest enabled (Read-Write or Read-Only) schema of given topic.
    It returns None if no such schema can be found.
    """
    topic = models.Topic.get_by_name(topic_name)
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic.id,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(
        models.AvroSchema.id.desc()
    ).first()


def is_schema_compatible(target_schema, namespace, source):
    """Check whether given schema is a valid Avro schema. It then determines
    the topic of given Avro schema belongs to and checks the compatibility
    against the existing schemas in this topic. Note that given target_schema
    is expected as Avro json object.
    """
    topic = get_latest_topic_of_namespace_source(namespace, source)
    if not topic:
        return True
    return is_schema_compatible_in_topic(target_schema, topic)


def get_schemas_by_topic_name(topic_name, include_disabled=False):
    topic = models.Topic.get_by_name(topic_name)
    qry = session.query(models.AvroSchema).filter(
        models.AvroSchema.topic_id == topic.id
    )
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    return qry.order_by(models.AvroSchema.id).all()


def get_schemas_by_topic_id(topic_id, include_disabled=False):
    """Get all the Avro schemas of specified topic. Default it excludes
    disabled schemas. Set `include_disabled` to True to include disabled ones.
    """
    qry = session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic_id
    )
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    return qry.order_by(models.AvroSchema.id).all()


def get_topics_by_source_id(source_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.source_id == source_id
    ).order_by(
        models.Topic.id
    ).all()


def get_source_id_by_topic_id(topic_id):
    topic = session.query(
        models.Topic
    ).filter(
        models.Topic.id == topic_id
    ).first()
    return topic.source_id


def get_latest_topic_of_source_id(source_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.source_id == source_id
    ).order_by(
        models.Topic.id.desc()
    ).first()


def list_refreshes_by_source_id(source_id):
    return session.query(
        models.Refresh
    ).filter(
        models.Refresh.source_id == source_id
    ).order_by(
        models.Refresh.id
    ).all()


def create_refresh(
        source_id,
        offset,
        batch_size,
        priority,
        filter_condition,
        avg_rows_per_second_cap
):
    refresh = models.Refresh(
        source_id=source_id,
        offset=offset,
        batch_size=batch_size,
        priority=priority,
        filter_condition=filter_condition,
        avg_rows_per_second_cap=avg_rows_per_second_cap
    )
    session.add(refresh)
    session.flush()
    return refresh


def get_schema_elements_by_schema_id(schema_id):
    return session.query(
        models.AvroSchemaElement
    ).filter(
        models.AvroSchemaElement.avro_schema_id == schema_id
    ).order_by(
        models.AvroSchemaElement.id
    ).all()


def get_meta_attributes_by_schema_id(schema_id):
    """Logic Method to list the schema_ids of all meta attributes registered to
    the specified schema id. Invalid schema id will raise an
    EntityNotFoundError exception"""
    models.AvroSchema.get_by_id(schema_id)
    mappings = session.query(
        SchemaMetaAttributeMapping
    ).filter(
        SchemaMetaAttributeMapping.schema_id == schema_id
    ).all()
    return [m.meta_attr_schema_id for m in mappings]


def _add_meta_attribute_mappings(schema_id, source_id):
    mappings = []
    for meta_attr_schema_id in meta_attr_repo.get_meta_attributes_by_source(
        source_id
    ):
        new_mapping = SchemaMetaAttributeMapping(
            schema_id=schema_id,
            meta_attr_schema_id=meta_attr_schema_id
        )
        session.add(new_mapping)
        mappings.append(new_mapping)
    session.flush()
    return mappings


def get_topics_by_criteria(
    namespace=None,
    source=None,
    created_after=None,
    page_info=None
):
    """Get all the topics that match given criteria, including namespace,
    source, and/or topic created timestamp.

    This function supports pagination, i.e. caller can specify miniumum topic
    id and page size to get single chunk of topics.

    Args:
        namespace(Optional[str]): get topics of given namespace if specified
        source(Optional[str]): get topics of given source name if specified
        created_after(Optional[datetime]): get topics created after given utc
            datetime (inclusive) if specified.
        page_info(Optional[:class:schematizer.models.page_info.PageInfo]):
            limits the topics to count and those with id greater than or
            equal to min_id.

    Returns:
        (list[:class:schematizer.models.Topic]): List of topics sorted by
        their ids.
    """
    qry = session.query(models.Topic)
    if namespace or source:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Topic.source_id
        )
    if namespace:
        qry = qry.join(models.Namespace).filter(
            models.Namespace.name == namespace,
            models.Namespace.id == models.Source.namespace_id,
        )
    if source:
        qry = qry.filter(models.Source.name == source)
    if created_after is not None:
        qry = qry.filter(models.Topic.created_at >= created_after)

    min_id = page_info.min_id if page_info else 0
    qry = qry.filter(models.Topic.id >= min_id)

    qry = qry.order_by(models.Topic.id)
    if page_info and page_info.count:
        qry = qry.limit(page_info.count)
    return qry.all()


def get_schemas_by_criteria(
    namespace_name=None,
    source_name=None,
    created_after=None,
    include_disabled=False,
    page_info=None
):
    """Get avro schemas that match the specified criteria, including namespace,
    source, schema created timestamp, and/or schema status.

    This function supports pagination, i.e. caller can specify minimum schema
    id and page size to get single chunk of schemas.

    Args:
        namespace(Optional[str]): get schemas of given namespace if specified
        source(Optional[str]): get schemas of given source name if specified
        created_after(Optional[int]): get schemas created after given unix
            timestamp (inclusive) if specified
        included_disabled(Optional[bool]): whether to include disabled schemas
        page_info(Optional[:class:schematizer.models.page_info.PageInfo]):
            limits the schemas to count and those with id greater than or
            equal to min_id.

    Returns:
        (list[:class:schematizer.models.AvroSchema]): List of avro schemas
        sorted by their ids.
    """
    qry = session.query(models.AvroSchema)
    if created_after is not None:
        qry = qry.filter(models.AvroSchema.created_at >= created_after)
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    min_id = page_info.min_id if page_info else 0
    qry = qry.filter(models.AvroSchema.id >= min_id)

    qry = qry.order_by(models.AvroSchema.id)
    if page_info and page_info.count:
        qry = qry.limit(page_info.count)

    if namespace_name or source_name:
        qry = qry.join(
            models.Topic,
            models.Source
        ).filter(
            models.AvroSchema.topic_id == models.Topic.id,
            models.Topic.source_id == models.Source.id,
        )
    if namespace_name:
        qry = qry.join(models.Namespace).filter(
            models.Source.namespace_id == models.Namespace.id,
            models.Namespace.name == namespace_name
        )
    if source_name:
        qry = qry.filter(models.Source.name == source_name)
    return qry.all()


def get_refreshes_by_criteria(
    namespace=None,
    source_name=None,
    status=None,
    created_after=None,
    updated_after=None
):
    """Get all the refreshes that match the given filter criteria.

    Args:
        namespace(str, optional): get refreshes of given namespace if specified
        source_name(str, optional): get refreshes of given source if specified
        status(str, optional): get refreshes of given status if specified
        created_after(int, optional): get refreshes created after given unix
            timestamp (inclusive) if specified.
        updated_after(int, optional): get refreshes updated after given unix
            timestamp (inclusive) if specified.
    """
    qry = session.query(models.Refresh)
    if namespace:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Refresh.source_id
        )
        qry = qry.join(models.Namespace).filter(
            models.Namespace.name == namespace,
            models.Namespace.id == models.Source.namespace_id
        )
    if source_name:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Refresh.source_id,
            models.Source.name == source_name
        )
    if status:
        qry = qry.filter(models.Refresh.status == status)
    if created_after is not None:
        qry = qry.filter(models.Refresh.created_at >= created_after)
    if updated_after is not None:
        qry = qry.filter(models.Refresh.updated_at >= updated_after)
    return qry.order_by(
        desc(models.Refresh.priority)
    ).order_by(
        models.Refresh.id
    ).all()
