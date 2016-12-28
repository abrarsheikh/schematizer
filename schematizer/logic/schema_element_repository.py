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

from sqlalchemy.orm import exc as orm_exc

from schematizer import models
from schematizer.logic import exceptions as sch_exc
from schematizer.models.database import session


def get_element_chains_by_schema_id(schema_id):
    """Build the element chain for each schema element in the given schema.
    The elements in the same element chain represent the same entity, such
    as same field, same column, etc, from the previous version schemas of
    the same source.

    Note that if a schema that is newer than the given schema exists, the
    element chains will not include the elements from the newer schema.

    :param schema_id: the Avro schema Id.
    :return: List of element chains ([[schematizer.models.AvroSchemaElement]].
    Each element chain is a list of schema elements sorted by their timestamp
    reversely: the list starts with the most recent schema element, i.e. the
    element of given schema.
    """
    try:
        avro_schema = models.AvroSchema.get_by_id(schema_id)
    except orm_exc.NoResultFound:
        raise sch_exc.EntityNotFoundException(
            "Cannot find Avro schema id {0}.".format(schema_id)
        )

    identity_to_element_chain_map = _initialize_schema_element_chains(
        avro_schema.avro_schema_elements
    )
    elements = _get_schema_elements_by_source(
        avro_schema.topic.source_id,
        no_later_than_schema_id=schema_id
    )

    schema_start_idx = 0
    missing_identities = set()
    for i, element in enumerate(elements):
        if element.avro_schema_id != elements[schema_start_idx].avro_schema_id:
            _update_schema_element_chains(
                identity_to_element_chain_map,
                elements[schema_start_idx:i],
                missing_identities
            )
            schema_start_idx = i
    _update_schema_element_chains(
        identity_to_element_chain_map,
        elements[schema_start_idx:],
        missing_identities
    )
    return identity_to_element_chain_map.values()


def _initialize_schema_element_chains(elements):
    if not elements:
        return {}
    return dict((_get_schema_element_identity(e), [e]) for e in elements)


def _get_schema_element_identity(element):
    """The identity of an element is the value used to see if two schema
    elements represent the same entity (field, column, etc.).
    """
    return element.key


def _get_schema_elements_by_source(source_id, no_later_than_schema_id=None):
    """Get all the schema elements of the schemas that belong to specified
    source. If `no_later_than_schema_id` is specified, only the elements of
    the schemas that are earlier than the specified schema id will be returned.

    :param source_id:
    :param no_later_than_schema_id:
    :return: A list of schema elements sorted by the creation timestamp of
    their enclosed schema reversely, i.e. the list starts with the elements of
    the latest schema.
    """
    qry = session.query(models.AvroSchemaElement).join(
        models.AvroSchema,
        models.Topic,
    ).filter(
        models.AvroSchemaElement.avro_schema_id == models.AvroSchema.id,
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.source_id == source_id
    )
    if no_later_than_schema_id:
        qry = qry.filter(models.AvroSchema.id < no_later_than_schema_id)
    qry = qry.order_by(models.AvroSchema.id.desc())

    return qry.all()


def _update_schema_element_chains(
    identity_to_element_chain_map,
    elements,
    missing_identities
):
    """If the identity of an element in the given elements is the same as
    the key of the identity_to_element_chain_map, the element is added to
    the corresponding element chain.

    Note that if an element with same identity appear in two schemas, and
    these two schemas are not consecutive version schemas, the element of
    newer version schema will not be added to the chain. `missing_identities`
    is used to track such elements.
    """
    missing_identities_this_round = set(identity_to_element_chain_map.keys())
    for element in elements:
        identity = _get_schema_element_identity(element)
        chain = identity_to_element_chain_map.get(identity)
        if chain and identity not in missing_identities:
            chain.append(element)
            missing_identities_this_round.remove(identity)
    missing_identities.update(missing_identities_this_round)
