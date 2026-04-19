"""
Resource Groups Service Emulator.
REST/JSON protocol — path-based routing.

Supports: CreateGroup, DeleteGroup, GetGroup, GetGroupConfiguration,
         GetGroupQuery, GetTags, GroupResources, ListGroupResources,
         ListGroups, PutGroupConfiguration, SearchResources, Tag,
         UngroupResources, Untag, UpdateGroup, UpdateGroupQuery.
"""

import copy
import json
import logging
import os
import time
import uuid

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id

logger = logging.getLogger("resource-groups")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


_groups = AccountScopedDict()  # name -> group record
_group_resources = AccountScopedDict()  # name -> set of resource ARNs


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state():
    return copy.deepcopy({
        "groups": _groups,
        "group_resources": _group_resources,
    })


def restore_state(data):
    _groups.update(data.get("groups", {}))
    _group_resources.update(data.get("group_resources", {}))


_restored = load_state("resource_groups")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error(status, code, message):
    body = json.dumps({"__type": code, "Message": message}).encode()
    return status, {"Content-Type": "application/json"}, body


def _group_arn(name):
    return f"arn:aws:resource-groups:{REGION}:{get_account_id()}:group/{name}"


def _json(status, data):
    body = json.dumps(data, default=str).encode()
    return status, {"Content-Type": "application/json"}, body


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _resolve_group(body):
    """Resolve a group by GroupName or Group (ARN) from the request body."""
    group_name = body.get("GroupName") or body.get("Group")
    if not group_name:
        return None, None
    # If an ARN was provided, extract the name from it.
    if group_name.startswith("arn:"):
        group_name = group_name.rsplit("/", 1)[-1]
    record = _groups.get(group_name)
    return group_name, record


def _resource_type_from_arn(arn):
    """Best-effort extraction of resource type from an ARN."""
    parts = arn.split(":")
    if len(parts) >= 3:
        service = parts[2]
        resource = parts[-1].split("/")[0] if "/" in parts[-1] else ""
        return f"AWS::{service}::{resource}" if resource else f"AWS::{service}"
    return "AWS::Unknown"


# ---------------------------------------------------------------------------
# CreateGroup — POST /groups
# ---------------------------------------------------------------------------


def _create_group(body):
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    if name in _groups:
        return _error(409, "ConflictException",
                      f"A resource group with name '{name}' already exists")

    resource_query = body.get("ResourceQuery", {})
    tags = body.get("Tags", {})
    description = body.get("Description", "")
    configuration = body.get("Configuration", [])

    group_record = {
        "GroupArn": _group_arn(name),
        "Name": name,
        "Description": description,
        "OwnerId": get_account_id(),
    }
    _groups[name] = {
        "Group": group_record,
        "ResourceQuery": resource_query,
        "Tags": tags,
        "Configuration": configuration,
    }

    logger.info("CreateGroup: %s", name)

    result = {"Group": group_record}
    if resource_query:
        result["ResourceQuery"] = resource_query
    if tags:
        result["Tags"] = tags
    if configuration:
        result["GroupConfiguration"] = {
            "Configuration": configuration,
            "Status": "UPDATE_COMPLETE",
        }
    return _json(200, result)


# ---------------------------------------------------------------------------
# DeleteGroup — POST /delete-group
# ---------------------------------------------------------------------------


def _delete_group(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    del _groups[group_name]
    _group_resources.pop(group_name, None)
    logger.info("DeleteGroup: %s", group_name)
    return _json(200, {"Group": record["Group"]})


# ---------------------------------------------------------------------------
# GetGroup — POST /get-group
# ---------------------------------------------------------------------------


def _get_group(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    result = {"Group": record["Group"]}
    if record.get("Configuration"):
        result["GroupConfiguration"] = {
            "Configuration": record["Configuration"],
            "Status": "UPDATE_COMPLETE",
        }
    return _json(200, result)


# ---------------------------------------------------------------------------
# GetGroupConfiguration — POST /get-group-configuration
# ---------------------------------------------------------------------------


def _get_group_configuration(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    return _json(200, {
        "GroupConfiguration": {
            "Configuration": record.get("Configuration", []),
            "ProposedConfiguration": [],
            "Status": "UPDATE_COMPLETE",
        },
    })


# ---------------------------------------------------------------------------
# GetGroupQuery — POST /get-group-query
# ---------------------------------------------------------------------------


def _get_group_query(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    rq = record.get("ResourceQuery", {})
    if not rq:
        return _error(400, "BadRequestException",
                      f"Group '{group_name}' does not have a resource query")
    return _json(200, {
        "GroupQuery": {
            "GroupName": group_name,
            "ResourceQuery": rq,
        },
    })


# ---------------------------------------------------------------------------
# GetTags — GET /resources/{Arn}/tags
# ---------------------------------------------------------------------------


def _get_tags(arn):
    for record in _groups.values():
        if record["Group"]["GroupArn"] == arn:
            return _json(200, {
                "Arn": arn,
                "Tags": record.get("Tags", {}),
            })
    return _error(404, "NotFoundException",
                  f"No resource with ARN '{arn}'")


# ---------------------------------------------------------------------------
# Tag — PUT /resources/{Arn}/tags
# ---------------------------------------------------------------------------


def _tag(arn, body):
    tags = body.get("Tags", {})
    if not tags:
        return _error(400, "BadRequestException", "Tags are required")
    for record in _groups.values():
        if record["Group"]["GroupArn"] == arn:
            record.setdefault("Tags", {}).update(tags)
            logger.info("Tag: %s +%d tags", arn, len(tags))
            return _json(200, {"Arn": arn, "Tags": record["Tags"]})
    return _error(404, "NotFoundException",
                  f"No resource with ARN '{arn}'")


# ---------------------------------------------------------------------------
# Untag — PATCH /resources/{Arn}/tags
# ---------------------------------------------------------------------------


def _untag(arn, body):
    keys = body.get("Keys", [])
    if not keys:
        return _error(400, "BadRequestException", "Keys are required")
    for record in _groups.values():
        if record["Group"]["GroupArn"] == arn:
            for k in keys:
                record.get("Tags", {}).pop(k, None)
            logger.info("Untag: %s -%d keys", arn, len(keys))
            return _json(200, {"Arn": arn, "Keys": keys})
    return _error(404, "NotFoundException",
                  f"No resource with ARN '{arn}'")


# ---------------------------------------------------------------------------
# GroupResources — POST /group-resources
# ---------------------------------------------------------------------------


def _group_resources(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    arns = body.get("ResourceArns", [])
    if not arns:
        return _error(400, "BadRequestException",
                      "ResourceArns is required")
    existing = _group_resources.setdefault(group_name, [])
    succeeded = []
    failed = []
    for arn in arns:
        if arn in existing:
            failed.append({
                "ResourceArn": arn,
                "ErrorCode": "AlreadyExists",
                "ErrorMessage": "Resource already in group",
            })
        else:
            existing.append(arn)
            succeeded.append(arn)
    logger.info("GroupResources: %s +%d", group_name, len(succeeded))
    return _json(200, {
        "Succeeded": succeeded,
        "Failed": failed,
        "Pending": [],
    })


# ---------------------------------------------------------------------------
# ListGroupResources — POST /list-group-resources
# ---------------------------------------------------------------------------


def _list_group_resources(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    arns = _group_resources.get(group_name, [])
    max_results = int(body.get("MaxResults", 50))
    identifiers = []
    for arn in arns[:max_results]:
        identifiers.append({
            "ResourceArn": arn,
            "ResourceType": _resource_type_from_arn(arn),
        })
    return _json(200, {
        "ResourceIdentifiers": identifiers,
        "Resources": [{"Identifier": i} for i in identifiers],
    })


# ---------------------------------------------------------------------------
# ListGroups — POST /groups-list  or  GET /groups
# ---------------------------------------------------------------------------


def _list_groups(body, query):
    max_results = int(body.get("MaxResults", query.get("maxResults", 50)))

    filters = body.get("Filters", [])
    items = list(_groups.values())

    # Apply filters (resource-type filter on ResourceQuery.Type).
    for f in filters:
        f_name = f.get("Name", "")
        f_values = f.get("Values", [])
        if f_name == "resource-type" and f_values:
            items = [
                g for g in items
                if g.get("ResourceQuery", {}).get("Type") in f_values
            ]
        if f_name == "configuration-type" and f_values:
            items = [
                g for g in items
                if any(
                    c.get("Type") in f_values
                    for c in g.get("Configuration", [])
                )
            ]

    identifiers = []
    groups = []
    for g in items[:max_results]:
        grp = g["Group"]
        identifiers.append({
            "GroupName": grp["Name"],
            "GroupArn": grp["GroupArn"],
        })
        groups.append(grp)

    return _json(200, {
        "GroupIdentifiers": identifiers,
        "Groups": groups,
    })


# ---------------------------------------------------------------------------
# PutGroupConfiguration — POST /put-group-configuration
# ---------------------------------------------------------------------------


def _put_group_configuration(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    configuration = body.get("Configuration", [])
    record["Configuration"] = configuration
    logger.info("PutGroupConfiguration: %s", group_name)
    return _json(202, {})


# ---------------------------------------------------------------------------
# SearchResources — POST /resources-search
# ---------------------------------------------------------------------------


def _search_resources(body):
    resource_query = body.get("ResourceQuery", {})
    max_results = int(body.get("MaxResults", 50))
    # Stub: return an empty result set. A real implementation would
    # evaluate the query against actual resources.
    return _json(200, {
        "ResourceIdentifiers": [],
        "QueryErrors": [],
    })


# ---------------------------------------------------------------------------
# UngroupResources — POST /ungroup-resources
# ---------------------------------------------------------------------------


def _ungroup_resources(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    arns = body.get("ResourceArns", [])
    if not arns:
        return _error(400, "BadRequestException",
                      "ResourceArns is required")
    existing = _group_resources.get(group_name, [])
    succeeded = []
    failed = []
    for arn in arns:
        if arn in existing:
            existing.remove(arn)
            succeeded.append(arn)
        else:
            failed.append({
                "ResourceArn": arn,
                "ErrorCode": "NotFound",
                "ErrorMessage": "Resource not in group",
            })
    logger.info("UngroupResources: %s -%d", group_name, len(succeeded))
    return _json(200, {
        "Succeeded": succeeded,
        "Failed": failed,
        "Pending": [],
    })


# ---------------------------------------------------------------------------
# UpdateGroup — POST /update-group
# ---------------------------------------------------------------------------


def _update_group(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    if "Description" in body:
        record["Group"]["Description"] = body["Description"]
    logger.info("UpdateGroup: %s", group_name)
    return _json(200, {"Group": record["Group"]})


# ---------------------------------------------------------------------------
# UpdateGroupQuery — POST /update-group-query
# ---------------------------------------------------------------------------


def _update_group_query(body):
    group_name, record = _resolve_group(body)
    if not group_name:
        return _error(400, "BadRequestException",
                      "GroupName is required")
    if not record:
        return _error(404, "NotFoundException",
                      f"No resource group with name '{group_name}'")
    resource_query = body.get("ResourceQuery")
    if not resource_query:
        return _error(400, "BadRequestException",
                      "ResourceQuery is required")
    record["ResourceQuery"] = resource_query
    logger.info("UpdateGroupQuery: %s", group_name)
    return _json(200, {
        "GroupQuery": {
            "GroupName": group_name,
            "ResourceQuery": resource_query,
        },
    })


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


def reset():
    _groups.clear()
    _group_resources.clear()


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body_bytes, query_params):
    query = {k: (v[0] if isinstance(v, list) else v)
             for k, v in query_params.items()}

    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    # CreateGroup — POST /groups
    if path == "/groups" and method == "POST":
        return await _a(_create_group(body))

    # DeleteGroup — POST /delete-group
    if path == "/delete-group" and method == "POST":
        return await _a(_delete_group(body))

    # GetGroup — POST /get-group
    if path == "/get-group" and method == "POST":
        return await _a(_get_group(body))

    # GetGroupConfiguration — POST /get-group-configuration
    if path == "/get-group-configuration" and method == "POST":
        return await _a(_get_group_configuration(body))

    # GetGroupQuery — POST /get-group-query
    if path == "/get-group-query" and method == "POST":
        return await _a(_get_group_query(body))

    # GetTags — GET /resources/{arn}/tags
    if path.startswith("/resources/") and path.endswith("/tags") \
            and method == "GET":
        arn = path[len("/resources/"):-len("/tags")]
        return await _a(_get_tags(arn))

    # Tag — PUT /resources/{arn}/tags
    if path.startswith("/resources/") and path.endswith("/tags") \
            and method == "PUT":
        arn = path[len("/resources/"):-len("/tags")]
        return await _a(_tag(arn, body))

    # Untag — PATCH /resources/{arn}/tags
    if path.startswith("/resources/") and path.endswith("/tags") \
            and method == "PATCH":
        arn = path[len("/resources/"):-len("/tags")]
        return await _a(_untag(arn, body))

    # GroupResources — POST /group-resources
    if path == "/group-resources" and method == "POST":
        return await _a(_group_resources(body))

    # ListGroupResources — POST /list-group-resources
    if path == "/list-group-resources" and method == "POST":
        return await _a(_list_group_resources(body))

    # ListGroups — POST /groups-list  or  GET /groups
    if path == "/groups-list" and method == "POST":
        return await _a(_list_groups(body, query))
    if path == "/groups" and method == "GET":
        return await _a(_list_groups({}, query))

    # PutGroupConfiguration — POST /put-group-configuration
    if path == "/put-group-configuration" and method == "POST":
        return await _a(_put_group_configuration(body))

    # SearchResources — POST /resources-search
    if path == "/resources-search" and method == "POST":
        return await _a(_search_resources(body))

    # UngroupResources — POST /ungroup-resources
    if path == "/ungroup-resources" and method == "POST":
        return await _a(_ungroup_resources(body))

    # UpdateGroup — POST /update-group
    if path == "/update-group" and method == "POST":
        return await _a(_update_group(body))

    # UpdateGroupQuery — POST /update-group-query
    if path == "/update-group-query" and method == "POST":
        return await _a(_update_group_query(body))

    return _error(400, "BadRequestException",
                  f"Unknown Resource Groups path: {method} {path}")


async def _a(result):
    return result
