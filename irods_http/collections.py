"""Collection operations for iRODS HTTP API."""

import json

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def create(session: IRODSHTTPSession, lpath: str, create_intermediates: int = 0) -> dict:
	"""
	Create a new collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to be created.
	    create_intermediates: Set to 1 to create intermediates, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(create_intermediates)

	data = {
		"op": "create",
		"lpath": lpath,
		"create-intermediates": create_intermediates,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove(session: IRODSHTTPSession, lpath: str, recurse: int = 0, no_trash: int = 0) -> dict:
	"""
	Remove an existing collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to be removed.
	    recurse: Set to 1 to remove contents of the collection, otherwise set to 0. Defaults to 0.
	    no_trash: Set to 1 to permanently remove, 0 to move to trash.  Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(recurse)
	common.validate_0_or_1(no_trash)

	data = {
		"op": "remove",
		"lpath": lpath,
		"recurse": recurse,
		"no-trash": no_trash,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def stat(session: IRODSHTTPSession, lpath: str, ticket: str = "") -> dict:
	"""
	Give information about a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection being accessed.
	    ticket: Ticket to be enabled before the operation. Defaults to an empty string.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(ticket, str)

	params = {"op": "stat", "lpath": lpath, "ticket": ticket}

	r = requests.get(session.url_base + "/collections", params=params, headers=session.get_headers)  # noqa: S113
	return common.process_response(r)


def list_collection(session: IRODSHTTPSession, lpath: str, recurse: int = 0, ticket: str = "") -> dict:
	"""
	Show the contents of a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to have its contents listed.
	    recurse: Set to 1 to list the contents of objects in the collection,
	      otherwise set to 0. Defaults to 0.
	    ticket: Ticket to be enabled before the operation. Defaults to an empty string.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(recurse)
	common.validate_instance(ticket, str)

	params = {"op": "list", "lpath": lpath, "recurse": recurse, "ticket": ticket}

	r = requests.get(session.url_base + "/collections", params=params, headers=session.get_headers)  # noqa: S113
	return common.process_response(r)


def set_permission(
	session: IRODSHTTPSession,
	lpath: str,
	entity_name: str,
	permission: str,
	admin: int = 0,
) -> dict:
	"""
	Set the permission of a user for a given collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to have a permission set.
	    entity_name: The name of the user or group having its permission set.
	    permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If permission is not 'null', 'read', 'write', or 'own'.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(entity_name, str)
	common.validate_instance(permission, str)
	if permission not in ["null", "read", "write", "own"]:
		raise ValueError("permission must be either 'null', 'read', 'write', or 'own'")
	common.validate_0_or_1(admin)

	data = {
		"op": "set_permission",
		"lpath": lpath,
		"entity-name": entity_name,
		"permission": permission,
		"admin": admin,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def set_inheritance(session: IRODSHTTPSession, lpath: str, enable: int, admin: int = 0) -> dict:
	"""
	Set the inheritance for a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to have its inheritance set.
	    enable: Set to 1 to enable inheritance, or 0 to disable.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(enable)
	common.validate_0_or_1(admin)

	data = {
		"op": "set_inheritance",
		"lpath": lpath,
		"enable": enable,
		"admin": admin,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify_permissions(session: IRODSHTTPSession, lpath: str, operations: dict, admin: int = 0) -> dict:
	"""
	Modify permissions for multiple users or groups for a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to have its permissions modified.
	    operations: Dictionary containing the operations to carry out. Should contain names
	      and permissions for all operations.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(operations, list)
	common.validate_instance(operations[0], dict)
	common.validate_0_or_1(admin)

	data = {
		"op": "modify_permissions",
		"lpath": lpath,
		"operations": json.dumps(operations),
		"admin": admin,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify_metadata(session: IRODSHTTPSession, lpath: str, operations: dict, admin: int = 0) -> dict:
	"""
	Modify the metadata for a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection to have its metadata modified.
	    operations: Dictionary containing the operations to carry out. Should contain the
	      operation, attribute, value, and optionally units.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(operations, list)
	common.validate_instance(operations[0], dict)
	common.validate_0_or_1(admin)

	data = {
		"op": "modify_metadata",
		"lpath": lpath,
		"operations": json.dumps(operations),
		"admin": admin,
	}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def rename(session: IRODSHTTPSession, old_lpath: str, new_lpath: str) -> dict:
	"""
	Rename or move a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    old_lpath: The current absolute logical path of the collection.
	    new_lpath: The absolute logical path of the destination for the collection.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(old_lpath, str)
	common.validate_instance(new_lpath, str)

	data = {"op": "rename", "old-lpath": old_lpath, "new-lpath": new_lpath}

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def touch(session: IRODSHTTPSession, lpath: str, seconds_since_epoch: int = -1, reference: str = "") -> dict:
	"""
	Update mtime for a collection.

	Args:
	    session: IRODSHTTPSession object containing the base URL and authentication token.
	    lpath: The absolute logical path of the collection being touched.
	    seconds_since_epoch: The value to set mtime to, defaults to -1 as a flag.
	    reference: The absolute logical path of the collection to use as a reference for mtime.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_gte_minus1(seconds_since_epoch)
	common.validate_instance(reference, str)

	data = {"op": "touch", "lpath": lpath}

	if seconds_since_epoch != -1:
		data["seconds-since-epoch"] = seconds_since_epoch

	if reference != "":
		data["reference"] = reference

	r = requests.post(session.url_base + "/collections", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)
