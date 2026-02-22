"""Data object operations for iRODS HTTP API."""

import json

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def touch(
	session: IRODSHTTPSession,
	lpath: str,
	no_create: int = 0,
	replica_number: int = -1,
	leaf_resources: str = "",
	seconds_since_epoch: int = -1,
	reference: str = "",
) -> dict:
	"""
	Update mtime for an existing data object or create a new one.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object being touched.
	    no_create: Set to 1 to prevent creating a new object, otherwise set to 0. Defaults to 0.
	    replica_number: The replica number of the target replica. Defaults to -1.
	    leaf_resources: The resource holding an existing replica. If one does not exist, creates one.
	      Defaults to "".
	    seconds_since_epoch: The value to set mtime to, defaults to -1 as a flag. Defaults to -1.
	    reference: The absolute logical path of the data object to use as a reference for mtime.
	      Defaults to "".

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(no_create)
	common.validate_gte_minus1(replica_number)
	common.validate_instance(leaf_resources, str)
	common.validate_gte_minus1(seconds_since_epoch)
	common.validate_instance(reference, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "touch", "lpath": lpath, "no-create": no_create}

	if seconds_since_epoch != -1:
		data["seconds-since-epoch"] = seconds_since_epoch

	if replica_number != -1:
		data["replica-number"] = replica_number

	if leaf_resources != "":
		data["leaf-resources"] = leaf_resources

	if reference != "":
		data["reference"] = reference

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove(session: IRODSHTTPSession, lpath: str, catalog_only: int = 0, no_trash: int = 0, admin: int = 0) -> dict:
	"""
	Remove an existing data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to be removed.
	    catalog_only: Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
	    no_trash: Set to 1 to move the data object to trash, 0 to permanently remove. Defaults to 0.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_0_or_1(catalog_only)
	common.validate_0_or_1(no_trash)
	common.validate_0_or_1(admin)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "remove",
		"lpath": lpath,
		"catalog-only": catalog_only,
		"no-trash": no_trash,
		"admin": admin,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def calculate_checksum(
	session: IRODSHTTPSession,
	lpath: str,
	resource: str = "",
	replica_number: int = -1,
	force: int = 0,
	all_: int = 0,
	admin: int = 0,
) -> dict:
	"""
	Calculate the checksum for a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have its checksum calculated.
	    resource: The resource holding the existing replica. Defaults to "".
	    replica_number: The replica number of the target replica. Defaults to -1.
	    force: Set to 1 to replace the existing checksum, otherwise set to 0. Defaults to 0.
	    all_: Set to 1 to calculate the checksum for all replicas, otherwise set to 0. Defaults to 0.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(resource, str)
	common.validate_gte_minus1(replica_number)
	common.validate_0_or_1(force)
	common.validate_0_or_1(all_)
	common.validate_0_or_1(admin)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "calculate_checksum",
		"lpath": lpath,
		"force": force,
		"all": all_,
		"admin": admin,
	}

	if resource != "":
		data["resource"] = resource

	if replica_number != -1:
		data["replica-number"] = replica_number

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def verify_checksum(
	session: IRODSHTTPSession,
	lpath: str,
	resource: str = "",
	replica_number: int = -1,
	compute_checksums: int = 0,
	admin: int = 0,
) -> dict:
	"""
	Verify the checksum for a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have its checksum verified.
	    resource: The resource holding the existing replica. Defaults to "".
	    replica_number: The replica number of the target replica. Defaults to -1.
	    compute_checksums: Set to 1 to skip checksum calculation, otherwise set to 0. Defaults to 0.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(resource, str)
	common.validate_gte_minus1(replica_number)
	common.validate_0_or_1(compute_checksums)
	common.validate_0_or_1(admin)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "calculate_checksum",
		"lpath": lpath,
		"compute-checksums": compute_checksums,
		"admin": admin,
	}

	if resource != "":
		data["resource"] = resource

	if replica_number != -1:
		data["replica-number"] = replica_number

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def stat(session: IRODSHTTPSession, lpath: str, ticket: str = "") -> dict:
	"""
	Give information about a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object being accessed.
	    ticket: Ticket to be enabled before the operation. Defaults to an empty string.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(ticket, str)

	headers = {
		"Authorization": "Bearer " + session.token,
	}

	params = {"op": "stat", "lpath": lpath, "ticket": ticket}

	r = requests.get(session.url_base + "/data-objects", params=params, headers=headers)  # noqa: S113
	return common.process_response(r)


def rename(session: IRODSHTTPSession, old_lpath: str, new_lpath: str) -> dict:
	"""
	Rename or move a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    old_lpath: The current absolute logical path of the data object.
	    new_lpath: The absolute logical path of the destination for the data object.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(old_lpath, str)
	common.validate_instance(new_lpath, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "rename", "old-lpath": old_lpath, "new-lpath": new_lpath}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def copy(
	session: IRODSHTTPSession,
	src_lpath: str,
	dst_lpath: str,
	src_resource: str = "",
	dst_resource: str = "",
	overwrite: int = 0,
) -> dict:
	"""
	Copy a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    src_lpath: The absolute logical path of the source data object.
	    dst_lpath: The absolute logical path of the destination.
	    src_resource: The name of the source resource. Defaults to "".
	    dst_resource: The name of the destination resource. Defaults to "".
	    overwrite: Set to 1 to overwrite an existing objject, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(src_lpath, str)
	common.validate_instance(dst_lpath, str)
	common.validate_instance(src_resource, str)
	common.validate_instance(dst_resource, str)
	common.validate_0_or_1(overwrite)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "copy",
		"src-lpath": src_lpath,
		"dst-lpath": dst_lpath,
		"overwrite": overwrite,
	}

	if src_resource != "":
		data["src-resource"] = src_resource

	if dst_resource != "":
		data["dst-resource"] = dst_resource

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def replicate(
	session: IRODSHTTPSession,
	lpath: str,
	src_resource: str = "",
	dst_resource: str = "",
	admin: int = 0,
) -> dict:
	"""
	Replicates a data object from one resource to another.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to be replicated.
	    src_resource: The name of the source resource. Defaults to "".
	    dst_resource: The name of the destination resource. Defaults to "".
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(src_resource, str)
	common.validate_instance(dst_resource, str)
	common.validate_0_or_1(admin)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "replicate", "lpath": lpath, "admin": admin}

	if src_resource != "":
		data["src-resource"] = src_resource

	if dst_resource != "":
		data["dst-resource"] = dst_resource

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def trim(session: IRODSHTTPSession, lpath: str, replica_number: int, catalog_only: int = 0, admin: int = 0) -> dict:
	"""
	Trims an existing replica or removes its catalog entry.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The  absolute logical path of the data object to be trimmed.
	    replica_number: The replica number of the target replica.
	    catalog_only: Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
	    admin: Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(replica_number, int)
	common.validate_0_or_1(catalog_only)
	common.validate_0_or_1(admin)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "trim",
		"lpath": lpath,
		"replica-number": replica_number,
		"catalog-only": catalog_only,
		"admin": admin,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def register(
	session: IRODSHTTPSession,
	lpath: str,
	ppath: str,
	resource: str,
	as_additional_replica: int = 0,
	data_size: int = -1,
	checksum: int = 0,
) -> dict:
	"""
	Register a data object/replica into the catalog.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to be registered.
	    ppath: The absolute physical path of the data object to be registered.
	    resource: The resource that will own the replica.
	    as_additional_replica: Set to 1 to register as a replica of an existing
	      object, otherwise set to 0. Defaults to 0.
	    data_size: The size of the replica in bytes. Defaults to -1.
	    checksum: Set to 1 to register with a checksum. Defaults to 0.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(ppath, str)
	common.validate_instance(resource, str)
	common.validate_0_or_1(as_additional_replica)
	common.validate_gte_minus1(data_size)
	common.validate_0_or_1(checksum)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "register",
		"lpath": lpath,
		"ppath": ppath,
		"resource": resource,
		"as_additional_replica": as_additional_replica,
		"checksum": checksum,
	}

	if data_size != -1:
		data["data-size"] = data_size

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def read(session: IRODSHTTPSession, lpath: str, offset: int = 0, count: int = -1, ticket: str = "") -> dict:
	"""
	Read bytes from a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to be read from.
	    offset: The number of bytes to skip. Defaults to 0.
	    count: The number of bytes to read. Defaults to -1.
	    ticket: Ticket to be enabled before the operation. Defaults to an empty string.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(offset, int)
	common.validate_gte_minus1(count)
	common.validate_instance(ticket, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	params = {"op": "read", "lpath": lpath, "offset": offset}

	if count != -1:
		params["count"] = count

	if ticket != "":
		params["ticket"] = ticket

	r = requests.get(session.url_base + "/data-objects", params=params, headers=headers)  # noqa: S113
	# this is the only payload that is different from common.process_response()
	return {'status_code': r.status_code, 'data': r.content}


def write(
	session: IRODSHTTPSession,
	bytes_,
	lpath: str = "",
	resource: str = "",
	offset: int = 0,
	truncate: int = 1,
	append: int = 0,
	parallel_write_handle: str = "",
	stream_index: int = -1,
) -> dict:
	"""
	Write bytes to a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    bytes_: The bytes to be written.
	    lpath: The absolute logical path of the data object to be written to. Defaults to "".
	    resource: The root resource to write to. Defaults to "".
	    offset: The number of bytes to skip. Defaults to 0.
	    truncate: Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
	    append: Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
	    parallel_write_handle: The handle to be used when writing in parallel. Defaults to "".
	    stream_index: The stream to use when writing in parallel. Defaults to -1.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    TypeError: If bytes is not bytes or str.
	    ValueError: If bytes length is less than 0.
	"""
	common.validate_not_none(session.token)
	if type(bytes_) not in [bytes, str]:
		raise TypeError("type(bytes_) must be 'bytes' or 'str'")
	common.validate_instance(lpath, str)
	common.validate_instance(resource, str)
	common.validate_gte_zero(offset)
	common.validate_0_or_1(truncate)
	common.validate_0_or_1(append)
	common.validate_instance(parallel_write_handle, str)
	common.validate_gte_minus1(stream_index)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "write",
		"offset": offset,
		"truncate": truncate,
		"append": append,
		"bytes": bytes_,
	}

	if parallel_write_handle != "":
		data["parallel-write-handle"] = parallel_write_handle
	else:
		data["lpath"] = lpath

	if resource != "":
		data["resource"] = resource

	if stream_index != -1:
		data["stream-index"] = stream_index

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def parallel_write_init(
	session: IRODSHTTPSession,
	lpath: str,
	stream_count: int,
	truncate: int = 1,
	append: int = 0,
	ticket: str = "",
) -> dict:
	"""
	Initialize server-side state for parallel writing.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to be initialized for parallel write.
	    stream_count: The number of streams to open.
	    truncate: Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
	    append: Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
	    ticket: Ticket to be enabled before the operation. Defaults to an empty string.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_gte_zero(stream_count)
	common.validate_0_or_1(truncate)
	common.validate_0_or_1(append)
	common.validate_instance(ticket, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "parallel_write_init",
		"lpath": lpath,
		"stream-count": stream_count,
		"truncate": truncate,
		"append": append,
	}

	if ticket != "":
		data["ticket"] = ticket

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def parallel_write_shutdown(session: IRODSHTTPSession, parallel_write_handle: str) -> dict:
	"""
	Shuts down the parallel write state in the server.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    parallel_write_handle: Handle obtained from parallel_write_init.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(parallel_write_handle, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "parallel_write_shutdown",
		"parallel-write-handle": parallel_write_handle,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify_metadata(session: IRODSHTTPSession, lpath: str, operations: list, admin: int = 0) -> dict:
	"""
	Modify the metadata for a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have its inheritance set.
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

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "modify_metadata",
		"lpath": lpath,
		"operations": json.dumps(operations),
		"admin": admin,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def set_permission(session: IRODSHTTPSession, lpath: str, entity_name: str, permission: str, admin: int = 0) -> dict:
	"""
	Set the permission of a user for a given data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have a permission set.
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

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "set_permission",
		"lpath": lpath,
		"entity-name": entity_name,
		"permission": permission,
		"admin": admin,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify_permissions(session: IRODSHTTPSession, lpath: str, operations: list, admin: int = 0) -> dict:
	"""
	Modify permissions for multiple users or groups for a data object.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have its permissions modified.
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

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {
		"op": "modify_permissions",
		"lpath": lpath,
		"operations": json.dumps(operations),
		"admin": admin,
	}

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify_replica(
	session: IRODSHTTPSession,
	lpath: str,
	resource_hierarchy: str = "",
	replica_number: int = -1,
	new_data_checksum: str = "",
	new_data_comments: str = "",
	new_data_create_time: str = "",
	new_data_expiry: str = "",
	new_data_mode: str = "",
	new_data_modify_time: str = "",
	new_data_path: str = "",
	new_data_replica_number: int = -1,
	new_data_replica_status: int = -1,
	new_data_resource_id: int = -1,
	new_data_size: int = -1,
	new_data_status: str = "",
	new_data_type_name: str = "",
	new_data_version: int = -1,
) -> dict:
	"""
	Modify properties of a single replica.

	Warning:
	This operation requires rodsadmin level privileges and should only be used when there isn't a safer option.
	Misuse can lead to catalog inconsistencies and unexpected behavior.

	Args:
	    session: IRODSHTTPSession object containing base URL and authentication token.
	    lpath: The absolute logical path of the data object to have a replica modified.
	    resource_hierarchy: The hierarchy containing the resource to be modified. Defaults to "".
	      Mutually exclusive with replica_number.
	    replica_number: The number of the replica to be modified. Defaults to -1. Mutually exclusive with
	      resource_hierarchy.
	    new_data_checksum: The new checksum to be set. Defaults to "".
	    new_data_comments: The new comments to be set. Defaults to "".
	    new_data_create_time: The new create time to be set. Defaults to "".
	    new_data_expiry: The new expiry to be set. Defaults to "".
	    new_data_mode: The new mode to be set. Defaults to "".
	    new_data_modify_time: The new modify time to be set. Defaults to "".
	    new_data_path: The new path to be set. Defaults to "".
	    new_data_replica_number: The new replica number to be set. Defaults to -1.
	    new_data_replica_status: The new replica status to be set. Defaults to -1.
	    new_data_resource_id: The new resource id to be set. Defaults to -1.
	    new_data_size: The new size to be set. Defaults to -1.
	    new_data_status: The new data status to be set. Defaults to "".
	    new_data_type_name: The new type name to be set. Defaults to "".
	    new_data_version: The new version to be set. Defaults to -1.

	Note:
	    At least one of the new_data parameters must be passed in.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If both resource_hierarchy and replica_number are provided.
	    RuntimeError: If no new_data parameters are provided.
	"""
	common.validate_not_none(session.token)
	common.validate_instance(lpath, str)
	common.validate_instance(resource_hierarchy, str)
	common.validate_instance(replica_number, int)
	if (resource_hierarchy != "") and (replica_number != -1):
		raise ValueError("replica_hierarchy and replica_number are mutually exclusive")
	common.validate_instance(new_data_checksum, str)
	common.validate_instance(new_data_comments, str)
	common.validate_instance(new_data_create_time, str)
	common.validate_instance(new_data_expiry, str)
	common.validate_instance(new_data_mode, str)
	common.validate_instance(new_data_modify_time, str)
	common.validate_instance(new_data_path, str)
	common.validate_gte_minus1(new_data_replica_number)
	common.validate_gte_minus1(new_data_replica_status)
	common.validate_gte_minus1(new_data_resource_id)
	common.validate_gte_minus1(new_data_size)
	common.validate_instance(new_data_status, str)
	common.validate_instance(new_data_type_name, str)
	common.validate_gte_minus1(new_data_version)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "modify_replica", "lpath": lpath}

	if resource_hierarchy != "":
		data["resource-hierarchy"] = resource_hierarchy

	if replica_number != -1:
		data["replica-number"] = replica_number

	# Boolean for checking if the user passed in any new_data parameters
	no_params = True

	if new_data_checksum != "":
		data["new-data-checksum"] = new_data_checksum
		no_params = False

	if new_data_comments != "":
		data["new-data-comments"] = new_data_comments
		no_params = False

	if new_data_create_time != "":
		data["new-data-create-time"] = new_data_create_time
		no_params = False

	if new_data_expiry != "":
		data["new-data-expiry"] = new_data_expiry
		no_params = False

	if new_data_mode != "":
		data["new-data-mode"] = new_data_mode
		no_params = False

	if new_data_modify_time != "":
		data["new-data-modify-time"] = new_data_modify_time
		no_params = False

	if new_data_path != "":
		data["new-data-path"] = new_data_path
		no_params = False

	if new_data_replica_number != -1:
		data["new-data-replica-number"] = new_data_replica_number
		no_params = False

	if new_data_replica_status != -1:
		data["new-data-replica-status"] = new_data_replica_status
		no_params = False

	if new_data_resource_id != -1:
		data["new-data-resource-id"] = new_data_resource_id
		no_params = False

	if new_data_size != -1:
		data["new-data-size"] = new_data_size
		no_params = False

	if new_data_status != "":
		data["new-data-status"] = new_data_status
		no_params = False

	if new_data_type_name != "":
		data["new-data-type-name"] = new_data_type_name
		no_params = False

	if new_data_version != -1:
		data["new-data-version"] = new_data_version
		no_params = False

	if no_params:
		raise RuntimeError("At least one new data parameter must be given.")

	r = requests.post(session.url_base + "/data-objects", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)
