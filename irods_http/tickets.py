"""Ticket operations for iRODS HTTP API."""

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def create(
	session: IRODSHTTPSession,
	lpath: str,
	type: str = "read",  # noqa: A002
	use_count: int = -1,
	write_data_object_count: int = -1,
	write_byte_count: int = -1,
	seconds_until_expiration: int = -1,
	users: str = "",
	groups: str = "",
	hosts: str = "",
):
	"""
	Create a new ticket for a collection or data object.

	Args:
	    session: An IRODSHTTPSession instance.
	    lpath: Absolute logical path to a data object or collection.
	    type: Read or write. Defaults to read.
	    use_count: Number of times the ticket can be used.
	    write_data_object_count: Max number of writes that can be performed.
	    write_byte_count: Max number of bytes that can be written.
	    seconds_until_expiration: Number of seconds before the ticket expires.
	    users: Comma-delimited list of users allowed to use the ticket.
	    groups: Comma-delimited list of groups allowed to use the ticket.
	    hosts: Comma-delimited list of hosts allowed to use the ticket.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If type is not 'read' or 'write'.
	"""
	common.validate_instance(lpath, str)
	common.validate_instance(type, str)
	if type not in ["read", "write"]:
		raise ValueError("type must be either read or write")
	common.validate_gte_minus1(use_count)
	common.validate_gte_minus1(write_data_object_count)
	common.validate_gte_minus1(write_byte_count)
	common.validate_gte_minus1(seconds_until_expiration)
	common.validate_instance(users, str)
	common.validate_instance(groups, str)
	common.validate_instance(hosts, str)

	data = {"op": "create", "lpath": lpath, "type": type}

	if use_count != -1:
		data["use-count"] = use_count
	if write_data_object_count != -1:
		data["write-data-object-count"] = write_data_object_count
	if write_byte_count != -1:
		data["write-byte-count"] = write_byte_count
	if seconds_until_expiration != -1:
		data["seconds-until-expiration"] = seconds_until_expiration
	if users != "":
		data["users"] = users
	if groups != "":
		data["groups"] = groups
	if hosts != "":
		data["hosts"] = hosts

	r = requests.post(session.url_base + "/tickets", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove(session: IRODSHTTPSession, name: str):
	"""
	Remove an existing ticket.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The ticket to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	data = {"op": "remove", "name": name}

	r = requests.post(session.url_base + "/tickets", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)
