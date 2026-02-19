"""Zone operations for iRODS HTTP API."""

import requests

from . import common


def add(session: common.HTTPSession, name: str, connection_info: str = "", comment: str = ""):
	"""
	Add a remote zone to the local zone. Requires rodsadmin privileges.

	Args:
	    session: An HTTPSession instance.
	    name: The name of the zone to be added.
	    connection_info: The host and port to connect to. If included, must be in the format <host>:<port>.
	    comment: The comment to attach to the zone.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(connection_info, str)
	common.validate_instance(comment, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "add", "name": name}

	if connection_info != "":
		data["connection-info"] = connection_info
	if comment != "":
		data["comment"] = comment

	r = requests.post(session.url_base + "/zones", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove(session: common.HTTPSession, name: str):
	"""
	Remove a remote zone from the local zone. Requires rodsadmin privileges.

	Args:
	    session: An HTTPSession instance.
	    name: The zone to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "remove", "name": name}

	r = requests.post(session.url_base + "/zones", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def modify(session: common.HTTPSession, name: str, property_: str, value: str):
	"""
	Modify properties of a remote zone. Requires rodsadmin privileges.

	Args:
	    session: An HTTPSession instance.
	    name: The name of the zone to be modified.
	    property_: The property to be modified. Can be set to 'name', 'connection_info', or 'comment'.
	              The value for 'connection_info' must be in the format <host>:<port>.
	    value: The new value to be set.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(property_, str)
	common.validate_instance(value, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "modify", "name": name, "property": property_, "value": value}

	r = requests.post(session.url_base + "/zones", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def report(session: common.HTTPSession):
	"""
	Return information about the iRODS zone. Requires rodsadmin privileges.

	Args:
	    session: An HTTPSession instance.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	params = {"op": "report"}

	r = requests.get(session.url_base + "/zones", headers=headers, params=params)  # noqa: S113
	return common.process_response(r)


def stat(session: common.HTTPSession, name: str):
	"""
	Return information about a named iRODS zone. Requires rodsadmin privileges.

	Args:
	    session: An HTTPSession instance.
	    name: The name of the zone.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	params = {"op": "stat", "name": name}

	r = requests.get(session.url_base + "/zones", headers=headers, params=params)  # noqa: S113
	return common.process_response(r)
