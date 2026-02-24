"""User and group operations for iRODS HTTP API."""

import json

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def create_user(session: IRODSHTTPSession, name: str, zone: str, type: str = "rodsuser"):  # noqa: A002
	"""
	Create a new user. Requires rodsadmin or groupadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the user to be created.
	    zone: The zone for the user to be created.
	    type: Can be rodsuser, groupadmin, or rodsadmin. Defaults to rodsuser.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If type is not 'rodsuser', 'groupadmin', or 'rodsadmin'.
	"""
	common.validate_instance(name, str)
	common.validate_instance(zone, str)
	common.validate_instance(type, str)
	if type not in ["rodsuser", "groupadmin", "rodsadmin"]:
		raise ValueError("type must be set to rodsuser, groupadmin, or rodsadmin.")

	data = {"op": "create_user", "name": name, "zone": zone, "user-type": type}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove_user(session: IRODSHTTPSession, name: str, zone: str):
	"""
	Remove a user. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the user to be removed.
	    zone: The zone for the user to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(zone, str)

	data = {"op": "remove_user", "name": name, "zone": zone}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def set_password(session: IRODSHTTPSession, name: str, zone: str, new_password: str = ""):
	"""
	Change a users password. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the user to have their password changed.
	    zone: The zone for the user to have their password changed.
	    new_password: The new password to set for the user.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(zone, str)
	common.validate_instance(new_password, str)

	data = {
		"op": "set_password",
		"name": name,
		"zone": zone,
		"new-password": new_password,
	}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def set_user_type(session: IRODSHTTPSession, name: str, zone: str, type: str):  # noqa: A002
	"""
	Change a users type. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the user to have their type updated.
	    zone: The zone for the user to have their type updated.
	    type: Can be rodsuser, groupadmin, or rodsadmin.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If user_type is not 'rodsuser', 'groupadmin', or 'rodsadmin'.
	"""
	common.validate_instance(name, str)
	common.validate_instance(zone, str)
	common.validate_instance(type, str)
	if type not in ["rodsuser", "groupadmin", "rodsadmin"]:
		raise ValueError("type must be set to rodsuser, groupadmin, or rodsadmin.")

	data = {
		"op": "set_user_type",
		"name": name,
		"zone": zone,
		"new-user-type": type,
	}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def create_group(session: IRODSHTTPSession, name: str):
	"""
	Create a new group. Requires rodsadmin or groupadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the group to be created.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	data = {"op": "create_group", "name": name}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove_group(session: IRODSHTTPSession, name: str):
	"""
	Remove a group. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the group to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	data = {"op": "remove_group", "name": name}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def add_to_group(session: IRODSHTTPSession, user: str, zone: str, group: str = ""):
	"""
	Add a user to a group. Requires rodsadmin or groupadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    user: The user to be added to the group.
	    zone: The zone for the user to be added to the group.
	    group: The group for the user to be added to.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(user, str)
	common.validate_instance(zone, str)
	common.validate_instance(group, str)

	data = {"op": "add_to_group", "user": user, "zone": zone, "group": group}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove_from_group(session: IRODSHTTPSession, user: str, zone: str, group: str):
	"""
	Remove a user from a group. Requires rodsadmin or groupadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    user: The user to be removed from the group.
	    zone: The zone for the user to be removed from the group.
	    group: The group for the user to be removed from.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(user, str)
	common.validate_instance(zone, str)
	common.validate_instance(group, str)

	data = {"op": "remove_from_group", "user": user, "zone": zone, "group": group}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)


def users(session: IRODSHTTPSession):
	"""
	List all users in the zone. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	params = {"op": "users"}

	r = requests.get(session.url_base + "/users-groups", headers=session.get_headers, params=params)  # noqa: S113
	return common.process_response(r)


def groups(session: IRODSHTTPSession):
	"""
	List all groups in the zone. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	params = {"op": "groups"}

	r = requests.get(session.url_base + "/users-groups", headers=session.get_headers, params=params)  # noqa: S113
	return common.process_response(r)


def is_member_of_group(session: IRODSHTTPSession, group: str, user: str, zone: str):
	"""
	Return whether a user is a member of a group or not.

	Args:
	    session: An IRODSHTTPSession instance.
	    group: The group being checked.
	    user: The user being checked.
	    zone: The zone for the user being checked.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(group, str)
	common.validate_instance(user, str)
	common.validate_instance(zone, str)

	params = {
		"op": "is_member_of_group",
		"group": group,
		"user": user,
		"zone": zone,
	}

	r = requests.get(session.url_base + "/users-groups", headers=session.post_headers, params=params)  # noqa: S113
	return common.process_response(r)


def stat(session: IRODSHTTPSession, name: str, zone: str = ""):
	"""
	Return information about a user or group.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the user or group to be accessed.
	    zone: The zone of the user to be accessed. Not required for groups.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(zone, str)

	params = {"op": "stat", "name": name}

	if zone != "":
		params["zone"] = zone

	r = requests.get(session.url_base + "/users-groups", headers=session.get_headers, params=params)  # noqa: S113
	return common.process_response(r)


def modify_metadata(session: IRODSHTTPSession, name: str, operations: list):
	"""
	Modify the metadata for a user or group. Requires rodsadmin privileges.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The user or group to be modified.
	    operations: The operations to be carried out.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(operations, list)
	common.validate_instance(operations[0], dict)

	data = {
		"op": "modify_metadata",
		"name": name,
		"operations": json.dumps(operations),
	}

	r = requests.post(session.url_base + "/users-groups", headers=session.post_headers, data=data)  # noqa: S113
	return common.process_response(r)
