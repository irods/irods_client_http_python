"""User and group operations for iRODS HTTP API."""

import json

import requests

from . import common


class UsersGroups:
	"""Perform user and group operations via iRODS HTTP API."""

	def __init__(self, url_base: str):
		"""
		Initialize UsersGroups with a base url.

		Token is set to None initially, and updated when setToken() is called in irodsClient.
		"""
		self.url_base = url_base
		self.token = None

	def create_user(self, name: str, zone: str, user_type: str = "rodsuser"):
		"""
		Create a new user. Requires rodsadmin or groupadmin privileges.

		Args:
		    name: The name of the user to be created.
		    zone: The zone for the user to be created.
		    user_type: Can be rodsuser, groupadmin, or rodsadmin. Defaults to rodsuser.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.

		Raises:
		    ValueError: If user_type is not 'rodsuser', 'groupadmin', or 'rodsadmin'.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(zone, str)
		common.validate_instance(user_type, str)
		if user_type not in ["rodsuser", "groupadmin", "rodsadmin"]:
			raise ValueError("user_type must be set to rodsuser, groupadmin, or rodsadmin.")

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "create_user", "name": name, "zone": zone, "user-type": user_type}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove_user(self, name: str, zone: str):
		"""
		Remove a user. Requires rodsadmin privileges.

		Args:
		    name: The name of the user to be removed.
		    zone: The zone for the user to be removed.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(zone, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "remove_user", "name": name, "zone": zone}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def set_password(self, name: str, zone: str, new_password: str = ""):
		"""
		Change a users password. Requires rodsadmin privileges.

		Args:
		    name: The name of the user to have their password changed.
		    zone: The zone for the user to have their password changed.
		    new_password: The new password to set for the user.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(zone, str)
		common.validate_instance(new_password, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {
			"op": "set_password",
			"name": name,
			"zone": zone,
			"new-password": new_password,
		}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def set_user_type(self, name: str, zone: str, user_type: str):
		"""
		Change a users type. Requires rodsadmin privileges.

		Args:
		    name: The name of the user to have their type updated.
		    zone: The zone for the user to have their type updated.
		    user_type: Can be rodsuser, groupadmin, or rodsadmin.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.

		Raises:
		    ValueError: If user_type is not 'rodsuser', 'groupadmin', or 'rodsadmin'.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(zone, str)
		common.validate_instance(user_type, str)
		if user_type not in ["rodsuser", "groupadmin", "rodsadmin"]:
			raise ValueError("user_type must be set to rodsuser, groupadmin, or rodsadmin.")

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {
			"op": "set_user_type",
			"name": name,
			"zone": zone,
			"new-user-type": user_type,
		}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def create_group(self, name: str):
		"""
		Create a new group. Requires rodsadmin or groupadmin privileges.

		Args:
		    name: The name of the group to be created.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "create_group", "name": name}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove_group(self, name: str):
		"""
		Remove a group. Requires rodsadmin privileges.

		Args:
		    name: The name of the group to be removed.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "remove_group", "name": name}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def add_to_group(self, user: str, zone: str, group: str = ""):
		"""
		Add a user to a group. Requires rodsadmin or groupadmin privileges.

		Args:
		    user: The user to be added to the group.
		    zone: The zone for the user to be added to the group.
		    group: The group for the user to be added to.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(user, str)
		common.validate_instance(zone, str)
		common.validate_instance(group, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "add_to_group", "user": user, "zone": zone, "group": group}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove_from_group(self, user: str, zone: str, group: str):
		"""
		Remove a user from a group. Requires rodsadmin or groupadmin privileges.

		Args:
		    user: The user to be removed from the group.
		    zone: The zone for the user to be removed from the group.
		    group: The group for the user to be removed from.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(user, str)
		common.validate_instance(zone, str)
		common.validate_instance(group, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "remove_from_group", "user": user, "zone": zone, "group": group}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def users(self):
		"""
		List all users in the zone. Requires rodsadmin privileges.

		Returns
		- A dict containing the HTTP status code and iRODS response.
		- The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)

		headers = {"Authorization": "Bearer " + self.token}

		params = {"op": "users"}

		r = requests.get(self.url_base + "/users-groups", headers=headers, params=params, timeout=30)
		return common.process_response(r)

	def groups(self):
		"""
		List all groups in the zone. Requires rodsadmin privileges.

		Returns
		- A dict containing the HTTP status code and iRODS response.
		- The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)

		headers = {
			"Authorization": "Bearer " + self.token,
		}

		params = {"op": "groups"}

		r = requests.get(self.url_base + "/users-groups", headers=headers, params=params, timeout=30)
		return common.process_response(r)

	def is_member_of_group(self, group: str, user: str, zone: str):
		"""
		Return whether a user is a member of a group or not.

		Args:
		    group: The group being checked.
		    user: The user being checked.
		    zone: The zone for the user being checked.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(group, str)
		common.validate_instance(user, str)
		common.validate_instance(zone, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		params = {
			"op": "is_member_of_group",
			"group": group,
			"user": user,
			"zone": zone,
		}

		r = requests.get(self.url_base + "/users-groups", headers=headers, params=params, timeout=30)
		return common.process_response(r)

	def stat(self, name: str, zone: str = ""):
		"""
		Return information about a user or group.

		Args:
		    name: The name of the user or group to be accessed.
		    zone: The zone of the user to be accessed. Not required for groups.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(zone, str)

		headers = {"Authorization": "Bearer " + self.token}

		params = {"op": "stat", "name": name}

		if zone != "":
			params["zone"] = zone

		r = requests.get(self.url_base + "/users-groups", headers=headers, params=params, timeout=30)
		return common.process_response(r)

	def modify_metadata(self, name: str, operations: list):
		"""
		Modify the metadata for a user or group. Requires rodsadmin privileges.

		Args:
		    name: The user or group to be modified.
		    operations: The operations to be carried out.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(operations, list)
		common.validate_instance(operations[0], dict)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {
			"op": "modify_metadata",
			"name": name,
			"operations": json.dumps(operations),
		}

		r = requests.post(self.url_base + "/users-groups", headers=headers, data=data, timeout=30)
		return common.process_response(r)
