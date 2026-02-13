"""Zone operations for iRODS HTTP API."""

import requests

from . import common


class Zones:
	"""Perform zone operations via iRODS HTTP API."""

	def __init__(self, url_base: str):
		"""
		Initialize Zones with a base url.

		Token is set to None initially, and updated when setToken() is called in irodsClient.
		"""
		self.url_base = url_base
		self.token = None

	def add(self, name: str, connection_info: str = "", comment: str = ""):
		"""
		Add a remote zone to the local zone. Requires rodsadmin privileges.

		Args:
		    name: The name of the zone to be added.
		    connection_info: The host and port to connect to. If included, must be in the format <host>:<port>.
		    comment: The comment to attach to the zone.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(connection_info, str)
		common.validate_instance(comment, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "add", "name": name}

		if connection_info != "":
			data["connection-info"] = connection_info
		if comment != "":
			data["comment"] = comment

		r = requests.post(self.url_base + "/zones", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove(self, name: str):
		"""
		Remove a remote zone from the local zone. Requires rodsadmin privileges.

		Args:
		    name: The zone to be removed.

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

		data = {"op": "remove", "name": name}

		r = requests.post(self.url_base + "/zones", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def modify(self, name: str, property_: str, value: str):
		"""
		Modify properties of a remote zone. Requires rodsadmin privileges.

		Args:
		    name: The name of the zone to be modified.
		    property_: The property to be modified. Can be set to 'name', 'connection_info', or 'comment'.
		              The value for 'connection_info' must be in the format <host>:<port>.
		    value: The new value to be set.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)
		common.validate_instance(property_, str)
		common.validate_instance(value, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "modify", "name": name, "property": property_, "value": value}

		r = requests.post(self.url_base + "/zones", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def report(self):
		"""
		Return information about the iRODS zone. Requires rodsadmin privileges.

		Returns
		- A dict containing the HTTP status code and iRODS response.
		- The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		params = {"op": "report"}

		r = requests.get(self.url_base + "/zones", headers=headers, params=params, timeout=30)
		return common.process_response(r)

	def stat(self, name: str):
		"""
		Return information about a named iRODS zone. Requires rodsadmin privileges.

		Returns
		- A dict containing the HTTP status code and iRODS response.
		- The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(name, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		params = {"op": "stat", "name": name}

		r = requests.get(self.url_base + "/zones", headers=headers, params=params, timeout=30)
		return common.process_response(r)
