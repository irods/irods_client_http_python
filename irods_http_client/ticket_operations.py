"""Ticket operations for iRODS HTTP API."""

import requests

from . import common


class Tickets:
	"""Perform ticket operations via iRODS HTTP API."""

	def __init__(self, url_base: str):
		"""
		Initialize Tickets with a base url.

		Token is set to None initially, and updated when setToken() is called in irodsClient.
		"""
		self.url_base = url_base
		self.token = None

	def create(
		self,
		lpath: str,
		type_: str = "read",
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
		    lpath: Absolute logical path to a data object or collection.
		    type_: Read or write. Defaults to read.
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
		    ValueError: If type_ is not 'read' or 'write'.
		"""
		common.check_token(self.token)
		common.validate_instance(lpath, str)
		common.validate_instance(type_, str)
		if type_ not in ["read", "write"]:
			raise ValueError("type must be either read or write")
		common.validate_gte_minus1(use_count)
		common.validate_gte_minus1(write_data_object_count)
		common.validate_gte_minus1(write_byte_count)
		common.validate_gte_minus1(seconds_until_expiration)
		common.validate_instance(users, str)
		common.validate_instance(groups, str)
		common.validate_instance(hosts, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "create", "lpath": lpath, "type": type_}

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

		r = requests.post(self.url_base + "/tickets", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove(self, name: str):
		"""
		Remove an existing ticket.

		Args:
		    name: The ticket to be removed.

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

		r = requests.post(self.url_base + "/tickets", headers=headers, data=data, timeout=30)
		return common.process_response(r)
