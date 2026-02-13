"""Rule operations for iRODS HTTP API."""

import requests

from . import common


class Rules:
	"""Perform rule operations via iRODS HTTP API."""

	def __init__(self, url_base: str):
		"""
		Initialize Rules with a base url.

		Token is set to None initially, and updated when setToken() is called in irodsClient.
		"""
		self.url_base = url_base
		self.token = None

	def list_rule_engines(self):
		"""
		List available rule engine plugin instances.

		Returns
		- A dict containing the HTTP status code and iRODS response.
		- The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)

		headers = {
			"Authorization": "Bearer " + self.token,
		}

		params = {"op": "list_rule_engines"}

		r = requests.get(self.url_base + "/rules", params=params, headers=headers, timeout=30)
		return common.process_response(r)

	def execute(self, rule_text: str, rep_instance: str = ""):
		"""
		Execute rule code.

		Args:
		    rule_text: The rule code to execute.
		    rep_instance: The rule engine plugin to run the rule-text against.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_instance(rule_text, str)
		common.validate_instance(rep_instance, str)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "execute", "rule-text": rule_text}

		if rep_instance != "":
			data["rep-instance"] = rep_instance

		r = requests.post(self.url_base + "/rules", headers=headers, data=data, timeout=30)
		return common.process_response(r)

	def remove_delay_rule(self, rule_id: int):
		"""
		Remove a delay rule from the catalog.

		Args:
		    rule_id: The id of the delay rule to be removed.

		Returns:
		    A dict containing the HTTP status code and iRODS response.
		    The iRODS response is only valid if no error occurred during HTTP communication.
		"""
		common.check_token(self.token)
		common.validate_gte_zero(rule_id)

		headers = {
			"Authorization": "Bearer " + self.token,
			"Content-Type": "application/x-www-form-urlencoded",
		}

		data = {"op": "remove_delay_rule", "rule-id": rule_id}

		r = requests.post(self.url_base + "/rules", headers=headers, data=data, timeout=30)
		return common.process_response(r)
