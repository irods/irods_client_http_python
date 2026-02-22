"""Rule operations for iRODS HTTP API."""

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def list_rule_engines(session: IRODSHTTPSession):
	"""
	List available rule engine plugin instances.

	Args:
	    session: An IRODSHTTPSession instance.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	headers = {
		"Authorization": "Bearer " + session.token,
	}

	params = {"op": "list_rule_engines"}

	r = requests.get(session.url_base + "/rules", params=params, headers=headers)  # noqa: S113
	return common.process_response(r)


def execute(session: IRODSHTTPSession, rule_text: str, rep_instance: str = ""):
	"""
	Execute rule code.

	Args:
	    session: An IRODSHTTPSession instance.
	    rule_text: The rule code to execute.
	    rep_instance: The rule engine plugin to run the rule-text against.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(rule_text, str)
	common.validate_instance(rep_instance, str)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "execute", "rule-text": rule_text}

	if rep_instance != "":
		data["rep-instance"] = rep_instance

	r = requests.post(session.url_base + "/rules", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove_delay_rule(session: IRODSHTTPSession, rule_id: int):
	"""
	Remove a delay rule from the catalog.

	Args:
	    session: An IRODSHTTPSession instance.
	    rule_id: The id of the delay rule to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_gte_zero(rule_id)

	headers = {
		"Authorization": "Bearer " + session.token,
		"Content-Type": "application/x-www-form-urlencoded",
	}

	data = {"op": "remove_delay_rule", "rule-id": rule_id}

	r = requests.post(session.url_base + "/rules", headers=headers, data=data)  # noqa: S113
	return common.process_response(r)
