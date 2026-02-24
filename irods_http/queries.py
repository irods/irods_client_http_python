"""Query operations for iRODS HTTP API."""

import requests

from . import common
from .irods_http import IRODSHTTPSession  # noqa: TC001


def execute_genquery(
	session: IRODSHTTPSession,
	query: str,
	offset: int = 0,
	count: int = -1,
	case_sensitive: int = 1,
	distinct: int = 1,
	parser: str = "genquery1",
	sql_only: int = 0,
	zone: str = "",
):
	"""
	Execute a GenQuery string and returns the results.

	Args:
	    session: An IRODSHTTPSession instance.
	    query: The query being executed.
	    offset: Number of rows to skip. Defaults to 0.
	    count: Number of rows to return. Default set by administrator.
	    case_sensitive: Set to 1 to execute a case sensitive query, otherwise
	      set to 0. Defaults to 1. Only supported by GenQuery1.
	    distinct: Set to 1 to collapse duplicate rows, otherwise set to 0.
	      Defaults to 1. Only supported by GenQuery 1.
	    parser: User either genquery1 or genquery2. Defaults to genquery1.
	    sql_only: Set to 1 to execute an SQL only query, otherwise set to 0.
	      Defaults to 0. Only supported by GenQuery2.
	    zone: The zone name. Defaults to the local zone.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.

	Raises:
	    ValueError: If parser is not 'genquery1' or 'genquery2'.
	"""
	common.validate_instance(query, str)
	common.validate_gte_zero(offset)
	common.validate_gte_minus1(count)
	common.validate_0_or_1(case_sensitive)
	common.validate_0_or_1(distinct)
	common.validate_instance(parser, str)
	if parser not in ["genquery1", "genquery2"]:
		raise ValueError("parser must be either 'genquery1' or 'genquery2'")
	common.validate_0_or_1(sql_only)
	common.validate_instance(zone, str)

	params = {
		"op": "execute_genquery",
		"query": query,
		"offset": offset,
		"parser": parser,
	}

	if count != -1:
		params["count"] = count

	if zone != "":
		params["zone"] = zone

	if parser == "genquery1":
		params["case-sensitive"] = case_sensitive
		params["distinct"] = distinct
	else:
		params["sql-only"] = sql_only

	r = requests.get(session.url_base + "/query", headers=session.get_headers, params=params)  # noqa: S113
	return common.process_response(r)


def execute_specific_query(
	session: IRODSHTTPSession,
	name: str,
	args: str = "",
	args_delimiter: str = ",",
	offset: int = 0,
	count: int = -1,
):
	"""
	Execute a specific query and returns the results.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the query to be executed.
	    args: The arguments to be passed into the query.
	    args_delimiter: The delimiter to be used to parse the args. Defaults to ','.
	    offset: Number of rows to skip. Defaults to 0.
	    count: Number of rows to return. Default set by administrator.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(args, str)
	common.validate_instance(args_delimiter, str)
	common.validate_gte_zero(offset)
	common.validate_gte_minus1(count)

	params = {
		"op": "execute_specific_query",
		"name": name,
		"offset": offset,
		"args-delimiter": args_delimiter,
	}

	if count != -1:
		params["count"] = count

	if args != "":
		params["args"] = args

	r = requests.get(session.url_base + "/query", headers=session.get_headers, params=params)  # noqa: S113
	return common.process_response(r)


def add_specific_query(session: IRODSHTTPSession, name: str, sql: str):
	"""
	Add a SpecificQuery to the iRODS zone.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the query to be added.
	    sql: The SQL attached to the query.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)
	common.validate_instance(sql, str)

	data = {"op": "add_specific_query", "name": name, "sql": sql}

	r = requests.post(session.url_base + "/query", headers=session.get_headers, data=data)  # noqa: S113
	return common.process_response(r)


def remove_specific_query(session: IRODSHTTPSession, name: str):
	"""
	Remove a SpecificQuery from the iRODS zone.

	Args:
	    session: An IRODSHTTPSession instance.
	    name: The name of the SpecificQuery to be removed.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	common.validate_instance(name, str)

	data = {"op": "remove_specific_query", "name": name}

	r = requests.post(session.url_base + "/query", headers=session.get_headers, data=data)  # noqa: S113
	return common.process_response(r)
