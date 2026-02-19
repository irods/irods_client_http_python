"""Main client module for iRODS HTTP API interactions."""

import requests

from irods_http_client import common


def authenticate(url_base: str, username: str, password: str) -> str:
	"""
	Authenticate using basic authentication credentials.

	Makes a POST request to {url_base}/authenticate with HTTP basic auth
	using the provided username and password.

	Args:
	    url_base: The base URL of the iRODS HTTP API server (e.g., "http://localhost:8080").
	    username: The username for authentication. Must be a non-empty string.
	    password: The password for authentication. Must be a string.

	Returns:
	    A token string that can be used for subsequent authenticated requests.

	Raises:
	    TypeError: If username or password are not strings.
	    ValueError: If username is empty.
	    RuntimeError: If authentication fails (non-2xx response status).

	Example:
	    >>> session = authenticate("http://localhost:8080", "user", "pass")
	    >>> print(session.token)
	    'eae9c...'
	"""
	common.validate_instance(username, str)
	common.validate_instance(password, str)
	if not username:
		raise ValueError("username cannot be empty")

	try:
		r = requests.post(f"{url_base}/authenticate", auth=(username, password))  # noqa: S113

		# Check for success status code (2xx)
		if 200 <= r.status_code < 300:  # noqa: PLR2004
			return common.HTTPSession(url_base, r.text)

		# Handle error status codes
		error_msg = f"Authentication failed with status {r.status_code}"
		if r.text:
			error_msg += f": {r.text}"
		raise RuntimeError(error_msg)

	except requests.exceptions.RequestException as e:
		raise RuntimeError(f"Authentication request failed: {e!s}") from e


def get_server_info(session: common.HTTPSession):
	"""
	Get general information about the iRODS server.

	Args:
	    session: An HTTPSession instance.

	Returns:
	    A dict containing the HTTP status code and iRODS response.
	    The iRODS response is only valid if no error occurred during HTTP communication.
	"""
	headers = {
		"Authorization": "Bearer " + session.token,
	}

	r = requests.get(session.url_base + "/info", headers=headers)  # noqa: S113
	return common.process_response(r)
