"""Common utility functions for iRODS HTTP client operations."""


class HTTPSession:
	"""
	Encapsulates HTTP session details for iRODS HTTP API.

	This class binds together the base URL and authentication token that are
	always used together in API calls.

	Attributes:
	    url_base: The base URL for the iRODS HTTP API.
	    token: The authentication token for the API.
	"""

	def __init__(self, url_base: str, token: str):
		"""
		Initialize HTTPSession with URL and token.

		Args:
		    url_base: The base URL for the iRODS HTTP API.
		    token: The authentication token for the API.
		"""
		self.url_base = url_base
		self.token = token


def process_response(r):
	"""
	Process an HTTP response and return standardized response dict.

	Args:
	    r: The HTTP response object.

	Returns:
	    A dict with 'status_code' and 'data' keys containing the HTTP status code
	    and parsed JSON response body (or None if response is empty).
	"""
	rdict = r.json() if r.text != "" else None
	return {"status_code": r.status_code, "data": rdict}


def validate_not_none(x):
	"""
	Validate that a value is not None.

	Args:
	    x: The value to validate.

	Raises:
	    ValueError: If x is None
	"""
	if x is None:
		raise ValueError


def validate_instance(x, expected_type):
	"""
	Validate that a value is an instance of the expected type.

	Args:
	    x: The value to validate.
	    expected_type: The expected type.

	Raises:
	    TypeError: If x is not an instance of expected_type.
	"""
	if not isinstance(x, expected_type):
		raise TypeError


def validate_0_or_1(x):
	"""
	Validate that a value is either 0 or 1.

	Args:
	    x: The value to validate (must be an int).

	Raises:
	    TypeError: If x is not an integer.
	    ValueError: If x is not 0 or 1.
	"""
	validate_instance(x, int)
	if x not in [0, 1]:
		raise ValueError(f"{x} must be 0 or 1")


def validate_gte_zero(x):
	"""
	Validate that a value is greater than or equal to zero.

	Args:
	    x: The value to validate (must be an int).

	Raises:
	    TypeError: If x is not an integer.
	    ValueError: If x is less than 0.
	"""
	validate_instance(x, int)
	if not x >= 0:
		raise ValueError(f"{x} must be >= 0")


def validate_gte_minus1(x):
	"""
	Validate that a value is greater than or equal to -1.

	Args:
	    x: The value to validate (must be an int).

	Raises:
	    TypeError: If x is not an integer.
	    ValueError: If x is less than -1.
	"""
	validate_instance(x, int)
	if not x >= -1:
		raise ValueError(f"{x} must be >= 0, or flag value of -1")
