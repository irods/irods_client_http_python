# iRODS HTTP API Python Wrapper

This is a Python wrapper for the [iRODS HTTP API](https://github.com/irods/irods_client_http_api). 

Documentation for the endpoint operations can be found [here](https://github.com/irods/irods_client_http_api/blob/main/API.md).

## Install

This wrapper is available via pip:

```
pip install irods-http
```

## Usage
To use the wrapper, follow the steps listed below.

```py
import irods_http

# Placeholder values needed for irods_http.authenticate()
url_base = "http://<host>:<port>/irods-http-api/<version>"
username = "<username>"
password = "<password>"

# Create an IRODSHTTPSession to an iRODS HTTP API server
session = irods_http.authenticate(url_base, username, password)

# Use the session for all other operations
response = irods_http.collections.create(session, '/<zone_name>/home/<username>/new_collection')

# Check the resopnse for errors
if response['status_code'] != 200:
    # Handle HTTP error.

if response['data']['irods_response']['status_code'] < 0:
    # Handle iRODS error.
```

The response dict will have this format:
```py
{
    'status_code': <integer>,
    'data': <dict>
}
```
where `status_code` is the HTTP status code from the response, and `data` is the result of the iRODS operation.

`response['data']` will contain a dict named `irods_response`, which will contain the `status_code` returned by the iRODS Server as well as any other expected properties.

```py
{
    'irods_response': {
        'status_code': <integer>
        # Other properties vary between endpoints
    }
}
```

When calling `data_objects.read()`, the `response['data']` will contain the raw bytes instead of a dict.

More information regarding iRODS HTTP API response data is available [here](https://github.com/irods/irods_client_http_api/blob/main/API.md).
