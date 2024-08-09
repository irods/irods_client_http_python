# iRODS HTTP API Python Wrapper

This is a Python wrapper for the [iRODS HTTP API](https://github.com/irods/irods_client_http_api). 

Documentation for the endpoint operations can be found [here](https://github.com/irods/irods_client_http_api/blob/main/API.md).

## Setup
**NOTICE:** This project is not yet available through pip. To use, clone the repository into the desired location.
```
git clone https://github.com/irods/irods_client_http_python.git
```
## Usage
To use the wrapper, follow the steps listed below.

```py
from irods_client import IrodsClient

# Create an instance of the wrapper with the base url of the iRODS server to
# be accessed. <host>, <port>, and <version> are placeholders, and need
# to be replaced by appropriate values.
api = IrodsClient('http://<host>:<port>/irods-http-api/<version>')

# Most endpoint operations require a user to be authenticated in order to
# be executed. Authenticate with a username and password, and store the
# token received.
token = api.authenticate('<username>', '<password>')

# When calling authenticate for the first time on a new instance, the token
# will be automatically set. To change the token to use operations as a
# different user, use `setToken()`.
api.setToken(token)

# Once a token is set, the rest of the operations can be used.
response = api.collections.create('/<zone_name>/home/<username>/new_collection')

# After executing the operation, the iRODS response data can be accessed like this.
if response['status_code'] != 200:
    # Handle HTTP error.

if response['data']['irods_response']['status_code'] < 0:
    # Handle iRODS error.
```

The data returned by the wrapper will be in this format:
```py
{
    'status_code': <integer>,
    'data': <dict>
}
```
where `status_code` is the HTTP status code from the response, and `data` is the result of the iRODS operation.

If there is data returned by the iRODS server, it will contain a dictionary called `irods_response`, which has an additional `status_code` indicating the result of the operation on the servers side, as well as any other expected data if the operation was successful.
```py
{
    'irods_response': {
        'status_code': <integer>
        # Other properties vary between endpoints
    }
}
```

More information regarding iRODS response data is available [here](https://github.com/irods/irods_client_http_api/blob/main/API.md).
