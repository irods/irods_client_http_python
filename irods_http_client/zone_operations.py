import requests
import json

class Zones:
    def __init__(self, url_base: str):
        """
        Initializes Zones with a base url. 
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None
    

    def add(self, name: str, connection_info: str='', comment: str=''):
        """
        Adds a remote zone to the local zone. Requires rodsadmin privileges.

        Parameters
        - name: The name of the zone to be added.
        - connection_info (optional): The host and port to connect to. If included, must be in the format <host>:<port>
        - comment (optional): The comment to attach to the zone.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(connection_info, str)):
            raise TypeError('connection_info must be a string')
        if (not isinstance(comment, str)):
            raise TypeError('comment must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'add',
            'name': name
        }

        if (connection_info != ''):
            data['connection-info'] = connection_info
        if (comment != ''):
            data['comment'] = comment

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to add zone \'' + name + '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Zone \'' + name + '\' added successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRODS Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )


    def remove(self, name: str):
        """
        Removes a remote zone from the local zone. Requires rodsadmin privileges.

        Parameters
        - name: The zone to be removed

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'remove',
            'name': name
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to remove zone \'' + name + '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Zone \'' + name + '\' removed successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRODS Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )


    def modify(self, name: str, property: str, value: str):
        """
        Modifies properties of a remote zone. Requires rodsadmin privileges.

        Parameters
        - name: The name of the zone to be modified.
        - property: The property to be modified. Can be set to 'name', 'connection_info', or 'comment'.
                    The value for 'connection_info' must be in the format <host>:<port>.
        - value: The new value to be set.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(property, str)):
            raise TypeError('property must be a string')
        if (not isinstance(value, str)):
            raise TypeError('value must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'modify',
            'property': property,
            'value': value
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to modify zone \'' + name + '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Zone \'' + name + '\' modified successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRODS Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )


    def report(self):
        """
        Returns information about the iRODS zone. Requires rodsadmin privileges.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        params = {
            'op': 'report'
        }

        r = requests.get(self.url_base + '/users-groups', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to retrieve information for the iRODS zone : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('GroInformation for the iRODS zone retrieved successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRODS Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )