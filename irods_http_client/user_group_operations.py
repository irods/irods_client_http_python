import requests
import json

class UsersGroups:
    def __init__(self, url_base: str):
        """
        Initializes UsersGroups with a base url. 
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None


    def create_user(self, name: str, zone: str, user_type: str='rodsuser'):
        """
        Creates a new user. Requires rodsadmin or groupadmin privileges.
        
        Parameters
        - name: The name of the user to be created.
        - zone: The zone for the user to be created.
        - user_type (optional): Can be rodsuser, groupadmin, or rodsadmin. Defaults to rodsuser.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')
        if (not isinstance(user_type, str)):
            raise TypeError('user_type must be a string')
        if user_type and user_type not in ['rodsuser', 'groupadmin', 'rodsadmin']:
            raise ValueError('user_type must be set to rodsuser, groupadmin, or rodsadmin.')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'create_user',
            'name': name,
            'zone': zone,
            'user-type': user_type
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to create user \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('User \'' + name + '\' created successfully')
            
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


    def remove_user(self, name: str, zone: str):
        """
        Removes a user. Requires rodsadmin privileges.
        
        Parameters
        - name: The name of the user to be removed.
        - zone: The zone for the user to be removed.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'remove_user',
            'name': name,
            'zone': zone
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to remove user \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('User \'' + name + '\' removed successfully')
            
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

    
    def set_password(self, name: str, zone: str, new_password: str=''):
        """
        Changes a users password. Requires rodsadmin privileges.
        
        Parameters
        - name: The name of the user to have their password changed.
        - zone: The zone for the user to have their password changed.
        - new_password: The new password to set for the user.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')
        if (not isinstance(new_password, str)):
            raise TypeError('new_password must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'set_password',
            'name': name,
            'zone': zone,
            'new-password': new_password
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to change password for user \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Password for user \'' + name + '\' changed successfully')
            
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
    

    def set_user_type(self, name: str, zone: str, user_type: str):
        """
        Changes a users type. Requires rodsadmin privileges.
        
        Parameters
        - name: The name of the user to have their type updated.
        - zone: The zone for the user to have their type updated.
        - user_type: Can be rodsuser, groupadmin, or rodsadmin.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')
        if (not isinstance(user_type, str)):
            raise TypeError('user_type must be a string')
        if user_type and user_type not in ['rodsuser', 'groupadmin', 'rodsadmin']:
            raise ValueError('user_type must be set to rodsuser, groupadmin, or rodsadmin.')


        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'set_user_type',
            'name': name,
            'zone': zone,
            'new-user-type': user_type
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to change type for user \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Type for user \'' + name + '\' changed successfully')
            
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
    

    def create_group(self, name: str):
        """
        Creates a new group. Requires rodsadmin or groupadmin privileges.
        
        Parameters
        - name: The name of the group to be created.

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
            'op': 'create_group',
            'name': name
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to create group \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Group \'' + name + '\' created successfully')
            
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
    

    def remove_group(self, name: str):
        """
        Removes a group. Requires rodsadmin privileges.
        
        Parameters
        - name: The name of the group to be removed.
        
        Parameters

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
            'op': 'remove_group',
            'name': name
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to remove group \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Group \'' + name + '\' removed successfully')
            
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
    

    def add_to_group(self, user: str, zone: str, group: str=''):
        """
        Adds a user to a group. Requires rodsadmin or groupadmin privileges.
        
        Parameters
        - user: The user to be added to the group.
        - zone: The zone for the user to be added to the group.
        - group: The group for the user to be added to.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(user, str)):
            raise TypeError('user must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')
        if (not isinstance(group, str)):
            raise TypeError('group must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'add_to_group',
            'user': user,
            'zone': zone,
            'group': group
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to add user \'' + user + '\' to group \'' + group +  '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('User \'' + user + '\' added to group \'' + group +  '\' successfully')
            
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


    def remove_from_group(self, user: str, zone: str, group: str):
        """
        Removes a user from a group. Requires rodsadmin or groupadmin privileges.
        
        Parameters
        - user: The user to be removed from the group.
        - zone: The zone for the user to be removed from the group.
        - group: The group for the user to be removed from.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(user, str)):
            raise TypeError('user must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')
        if (not isinstance(group, str)):
            raise TypeError('group must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'remove_from_group',
            'user': user,
            'zone': zone,
            'group': group
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to remove user \'' + user + '\' from group \'' + group +  '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('User \'' + user + '\' removed from group \'' + group +  '\' successfully')
            
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
    

    def users(self):
        """
        Lists all users in the zone. Requires rodsadmin privileges.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')

        headers = {
            'Authorization': 'Bearer ' + self.token
        }

        params = {
            'op': 'users'
        }

        r = requests.get(self.url_base + '/users-groups', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to retrieve user list : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('User list retrieved successfully')
            
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
    

    def groups(self):
        """
        Lists all groups in the zone. Requires rodsadmin privileges.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')

        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        params = {
            'op': 'groups'
        }

        r = requests.get(self.url_base + '/users-groups', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to retrieve group list : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Group list retrieved successfully')
            
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

    
    def is_member_of_group(self, group: str, user: str, zone: str):
        """
        Returns whether a user is a member of a group or not.
        
        Parameters
        group: The group being checked.
        user: The user being checked.
        zone: The zone for the user being checked.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(group, str)):
            raise TypeError('group must be a string')
        if (not isinstance(user, str)):
            raise TypeError('user must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        params = {
            'op': 'is_member_of_group',
            'group': group,
            'user': user,
            'zone': zone
        }

        r = requests.get(self.url_base + '/users-groups', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to check membership in group \'' + group + '\' for user \'' + user + '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Membership in group \'' + group + '\' for user \'' + user + '\' checked successfully')
            
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


    def stat(self, name: str, zone: str=''):
        """
        Returns information about a user or group.
        
        Parameters
        - name: The name of the user or group to be accessed.
        - zone: The zone of the user to be accessed. Not required for groups.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token
        }

        params = {
            'op': 'stat',
            'name': name
        }

        if (zone != ''):
            params['zone'] = zone

        r = requests.get(self.url_base + '/users-groups', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to retrieve information for \'' + name + '\' : iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Information for \'' + name + '\' retrieved successfully')
            
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
    

    def modify_metadata(self, name: str, operations: list):
        """
        Modifies the metadata for a user or group. Requires rodsadmin privileges.
        
        Parameters
        - name: The user or group to be modified.
        - operations: The operations to be carried out.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(operations, list)):
            raise TypeError('operations must be a list of dictionaries')
        if (not isinstance(operations[0], dict)):
            raise TypeError('operations must be a list of dictionaries')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'op': 'modify_metadata',
            'name': name,
            'operations': json.dumps(operations)
        }

        r = requests.post(self.url_base + '/users-groups', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify metadata for \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Metadata for \'' + name + '\' modified successfully')
            
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
