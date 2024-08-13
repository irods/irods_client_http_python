import requests
import json

class Resources:

    def __init__(self, url_base: str):
        """" 
        Initializes DataObjects with a base url. 
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None


    def create(self, name: str, type: str, host: str, vault_path: str, context: str):
        """
        Creates a new resource.
        
        Parameters
        - name: The name of the resource to be created.
        - type: The type of the resource to be created.
        - host: The host of the resource to be created. May or may not be required depending on the resource type.
        - vault_path: Path to the storage vault for the resource. May or may not be required depending on the resource type.
        - context:  May or may not be required depending on the resource type.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(type, str)):
            raise TypeError('type must be a string')
        if (not isinstance(host, str)):
            raise TypeError('host must be a string')
        if (not isinstance(vault_path, str)):
            raise TypeError('vault_path must be a string')
        if (not isinstance(context, str)):
            raise TypeError('context must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'create',
            'name': name,
            'type': type
        }

        if (host != ''):
            data['host'] = host
        
        if (vault_path != ''):
            data['vault-path'] = vault_path
        
        if (context != ''):
            data['context'] = context

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to create resource \'' + name + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']) + ' - ' + str(rdict['irods_response']['status_message']))
            else:
                print('Resource \'' + name + '\' created successfully')
            
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
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
    

    def remove(self, name: str):
        """
        Removes an existing resource.
        
        Parameters
        - name: The name of the resource to be removed.

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
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'remove',
            'name': name
        }

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to remove resource \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Resource \'' + name + '\' removed successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to remove resource \'' + name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def modify(self, name: str, property: str, value: str):
        """
        Modifies a property for a resource.
        
        Parameters
        - name: The name of the resource to be modified.
        - property: The property to be modified.
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
        if ((property != 'name') and (property != 'type') and (property != 'host') and (property != 'vault_path')
            and (property != 'context') and (property != 'status') and (property != 'free_space') and (property != 'comments')
            and (property != 'information')):
            raise ValueError('Invalid property. Valid properties:\n - name\n - type\n - host\n - ''vault_path\n - context' + 
                            '\n - status\n - free_space\n - comments\n - information')
        if (not isinstance(value, str)):
            raise TypeError('value must be a string')
        if ((property == 'status') and (value != 'up') and (value != 'down')):
            raise ValueError('status must be either \'up\' or \'down\'')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'modify',
            'name': name,
            'property': property,
            'value': value
        }

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify property for \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Property for \'' + name + '\' modified successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to modify property for \'' + name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )


    def add_child(self, parent_name: str, child_name: str, context: str=''):
        """
        Creates a parent-child relationship between two resources.
        
        Parameters
        - parent_name: The name of the parent resource.
        - child_name: The name of the child resource.
        - context (optional): Additional information for the zone.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(parent_name, str)):
            raise TypeError('parent_name must be a string')
        if (not isinstance(child_name, str)):
            raise TypeError('child_name must be a string')
        if (not isinstance(context, str)):
            raise TypeError('context must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'add_child',
            'parent-name': parent_name,
            'child-name': child_name
        }

        if (context != ''):
            data['context'] = context

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to add \'' + child_name + '\' as a child of \'' + parent_name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Added \'' + child_name + '\' as a child of \'' + parent_name + '\' successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to add \'' + child_name + '\' as a child of \'' + parent_name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        
    
    def remove_child(self, parent_name: str, child_name: str):
        """
        Removes a parent-child relationship between two resources.
        
        Parameters
        - parent_name: The name of the parent resource.
        - child_name: The name of the child resource.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(parent_name, str)):
            raise TypeError('parent_name must be a string')
        if (not isinstance(child_name, str)):
            raise TypeError('child_name must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'remove_child',
            'parent-name': parent_name,
            'child-name': child_name
        }

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to remove \'' + child_name + '\' as a child of \'' + parent_name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Removed \'' + child_name + '\' as a child of \'' + parent_name + '\' successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to remove \'' + child_name + '\' as a child of \'' + parent_name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
    

    def rebalance(self, name: str):
        """
        Rebalances a resource hierarchy.
        
        Parameters
        - name: The name of the resource to be rebalanced.

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
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'rebalance',
            'name': name
        }

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to rebalance \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + name + '\' rebalanced successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to rebalance\'' + name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )


    def stat(self, name: str):
        """
        Retrieves information for a resource.
        
        Parameters
        - name: The name of the resource to be accessed.

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
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        params = {
            'op': 'stat',
            'name': name
        }

        r = requests.get(self.url_base + '/resources', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to retrieve information for \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Information for \'' + name + '\' retrieved successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to retrieve information for \'' + name + '\'')

            return(r)
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def modify_metadata(self, name: str, operations: dict, admin: int=0):
        """
        Modifies the metadata for a resource.
        
        Parameters
        - name: The absolute logical path of the resource to have its metadata modified.
        - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

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
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'modify_metadata',
            'name': name,
            'operations': json.dumps(operations),
            'admin': admin
        }

        r = requests.post(self.url_base + '/resources', headers=headers, data=data)

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
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )