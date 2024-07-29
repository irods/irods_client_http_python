import requests
import json

class Resources:
        # Initializes resources_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None


        # Creates a new resource.
        # params
        # - name: The name of the resource to be created.
        # - type: The type of the resource to be created.
        # - host: The host of the resource to be created. May or may not be required depending on the resource type.
        # - vault_path: May or may not be required depending on the resource type.
        # - context:  May or may not be required depending on the resource type.
        # returns
        # - Status code and response message.
        def create(self, name: str, type: str, host: str, vault_path: str, context: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')
            if (not isinstance(type, str)):
                raise Exception('type must be a string')
            if (not isinstance(host, str)):
                raise Exception('host must be a string')
            if (not isinstance(vault_path, str)):
                raise Exception('vault_path must be a string')
            if (not isinstance(context, str)):
                raise Exception('context must be a string')

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
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
        

        # Removes an existing resource.
        # params
        # - name: The name of the resource to be removed.
        # returns
        # - Status code and response message.
        def remove(self, name: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')

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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to remove resource \'' + name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Modifies a property for a resource.
        # params
        # - name: The name of the resource to be removed.
        # - property: The property to be modified.
        # - value: The new value to be set.
        # returns
        # - Status code and response message.
        def modify(self, name: str, property: str, value: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')
            if ((property != 'name') and (property != 'type') and (property != 'host') and (property != 'vault_path')
                and (property != 'context') and (property != 'status') and (property != 'free_space') and (property != 'comments')
                and (property != 'information')):
                raise Exception('Invalid property. Valid properties:\n - name\n - type\n - host\n - ''vault_path\n - context' + 
                                '\n - status\n - free_space\n - comments\n - information')
            if (not isinstance(value, str)):
                raise Exception('value must be a string')
            if ((property == 'status') and (value != 'up') and (value != 'down')):
                raise Exception('status must be either \'up\' or \'down\'')

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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to modify property for \'' + name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)


        # Creates a parent-child relationship between two resources.
        # params
        # - parent_name: The name of the parent resource.
        # - child_name: The name of the child resource.
        # - context (optional): Additional information for the zone.
        # returns
        # - Status code and response message.
        def add_child(self, parent_name: str, child_name: str, context: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(parent_name, str)):
                raise Exception('parent_name must be a string')
            if (not isinstance(child_name, str)):
                raise Exception('child_name must be a string')
            if (not isinstance(context, str)):
                raise Exception('context must be a string')

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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to add \'' + child_name + '\' as a child of \'' + parent_name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
            
        
        # Removes a parent-child relationship between two resources.
        # params
        # - parent_name: The name of the parent resource.
        # - child_name: The name of the child resource.
        # returns
        # - Status code and response message.
        def remove_child(self, parent_name: str, child_name: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(parent_name, str)):
                raise Exception('parent_name must be a string')
            if (not isinstance(child_name, str)):
                raise Exception('child_name must be a string')

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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to remove \'' + child_name + '\' as a child of \'' + parent_name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
        

        # Rebalances a resource hierarchy.
        # params
        # - name: The name of the resource to be rebalanced.
        # returns
        # - Status code and response message.
        def rebalance(self, name: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')
            
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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to rebalance\'' + name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
    

        # Retrieves information for a resource.
        # params
        # - name: The name of the resource to be removed.
        # returns
        # - Status code and response message.
        def stat(self, name: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')
            
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
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to retrieve information for \'' + name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Modifies the metadata for a resource.
        # params
        # - name: The absolute logical path of the collection to have its metadata modified.
        # - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_metadata(self, name: str, operations: dict, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(name, str)):
                raise Exception('name must be a string')
            if (not isinstance(operations, list)):
                raise Exception('operations must be a list of dictionaries')
            if (not isinstance(operations[0], dict)):
                raise Exception('operations must be a list of dictionaries')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
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

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to modify metadata for \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Metadata for \'' + name + '\' modified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)