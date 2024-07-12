import requests
import json

# Manager that contains functions to access all iRODS HTTP API endpoints.
class manager:
    # Gets the username, password, and base url from the user to initialize a manager instance.
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.token = None

        self.collections = self.collections_manager(self.url_base)
        self.data_objects = self.data_objects_manager(self.url_base)
    
    def authenticate(self, username: str='', password: str='', openid_token: str=''):
        #TODO: Add error handling for authentication.
        if (not isinstance(username, str)):
            raise Exception('username must be a string')
        if (not isinstance(password, str)):
            raise Exception('password must be a string')
        if (not isinstance(openid_token, str)):
            raise Exception('openid_token must be a string')
        
        if (openid_token != ''): #TODO: Add openid authentication
            return('logged in with openid')

        r = requests.post(self.url_base + '/authenticate', auth=(username, password))

        if (r.status_code / 100 == 2):
            if (self.token == None):
                self.setToken(r.text)
            return(r.text)
        else:
            raise Exception('Failed to authenticate: ' + str(r.status_code))
        

    def setToken(self, token: str):
        if (not isinstance(token, str)):
            raise Exception('token must be a string')
        self.token = token

        self.collections.token = token
        self.data_objects.token = token

    # Returns the authentication token.
    def getToken(self):
        return(self.token)

    # Inner class to handle collections operations.
    class collections_manager:
        # Initializes collections_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None

        # Creates a new collection
        # params
        # - lpath: The absolute logical path of the collection to be created.
        # - create_intermediates (optional): Set to 1 to create intermediates, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def create(self, lpath: str, create_intermediates: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not create_intermediates == 0) and (not create_intermediates == 1)):
                raise Exception('create_intermediates must be an int 1 or 0')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'create',
                'lpath': lpath,
                'create-intermediates': create_intermediates
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code'] == 0 and rdict['created'] == False:
                    print('Failed to create collection: \'' + lpath + '\' already exists')
                elif rdict['irods_response']['status_code']:
                    print('Failed to create collection \'' + lpath + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']) + ' - ' + str(rdict['irods_response']['status_message']))
                else:
                    print('Collection \'' + lpath + '\' created successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Removes an existing collection.
        # params
        # - lpath: The absolute logical path of the collection to be removed.
        # - recurse (optional): Set to 1 to remove contents of the collection, otherwise set to 0. Defaults to 0.
        # - no_trash (optional): Set to 1 to move the collection to trash, 0 to permanently remove. Defaults to 0.
        # returns
        # - Status code and response message.
        def remove(self, lpath: str, recurse: int=0, no_trash: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not recurse == 0) and (not recurse == 1)):
                raise Exception('recurse must be an int 1 or 0')
            if ((not no_trash == 0) and (not no_trash == 1)):
                raise Exception('no_trash must be an int 1 or 0')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'remove',
                'lpath': lpath,
                'recurse': recurse,
                'no-trash': no_trash
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to remove collection \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Collection \'' + lpath + '\' removed successfully')
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to remove collection \'' + lpath + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']))

                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        

        # Gives information about a collection.
        # params
        # - lpath: The absolute logical path of the collection being queried.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # return
        # - Status code 2XX: Dictionary containing collection information.
        # - Other: Status code and return message.
        def stat(self, lpath: str, ticket: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(ticket, str)):
                raise Exception('ticket must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            params = {
                'op': 'stat',
                'lpath': lpath,
                'ticket': ticket
            }

            r = requests.get(self.url_base + '/collections', params=params, headers=headers)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to retrieve information for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Information for \'' + lpath + '\' retrieved successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        

        # Shows the contents of a collection
        # params
        # - lpath: The absolute logical path of the collection to have its contents listed.
        # - recurse (optional): Set to 1 to list the contents of objects in the collection, otherwise set to 0. Defaults to 0.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # return
        # - Status code 2XX: Dictionary containing collection information.
        # - Other: Status code and return message.
        def list(self, lpath: str, recurse: int=0, ticket: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not recurse == 0) and (not recurse == 1)):
                raise Exception('recurse must be an int 1 or 0')
            if (not isinstance(ticket, str)):
                raise Exception('ticket must be a string')

            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            params = {
                'op': 'list',
                'lpath': lpath,
                'recurse': recurse,
                'ticket': ticket
            }

            r = requests.get(self.url_base + '/collections', params=params, headers=headers)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to retrieve list for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('List for \'' + lpath + '\' retrieved successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Sets the permission of a user for a given collection
        # params
        # - lpath: The absolute logical path of the collection to have a permission set.
        # - entity_name: The name of the user or group having its permission set.
        # - permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def set_permission(self, lpath: str, entity_name: str, permission: str, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(entity_name, str)):
                raise Exception('entity_name must be a string')
            if (not isinstance(permission, str)):
                raise Exception('permission must be a string (\'null\', \'read\', \'write\', or \'own\')')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'set_permission',
                'lpath': lpath,
                'entity-name': entity_name,
                'permission': permission,
                'admin': admin
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to set permission for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Permission for \'' + lpath + '\' set successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Sets the inheritance for a collection.
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - enable: Set to 1 to enable inheritance, or 0 to disable.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def set_inheritance(self, lpath: str, enable: int, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not enable == 0) and (not enable == 1)):
                raise Exception('enable must be an int 1 or 0')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'set_inheritance',
                'lpath': lpath,
                'enable': enable,
                'admin': admin
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                rdict = r.json()

                operation = ''
                if (enable == 1):
                    operation = 'enabled'
                else:
                    operation = 'disabled'

                if rdict['irods_response']['status_code']:
                    print('Failed to set inheritance for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Inheritance for \'' + lpath + '\' ' + operation)
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Modifies permissions for multiple users or groups for a collection.
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_permissions(self, lpath: str, operations: dict, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
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
                'op': 'modify_permissions',
                'lpath': lpath,
                'operations': json.dumps(operations),
                'admin': admin
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to modify permissions for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Permissions for \'' + lpath + '\' modified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Modifies the metadata
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_metadata(self, lpath: str, operations: dict, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(operations, list)):
                raise Exception('operations must be a list of dictionaries')
            if (not isinstance(operations[0], dict)):
                raise Exception('operations must be a list of dictionaries')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
            # for entry in operations:
            #     print(str(entry))
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'modify_metadata',
                'lpath': lpath,
                'operations': json.dumps(operations),
                'admin': admin
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to modify metadata for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Metadata for \'' + lpath + '\' modified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Renames or moves a collection
        # params
        # - old_lpath: The current absolute logical path of the collection.
        # - new_lpath: The absolute logical path of the destination for the collection.
        # returns
        # - Status code and response message.
        def rename(self, old_lpath: str, new_lpath: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(old_lpath, str)):
                raise Exception('old_lpath must be a string')
            if (not isinstance(new_lpath, str)):
                raise Exception('new_lpath must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'rename',
                'old-lpath': old_lpath,
                'new-lpath': new_lpath
            }

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to rename \'' + old_lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('\'' + old_lpath + '\' renamed to \'' + new_lpath + '\'')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        # Renames or moves a collection
        # params
        # - lpath: The absolute logical path of the collection being touched.
        # - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
        # - reference (optional): The absolute logical path of the collection to use as a reference for mtime.
        # returns
        # - Status code and response message.
        def touch(self, lpath, seconds_since_epoch=-1, reference=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(seconds_since_epoch, int)):
                raise Exception('seconds_since_epoch must be an int')
            if (not isinstance(reference, str)):
                raise Exception('reference must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'touch',
                'lpath': lpath
            }

            if (seconds_since_epoch != -1):
                data['seconds-since-epoch'] = seconds_since_epoch
            
            if (reference != ''):
                data['reference'] = reference

            r = requests.post(self.url_base + '/collections', headers=headers, data=data)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to update mtime for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('mtime for \'' + lpath + '\' updated successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)



    # Inner class to handle data objects operations.
    class data_objects_manager:
        # Initializes data_objects_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None