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
        self.information = self.information_manager(self.url_base)
        self.queries = self.query_manager(self.url_base)
        self.resources = self.resources_manager(self.url_base)
        self.rules = self.rules_manager(self.url_base)
        self.tickets = self.ticket_manager(self.url_base)
        self.users = self.user_manager(self.url_base)
        self.zones = self.zone_manager(self.url_base)
    
    def authenticate(self, username: str='', password: str='', openid_token: str=''):
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
        self.information.token = token
        self.queries.token = token
        self.resources.token = token
        self.rules.token = token
        self.tickets.token = token
        self.users.token = token
        self.zones.token = token

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
        # - lpath: The absolute logical path of the collection to have its permissions modified.
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
        
        # Modifies the metadata for a collection.
        # params
        # - lpath: The absolute logical path of the collection to have its metadata modified.
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
        
        # Updates mtime for a collection
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

        # Updates mtime for an existing data object or creates a new one
            # params
            # - lpath: The absolute logical path of the collection being touched.
            # - no_create (optional): Set to 1 to prevent creating a new object, otherwise set to 0.
            # - replica_number (optional): The replica number of the target replica.
            # - leaf_resources (optional): The resource holding an existing replica. If one does not exist, creates one.
            # - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
            # - reference (optional): The absolute logical path of the collection to use as a reference for mtime.
            # returns
            # - Status code and response message.
        def touch(self, lpath, no_create: int=0, replica_number: int=-1, leaf_resources: str='', seconds_since_epoch=-1, reference=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not no_create == 0) and (not no_create == 1)):
                raise Exception('no_create must be an int 1 or 0')
            if (not isinstance(replica_number, int)):
                raise Exception('replica_number must be an int')
            if (not isinstance(leaf_resources, str)):
                raise Exception('leaf_resources must be a string')
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
                'lpath': lpath,
                'no-create': no_create
            }

            if (seconds_since_epoch != -1):
                data['seconds-since-epoch'] = seconds_since_epoch

            if (replica_number != -1):
                data['replica-number'] = replica_number
            
            if (leaf_resources != ''):
                data['leaf-resources'] = leaf_resources
            
            if (reference != ''):
                data['reference'] = reference

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to touch data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Data object \'' + lpath + '\' touched successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Removes an existing data object.
        # params
        # - lpath: The absolute logical path of the data object to be removed.
        # - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
        # - no_trash (optional): Set to 1 to move the data object to trash, 0 to permanently remove. Defaults to 0.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def remove(self, lpath: str, catalog_only: int=0, no_trash: int=0, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if ((not catalog_only == 0) and (not catalog_only == 1)):
                raise Exception('recurse must be an int 1 or 0')
            if ((not no_trash == 0) and (not no_trash == 1)):
                raise Exception('no_trash must be an int 1 or 0')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'remove',
                'lpath': lpath,
                'catalog-only': catalog_only,
                'no-trash': no_trash,
                'admin': admin
            }

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to remove data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Data object \'' + lpath + '\' removed successfully')
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                rdict = r.json()
                print('Failed to remove data object \'' + lpath + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']))

                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
        

        # Calculates the checksum for a data object.
        # params
        # - lpath: The absolute logical path of the data object to be removed.
        # - resource (optional): The resource holding the existing replica.
        # - replica_number (optional): The replica number of the target replica.
        # - force (optional): Set to 1 to replace the existing checksum, otherwise set to 0. Defaults to 0.
        # - all (optional): Set to 1 to calculate the checksum for all replicas, otherwise set to 0. Defaults to 0.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def calculate_checksum(self, lpath: str, resource: str='', replica_number: int=-1, force: int=0, all: int=0, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(resource, str)):
                raise Exception('resource must be a string')
            if (not isinstance(replica_number, int)):
                raise Exception('replica_number must be an int')
            if ((not force == 0) and (not force == 1)):
                raise Exception('force must be an int 1 or 0')
            if ((not all == 0) and (not all == 1)):
                raise Exception('all must be an int 1 or 0')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'calculate_checksum',
                'lpath': lpath,
                'force': force,
                'all': all,
                'admin': admin
            }

            if (resource != ''):
                data['resource'] = resource

            if (replica_number != -1):
                data['replica-number'] = replica_number
            

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to calculate checksum for data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Checksum for data object \'' + lpath + '\' calculated successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Verifies the checksum for a data object.
        # params
        # - lpath: The absolute logical path of the data object to be removed.
        # - resource (optional): The resource holding the existing replica.
        # - replica_number (optional): The replica number of the target replica.
        # - compute_checksums (optional): Set to 1 to skip checksum calculation, otherwise set to 0. Defaults to 0.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def verify_checksum(self, lpath: str, resource: str='', replica_number: int=-1, compute_checksums: int=0, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(resource, str)):
                raise Exception('resource must be a string')
            if (not isinstance(replica_number, int)):
                raise Exception('replica_number must be an int')
            if ((not compute_checksums == 0) and (not compute_checksums == 1)):
                raise Exception('force must be an int 1 or 0')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'calculate_checksum',
                'lpath': lpath,
                'compute-checksums': compute_checksums,
                'admin': admin
            }

            if (resource != ''):
                data['resource'] = resource

            if (replica_number != -1):
                data['replica-number'] = replica_number
            

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to verify checksum for data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Checksum for data object \'' + lpath + '\' verified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Gives information about a data object.
        # params
        # - lpath: The absolute logical path of the data object being queried.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # return
        # - Status code 2XX: Dictionary containing data object information.
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

            r = requests.get(self.url_base + '/data-objects', params=params, headers=headers)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to retrieve information for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Information for \'' + lpath + '\' retrieved successfully')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Renames or moves a data object.
        # params
        # - old_lpath: The current absolute logical path of the data object.
        # - new_lpath: The absolute logical path of the destination for the data object.
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

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to rename \'' + old_lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('\'' + old_lpath + '\' renamed to \'' + new_lpath + '\'')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Copies a data object.
        # params
        # - src_lpath: The absolute logical path of the source data object.
        # - dst_lpath: The absolute logical path of the destination.
        # - src_resource: The absolute logical path of the source resource.
        # - dst_resource: The absolute logical path of the destination resource.
        # - overwrite: set to 1 to overwrite an existing objject, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def copy(self, src_lpath: str, dst_lpath: str, src_resource: str='', dst_resource: str='', overwrite: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(src_lpath, str)):
                raise Exception('src_lpath must be a string')
            if (not isinstance(dst_lpath, str)):
                raise Exception('dst_lpath must be a string')
            if (not isinstance(src_resource, str)):
                raise Exception('src_resource must be a string')
            if (not isinstance(dst_resource, str)):
                raise Exception('dst_lpath must be a string')
            if ((not overwrite == 0) and (not overwrite == 1)):
                raise Exception('overwrite must be an int 1 or 0')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'copy',
                'src-lpath': src_lpath,
                'dst-lpath': dst_lpath,
                'overwrite': overwrite
            }

            if (src_resource != ''):
                data['src-resource'] = src_resource

            if (dst_resource != ''):
                data['dst-resource'] = dst_resource

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to copy \'' + src_lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('\'' + src_lpath + '\' copied to \'' + dst_lpath + '\'')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Replicates a data object from one resource to another.
        # params
        # - lpath: The  absolute logical path of the data object to be replicated.
        # - src_resource: The absolute logical path of the source resource.
        # - dst_resource: The absolute logical path of the destination resource.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def replicate(self, lpath: str, src_resource: str='', dst_resource: str='', admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(src_resource, str)):
                raise Exception('src_resource must be a string')
            if (not isinstance(dst_resource, str)):
                raise Exception('dst_lpath must be a string')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'replicate',
                'lpath': lpath,
                'admin': admin
            }

            if (src_resource != ''):
                data['src-resource'] = src_resource

            if (dst_resource != ''):
                data['dst-resource'] = dst_resource

            print(data)

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()
                if rdict['irods_response']['status_code']:
                    print('Failed to replicate \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('\'' + lpath + '\' replicated from \'' + src_resource + '\' to \'' + dst_resource + '\'')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            
        # Trims an existing replica or removes its catalog entry.
        # params
        # - lpath: The  absolute logical path of the data object to be replicated.
        # - replica_number: The replica number of the target replica.
        # - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def trim(self, lpath: str, replica_number: int, catalog_only: int=0, admin: int=0):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(replica_number, int)):
                raise Exception('replica_number must be an int')
            if ((not catalog_only == 0) and (not catalog_only == 1)):
                raise Exception('catalog_only must be an int 1 or 0')
            if ((not admin == 0) and (not admin == 1)):
                raise Exception('admin must be an int 1 or 0')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'trim',
                'lpath': lpath,
                'replica-number': replica_number,
                'catalog-only': catalog_only,
                'admin': admin
            }

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to trim \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Sucessfully trimmed \'' + lpath + '\'')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Registers a data object/replica into the catalog.
        # params
        # - lpath: The  absolute logical path of the data object to be registered.
        # - ppath: The  absolute physical path of the data object to be registered.
        # - resource: The resource that will own the replica.
        # - as_additional_replica (optional): Set to 1 to register as a replica of an existing object, otherwise set to 0. Defaults to 0.
        # - data_size (optional): The size of the replica in bytes.
        # - checksum (optional): The checksum to associate with the replica.
        # returns
        # - Status code and response message.
        def register(self, lpath: str, ppath: str, resource: str, as_additional_replica: int=0, data_size: int=-1, checksum: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(ppath, str)):
                raise Exception('ppath must be a string')
            if (not isinstance(resource, str)):
                raise Exception('resource must be a string')
            if ((not as_additional_replica == 0) and (not as_additional_replica == 1)):
                raise Exception('as_additional_replica must be an int 1 or 0')
            if (not isinstance(data_size, int)):
                raise Exception('data_size must be an int')
            if (not isinstance(checksum, str)):
                raise Exception('checksum must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'register',
                'lpath': lpath,
                'ppath': ppath,
                'resource': resource,
                'as_additional_replica': as_additional_replica
            }

            if (data_size != -1):
                data['data-size'] = data_size

            if (checksum != ''):
                data['checksum'] = checksum

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to register \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Sucessfully registered \'' + lpath + '\'')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Reads bytes from a data object.
        # params
        # - lpath: The absolute logical path of the data object to be read from.
        # - offset (optional): The number of bytes to skip. Defaults to 0.
        # - count (optional): The number of bytes to read.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # returns
        # - Status code and response message.
        def read(self, lpath: str, offset: int=0, count: int=-1, ticket: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(offset, int)):
                raise Exception('offset must be an int')
            if (not isinstance(count, int)):
                raise Exception('count must be an int')
            if (not isinstance(ticket, str)):
                raise Exception('ticket must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            params = {
                'op': 'read',
                'lpath': lpath,
                'offset': offset
            }

            if (count != -1):
                params['count'] = count

            if (ticket != ''):
                params['ticket'] = ticket

            r = requests.get(self.url_base + '/data-objects',params=params , headers=headers)

            if (r.status_code / 100 == 2):
                print('Sucessfully read \'' + lpath + '\'')
                return(r.text)
            else:
                print('Error: ' + r.text)

                return(r)
        

        # Writes bytes to a data object.
        # params
        # - lpath: The absolute logical path of the data object to be read from.
        # - bytes: The bytes to be written.
        # - resource (optional): The root resource to write to.
        # - offset (optional): The number of bytes to skip. Defaults to 0.
        # - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
        # - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
        # - parallel_write_handle (optional): The handle to be used when writing in parallel.
        # - stream_index (optional): The stream to use when writing in parallel.
        # returns
        # - Status code and response message.
        def write(self, bytes, lpath: str='', resource: str='', offset: int=0, truncate: int=1, append: int=0, parallel_write_handle: str='', stream_index: int=-1):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(resource, str)):
                raise Exception('resource must be a string')
            if (not isinstance(offset, int)):
                raise Exception('offset must be an int')
            if ((not truncate == 0) and (not truncate == 1)):
                raise Exception('truncate must be an int 1 or 0')
            if ((not append == 0) and (not append == 1)):
                raise Exception('append must be an int 1 or 0')
            if (not isinstance(parallel_write_handle, str)):
                raise Exception('parallel_write_handle must be a string')
            if (not isinstance(stream_index, int)):
                raise Exception('stream_index must be an int')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'write',
                'offset': offset,
                'truncate': truncate,
                'append': append,
                'bytes': bytes
            }

            if (parallel_write_handle != ''):
                data['parallel-write-handle'] = parallel_write_handle
            else:
                data['lpath'] = lpath

            if (resource != ''):
                data['resource'] = resource

            if (stream_index != -1):
                data['stream-index'] = stream_index

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to write to \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Sucessfully wrote to \'' + lpath + '\'')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Initializes server-side state for parallel writing.
        # params
        # - lpath: The absolute logical path of the data object to be read from.
        # - stream_count: THe number of streams to open.
        # - resource (optional): The root resource to write to.
        # - offset (optional): The number of bytes to skip. Defaults to 0.s
        # - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
        # - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
        # - ticket (optional):  Ticket to be enabled before the operation. Defaults to an empty string.
        # returns
        # - Status code and response message.
        def parallel_write_init(self, lpath: str, stream_count: int, truncate: int=1, append: int=0, ticket: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(stream_count, int)):
                raise Exception('stream_count must be an int')
            if ((not truncate == 0) and (not truncate == 1)):
                raise Exception('truncate must be an int 1 or 0')
            if ((not append == 0) and (not append == 1)):
                raise Exception('append must be an int 1 or 0')
            if (not isinstance(ticket, str)):
                raise Exception('ticket must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'parallel_write_init',
                'lpath': lpath,
                'stream-count': stream_count,
                'truncate': truncate,
                'append': append
            }

            if (ticket != ''):
                data['ticket'] = ticket

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to open parallel write to \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Sucessfully opened parallel write to \'' + lpath + '\'')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Initializes server-side state for parallel writing.
        # params
        # - parallel_write_handle: Handle obtained from parallel_write_init
        # returns
        # - Status code and response message.
        def parallel_write_shutdown(self, parallel_write_handle: str):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(parallel_write_handle, str)):
                raise Exception('parallel_write_handle must be a string')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'parallel_write_shutdown',
                'parallel-write-handle': parallel_write_handle
            }

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to close parallel write: iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Sucessfully closed parallel write.')
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            
        
        # Modifies the metadata for a data object
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_metadata(self, lpath: str, operations: list, admin: int=0):
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
                'op': 'modify_metadata',
                'lpath': lpath,
                'operations': json.dumps(operations),
                'admin': admin
            }

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to modify metadata for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Metadata for \'' + lpath + '\' modified successfully')
                
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

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to set permission for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Permission for \'' + lpath + '\' set successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
        
        
        # Modifies permissions for multiple users or groups for a data object.
        # params
        # - lpath: The absolute logical path of the data object to have its permissions modified.
        # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_permissions(self, lpath: str, operations: list, admin: int=0):
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

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to modify permissions for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Permissions for \'' + lpath + '\' modified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)
            
        
        # Modifies properties of a single replica
        # WARNING: This operation requires rodsadmin level privileges and should only be used when there isn't a safer option.
        #          Misuse can lead to catalog inconsistencies and unexpected behavior.
        # params
        # - lpath: The absolute logical path of the data object to have its permissions modified.
        # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_replica(self, lpath: str, resource_hierarchy: str='', replica_number: int=-1, new_data_checksum: str='',
                           new_data_comments: str='', new_data_create_time: int=-1, new_data_expiry: int=-1,
                           new_data_mode: str='', new_data_modify_time: str='', new_data_path: str='',
                           new_data_replica_number: int=-1, new_data_replica_status: int=-1, new_data_resource_id: int=-1,
                           new_data_size: int=-1, new_data_status: str='', new_data_type_name: str='', new_data_version: int=-1):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be a string')
            if (not isinstance(resource_hierarchy, str)):
                raise Exception('resource_hierarchy must be a string')
            if (not isinstance(replica_number, int)):
                raise Exception('replica_number must be an int')
            if ((resource_hierarchy != '') and (replica_number != -1)):
                raise Exception('replica_hierarchy and replica_number are mutually exclusive')
            if (not isinstance(new_data_checksum, str)):
                raise Exception('new_data_checksum must be a string')
            if (not isinstance(new_data_comments, str)):
                raise Exception('new_data_comments must be a string')
            if (not isinstance(new_data_create_time, int)):
                raise Exception('new_data_create_time must be an int')
            if (not isinstance(new_data_expiry, int)):
                raise Exception('new_data_expiry must be an int')
            if (not isinstance(new_data_mode, str)):
                raise Exception('new_data_mode must be a string')
            if (not isinstance(new_data_modify_time, str)):
                raise Exception('new_data_modify_time must be a string')
            if (not isinstance(new_data_path, str)):
                raise Exception('new_data_path must be a string')
            if (not isinstance(new_data_replica_number, int)):
                raise Exception('new_data_replica_number must be an int')
            if (not isinstance(new_data_replica_status, int)):
                raise Exception('new_data_replica_status must be an int')
            if (not isinstance(new_data_resource_id, int)):
                raise Exception('new_data_resource_id must be an int')
            if (not isinstance(new_data_size, int)):
                raise Exception('new_data_size must be an int')
            if (not isinstance(new_data_status, str)):
                raise Exception('new_data_status must be a string')
            if (not isinstance(new_data_type_name, str)):
                raise Exception('new_data_type_name must be a string')
            if (not isinstance(new_data_version, int)):
                raise Exception('new_data_version must be an int')
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'modify_permissions',
                'lpath': lpath
            }

            if (resource_hierarchy != ''):
                data['resource-hierarchy'] = resource_hierarchy
            
            if (replica_number != -1):
                data['replica-numbeer'] = replica_number

            # Boolean for checking if the user passed in any new_data parameters
            no_params = True

            if (new_data_checksum != ''):
                data['new-data-checksum'] = new_data_checksum
                no_params = False
            
            if (new_data_comments != ''):
                data['new-data-comments'] = new_data_comments
                no_params = False
            
            if (new_data_create_time != -1):
                data['new-data-create-time'] = new_data_create_time
                no_params = False
            
            if (new_data_expiry != -1):
                data['new-data-expiry'] = new_data_expiry
                no_params = False

            if (new_data_mode != ''):
                data['new-data-mode'] = new_data_mode
                no_params = False

            if (new_data_modify_time != ''):
                data['new-data-modify-time'] = new_data_modify_time
                no_params = False

            if (new_data_path != ''):
                data['new-data-path'] = new_data_path
                no_params = False

            if (new_data_replica_number != -1):
                data['new-data-replica-number'] = new_data_replica_number
                no_params = False

            if (new_data_replica_status != -1):
                data['new-data-replica-status'] = new_data_replica_status
                no_params = False

            if (new_data_resource_id != -1):
                data['new-data-resource-id'] = new_data_resource_id
                no_params = False

            if (new_data_size != -1):
                data['new-data-size'] = new_data_size
                no_params = False
            
            if (new_data_status != ''):
                data['new-data-status'] = new_data_status
                no_params = False
            
            if (new_data_type_name != ''):
                data['new-data-type-name'] = new_data_type_name
                no_params = False
            
            if (new_data_version != ''):
                data['new-data-version'] = new_data_version
                no_params = False

            if (no_params):
                raise Exception('At least one new data parameter must be given.')    

            r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to modify \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('\'' + lpath + '\' modified successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)



    # Inner class to handle information operations.
    class information_manager:
        # Initializes information_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None
        
        # Gives general information about the iRODS server.
        # return
        # - Status code 2XX: Dictionary containing server information.
        # - Other: Status code and return message.
        def info(self):
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            r = requests.get(self.url_base + '/info', headers=headers)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                print('Server information for retrieved successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)



    # Inner class to handle query operations.
    class query_manager:
        # Initializes query_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None

    #TODO: Add query operationsw

    # Inner class to handle resource operations.
    class resources_manager:
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
            


    # Inner class to handle rule operations.
    class rules_manager:
        # Initializes rules_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None
    
        # Lists available rule engine plugin instances.
        # return
        # - Status code 2XX: List of rule engine plugins.
        # - Other: Status code and return message.
        def list_rule_engines(self):
            
            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            params = {
                'op': 'list_rule_engines'
            }

            r = requests.get(self.url_base + '/rules', params=params, headers=headers)

            rdict = r.json()

            if (r.status_code / 100 == 2):
                if rdict['irods_response']['status_code']:
                    print('Failed to retrieve rule engines list: iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Rule engine list retrieved successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(rdict)


        # Executes rule code.
        # params
        # - rule_text: The rule code to execute.
        # - rep_instance (optional): The rule engine plugin to run the rule-text against.
        # returns
        # - Status code and response message.
        def execute(self, rule_text: str, rep_instance: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(rule_text, str)):
                raise Exception('name must be a string')
            if (not isinstance(rep_instance, str)):
                raise Exception('name must be a string')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'execute',
                'rule-text': rule_text
            }

            if (rep_instance != ''):
                data['rep-instance'] = rep_instance

            r = requests.post(self.url_base + '/rules', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to remove execute rule: iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Rule executed successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            
        
        # Removes a delay rule from the catalog.
        # params
        # - rule_id: The id of the delay rule to be removed.
        # returns
        # - Status code and response message.
        def remove_delay_rule(self, rule_id: int):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(rule_id, int)):
                raise Exception('rule_id must be an int')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'remove_delay_rule',
                'rule-id': rule_id
            }

            r = requests.post(self.url_base + '/resources', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to remove delay rule: iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Delay rule removed successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)



    # Inner class to handle ticket operations.
    class ticket_manager:
        # Initializes ticket_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None

        # Creates a new ticket for a collection or data object.
        # params
        # - lpath: Absolute logical path to a data object or collection.
        # - type (optional): Read or write. Defaults to read.
        # - use_count (optional): Number of times the ticket can be used.
        # - write_data_object_count (optional): Max number of writes that can be performed.
        # - write_byte_count (optional): Max number of bytes that can be written.
        # - seconds_until_expiration (optional): Number of seconds before the ticket expires.
        # - users (optional): Comma-delimited list of users allowed to use the ticket
        # - groups (optional): Comma-delimited list of groups allowed to use the ticket
        # - hosts (optional): Comma-delimited list of hosts allowed to use the ticket
        # returns
        # - Status code and response message.
        def create(self, lpath: str, type: str='read', use_count: int=-1, write_data_object_count: int=-1, write_byte_count: int=-1,
                   seconds_until_expiration: int=-1, users: str='', groups: str='', hosts: str=''):
            if (self.token == None):
                raise Exception('No token set. Use setToken() to set the auth token to be used')
            if (not isinstance(lpath, str)):
                raise Exception('lpath must be an string')
            if ((type != 'read') and (type != 'write')):
                raise Exception('type must be either read or write')
            if (not isinstance(use_count, int)):
                raise Exception('use_count must be an int')
            if (not isinstance(write_data_object_count, int)):
                raise Exception('write_data_object_count must be an int')
            if (not isinstance(write_byte_count, int)):
                raise Exception('write_byte_count must be an int')
            if (not isinstance(seconds_until_expiration, int)):
                raise Exception('seconds_until_expiration must be an int')
            if (not isinstance(users, str)):
                raise Exception('users must be an string')
            if (not isinstance(groups, str)):
                raise Exception('groups must be an string')
            if (not isinstance(hosts, str)):
                raise Exception('hosts must be an string')

            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'create',
                'lpath': lpath
            }

            if (use_count != -1):
                data['use-count'] = use_count
            if (use_count != -1):
                data['write-data-object-count'] = write_data_object_count
            if (write_byte_count != -1):
                data['write-byte-count'] = write_byte_count
            if (seconds_until_expiration != -1):
                data['seconds-until-expiration'] = seconds_until_expiration
            if (users != ''):
                data['users'] = users
            if (groups != ''):
                data['groups'] = groups
            if (hosts != ''):
                data['hosts'] = hosts

            r = requests.post(self.url_base + '/tickets', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                print('Ticket generated successfully')
                
                return(rdict)
            else:
                print('Error: ' + r.text)

                return(r)
            

        # Removes an existing ticket.
        # params
        # - name: The ticket to be removed.
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

            r = requests.post(self.url_base + '/tickets', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                rdict = r.json()

                if rdict['irods_response']['status_code']:
                    print('Failed to remove ticket \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
                else:
                    print('Ticket \'' + name + '\' removed successfully')
                
                return(rdict)
            elif (r.status_code / 100 == 4):
                print('Failed to remove ticket \'' + name + '\'')

                return(r)
            else:
                print('Error: ' + r.text)

                return(r)
    


    # Inner class to handle user and group operations.
    class user_manager:
        # Initializes user_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None

        #TODO: Add user operations


    
    # Inner class to handle zone operations.
    class zone_manager:
        # Initializes zone_manager with variables from the parent class.
        def __init__(self, url_base: str):
            self.url_base = url_base
            self.token = None

        #TODO: Add zone operations