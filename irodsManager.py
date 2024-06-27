import requests
import json

# Manager that contains functions to access all iRODS HTTP API endpoints.
class manager:
    # Gets the username, password, hostname, and version from the user to initialize a manager instance.
    # Calls the authenticate endpoint and stores the token, hostname, and version for later use.
    # Throws an error if the authentication fails.
    def __init__(self, username, password, url_base, version):
        self.url_base = url_base
        self.version = version #consider merging with url

        #TODO: Add error handling for authentication.
        r = requests.post(url_base + version + '/authenticate', auth=(username, password))
        self.token = r.text

        self.collections = self.collections_manager(self.url_base, self.version, self.token)
    
    # Prints the authentication token.
    def printToken(self):
        print("token: " + self.token)

    # Inner class to handle collections operations.
    class collections_manager:
        # Initializes collections_manager with variables from the parent class.
        def __init__(self, url_base, version, token):
            self.url_base = url_base
            self.version = version
            self.token = token
        
        # Creates a new collection
        # params
        # - lpath: The absolute logical path of the collection to be created.
        # - create_intermediates (optional): Set to 1 to creat intermediates, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def create(self, lpath, create_intermediates=0):
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'create',
                'lpath': lpath,
                'create-intermediates': create_intermediates
            }

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return('Success: [' + str(r.status_code) + '] Created collection at ' + lpath)
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Removes an existing collection.
        # params
        # - lpath: The absolute logical path of the collection to be removed.
        # - recurse (optional): Set to 1 to remove contents of the collection, otherwise set to 0. Defaults to 0.
        # - no_trash (optional): Set to 1 to move the collection to trash, 0 to permanently remove. Defaults to 0.
        # returns
        # - Status code and response message.
        def remove(self, lpath, recurse=0, no_trash=0):
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return('Success: [' + str(r.status_code) + '] Removed collection at ' + lpath)
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Gives information about a collection.
        # params
        # - lpath: The absolute logical path of the collection being queried.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # return
        # - Status code 2XX: Dictionary containing collection information.
        # - Other: Status code and return message.
        def stat(self, lpath, ticket=""):
            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            params = {
                'op': 'stat',
                'lpath': lpath,
                'ticket': ticket
            }

            r = requests.get(self.url_base + self.version + '/collections', params=params, headers=headers)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Shows the contents of a collection
        # params
        # - lpath: The absolute logical path of the collection to have its contents listed.
        # - recurse (optional): Set to 1 to list the contents of objects in the collection, otherwise set to 0. Defaults to 0.
        # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
        # return
        # - Status code 2XX: Dictionary containing collection information.
        # - Other: Status code and return message.
        def list(self, lpath, recurse=0, ticket=""):
            headers = {
                'Authorization': 'Bearer ' + self.token,
            }

            params = {
                'op': 'list',
                'lpath': lpath,
                'recurse': recurse,
                'ticket': ticket
            }

            r = requests.get(self.url_base + self.version + '/collections', params=params, headers=headers)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Sets the permission of a user for a given collection
        # params
        # - lpath: The absolute logical path of the collection to have a permission set.
        # - entity_name: The name of the user or group having its permission set.
        # - permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def set_permission(self, lpath, entity_name, permission, admin=0):
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Sets the inheritance for a collection.
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - enable: Set to 1 to enable inheritance, or 0 to disable.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def set_inheritance(self, lpath, enable, admin=0):
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Modifies permissions for multiple users or groups for a collection.
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_permissions(self, lpath, operations, admin=0):
            for entry in operations:
                print(str(entry))
            
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                #print(r.json().__class__)
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Modifies the metadata
        # params
        # - lpath: The absolute logical path of the collection to have its inheritance set.
        # - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
        # returns
        # - Status code and response message.
        def modify_metadata(self, lpath, operations, admin=0):
            for entry in operations:
                print(str(entry)) #TODO: Add processing to simplify initial parameter passed by user
            
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Renames or moves a collection
        # params
        # - old_lpath: The current absolute logical path of the collection.
        # - new_lpath: The absolute logical path of the destination for the collection.
        # returns
        # - Status code and response message.
        def rename(self, old_lpath, new_lpath):
            headers = {
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = {
                'op': 'rename',
                'old-lpath': old_lpath,
                'new-lpath': new_lpath
            }

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        # Renames or moves a collection
        # params
        # - lpath: The absolute logical path of the collection being touched.
        # - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
        # - reference (optional): The absolute logical path of the collection to use as a reference for mtime.
        # returns
        # - Status code and response message.
        def touch(self, lpath, seconds_since_epoch=-1, reference=''):
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

            r = requests.post(self.url_base + self.version + '/collections', headers=headers, data=data)

            if (r.status_code / 100 == 2):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)