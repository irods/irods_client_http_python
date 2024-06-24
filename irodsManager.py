import requests
import json

class manager:
    def __init__(self, username, password, url_base, version):
        self.url_base = url_base
        self.version = version

        r = requests.post(url_base + version + '/authenticate', auth=(username, password))
        self.token = r.text

        self.collections = self.collections_manager(self.url_base, self.version, self.token)
    
    def getToken(self):
        print("token: " + self.token)

    #division as inner classes
    class collections_manager:
        def __init__(self, url_base, version, token):
            self.url_base = url_base
            self.version = version
            self.token = token
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + ']')
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + ']')
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + ']')
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + ']')
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
        def modify_permissions(self, lpath, operations, admin=0):
            for entry in operations:
                print(str(entry)) #TODO: Add processing to simplify initial parameter passed by user
            
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)
        
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

            if (r.status_code == 200):
                return(r.json())
            else:
                return('Error: [' + str(r.status_code) + '] ' + r.text)