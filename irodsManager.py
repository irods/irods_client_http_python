import requests

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
            self.name = 'Jebediah Dingleberry'
            self.url_base = url_base
            self.version = version
            self.token = token
        
        def printName(self):
            print(self.name)
        
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
        
        def set_permission():
            print("To be implemented")
        
        def set_inheritance():
            print("To be implemented")
        
        def modify_permissions():
            print("To be implemented")
        
        def modify_metadata():
            print("To be implemented")
        
        def rename():
            print("To be implemented")
        
        def touch():
            print("To be implemented")