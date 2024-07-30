#from irods_client import _collections
from irods_client.collection_operations import Collections
from irods_client.data_object_operations import DataObjects
from irods_client.query_operations import Queries
from irods_client.resource_operations import Resources
from irods_client.rule_operations import Rules
from irods_client.ticket_operations import Tickets
import requests

class IrodsClient:
    # Gets the username, password, and base url from the user to initialize a manager instance.
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.token = None

        self.collections = Collections(url_base)
        self.data_objects = DataObjects(url_base)
        self.queries = Queries(url_base)
        self.resources = Resources(url_base)
        self.rules = Rules(url_base)
        self.tickets = Tickets(url_base)

    def authenticate(self, username: str='', password: str='', openid_token: str=''):
        if (not isinstance(username, str)):
            raise TypeError('username must be a string')
        if (not isinstance(password, str)):
            raise TypeError('password must be a string')
        if (not isinstance(openid_token, str)):
            raise TypeError('openid_token must be a string')
        
        if (openid_token != ''): #TODO: Add openid authentication
            return('logged in with openid')

        r = requests.post(self.url_base + '/authenticate', auth=(username, password))

        if (r.status_code / 100 == 2):
            if (self.token == None):
                self.setToken(r.text)
            return(r.text)
        else:
            raise RuntimeError('Failed to authenticate: ' + str(r.status_code))
        

    def setToken(self, token: str):
        if (not isinstance(token, str)):
            raise TypeError('token must be a string')
        self.token = token

        self.collections.token = token
        self.data_objects.token = token
        self.queries.token = token
        self.resources.token = token
        self.rules.token = token
        self.tickets.token = token
    

    # Returns the authentication token.
    def getToken(self):
        return(self.token)
    

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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            print('Error: ' + r.text)

            return(r)