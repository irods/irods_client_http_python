from irods_http_client.collection_operations import Collections
from irods_http_client.data_object_operations import DataObjects
from irods_http_client.query_operations import Queries
from irods_http_client.resource_operations import Resources
from irods_http_client.rule_operations import Rules
from irods_http_client.ticket_operations import Tickets
from irods_http_client.user_group_operations import UsersGroups
import requests

class IrodsHttpClient:
    def __init__(self, url_base: str):
        """ Gets the base url from the user to initialize a client instance. """
        self.url_base = url_base
        self.token = None

        self.collections = Collections(url_base)
        self.data_objects = DataObjects(url_base)
        self.queries = Queries(url_base)
        self.resources = Resources(url_base)
        self.rules = Rules(url_base)
        self.tickets = Tickets(url_base)
        self.users_groups = UsersGroups(url_base)


    def authenticate(self, username: str='', password: str='', openid_token: str=''):
        """
        Takes user credentials as parameters and attempts to authenticate and retrieve a token.

        Parameters
        - username: The username of the user to be authenticated.
        - password: The password of the user to be authenticated.
        
        Returns
        - User token generated by the server.
        """
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
        """
        Sets the token to be used when making requests.

        Parameters
        - token: The tokent to be set.
        """
        if (not isinstance(token, str)):
            raise TypeError('token must be a string')
        self.token = token

        self.collections.token = token
        self.data_objects.token = token
        self.queries.token = token
        self.resources.token = token
        self.rules.token = token
        self.tickets.token = token
        self.users_groups.token = token
    

    def getToken(self):
        """ Returns the authentication token currently in use """
        return(self.token)
    

    def info(self):
        """
        Gives general information about the iRODS server.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        
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