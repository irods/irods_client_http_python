import requests
import json

class Tickets:

    def __init__(self, url_base: str):
        """" 
        Initializes Tickets with a base url. 
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None


    def create(self, lpath: str, type: str='read', use_count: int=-1, write_data_object_count: int=-1, write_byte_count: int=-1,
                seconds_until_expiration: int=-1, users: str='', groups: str='', hosts: str=''):
        """
        Creates a new ticket for a collection or data object.
        
        Parameters
        - lpath: Absolute logical path to a data object or collection.
        - type (optional): Read or write. Defaults to read.
        - use_count (optional): Number of times the ticket can be used.
        - write_data_object_count (optional): Max number of writes that can be performed.
        - write_byte_count (optional): Max number of bytes that can be written.
        - seconds_until_expiration (optional): Number of seconds before the ticket expires.
        - users (optional): Comma-delimited list of users allowed to use the ticket.
        - groups (optional): Comma-delimited list of groups allowed to use the ticket.
        - hosts (optional): Comma-delimited list of hosts allowed to use the ticket.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be an string')
        if (not isinstance(type, str)):
            raise TypeError('type must be a string')
        if type not in ['read', 'write']:
            raise ValueError('type must be either read or write')
        if (not isinstance(use_count, int)):
            raise TypeError('use_count must be an int')
        if (not use_count >= -1):
            raise ValueError('use_count must be greater than or equal to 0 or flag value -1')
        if (not isinstance(write_data_object_count, int)):
            raise TypeError('write_data_object_count must be an int')
        if (not write_data_object_count >= -1):
            raise ValueError('write_data_object_count must be greater than or equal to 0 or flag value -1')
        if (not isinstance(write_byte_count, int)):
            raise TypeError('write_byte_count must be an int')
        if (not write_byte_count >= -1):
            raise ValueError('write_byte_count must be greater than or equal to 0 or flag value -1')
        if (not isinstance(seconds_until_expiration, int)):
            raise TypeError('seconds_until_expiration must be an int')
        if (not seconds_until_expiration >= -1):
            raise ValueError('seconds_until_expiration must be greater than or equal to 0 or flag value -1')
        if (not isinstance(users, str)):
            raise TypeError('users must be an string')
        if (not isinstance(groups, str)):
            raise TypeError('groups must be an string')
        if (not isinstance(hosts, str)):
            raise TypeError('hosts must be an string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'create',
            'lpath': lpath,
            'type': type
        }

        if (use_count != -1):
            data['use-count'] = use_count
        if (write_data_object_count != -1):
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

        print(data)

        r = requests.post(self.url_base + '/tickets', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            print('Ticket generated successfully')
            
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
        Removes an existing ticket.
        
        Parameters
        - name: The ticket to be removed.

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

        r = requests.post(self.url_base + '/tickets', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to remove ticket \'' + name + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Ticket \'' + name + '\' removed successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            print('Failed to remove ticket \'' + name + '\'')

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