import requests
import json

class Tickets:
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