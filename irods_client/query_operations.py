import requests
import json

class Queries:
    # Initializes query_manager with variables from the parent class.
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.token = None

    
    # Excecutes a GenQuery string and returns the results.
    # params
    # - query: The query being executed
    # - offset (optional): Number of rows to skip. Defaults to 0.
    # - count (optional): Number of rows to return. Default set by administrator.
    # - case_sensitive (optional): Set to 1 to execute a case sensitive query, otherwise set to 0. Defaults to 1. Only supported by GenQuery1.
    # - distinct (optional): Set to 1 to collapse duplicate rows, otherwise set to 0. Defaults to 1. Only supported by GenQuery 1
    # - parser (optional): User either genquery1 or genquery2. Defaults to genquery1.
    # - sql_only (optional): Set to 1 to execute an SQL only query, otherwise set to 0. Defaults to 0. Only supported by GenQuery2.
    # - zone (optional): The zone name. Defaults ot the local zone.
    # return
    # - Status code 2XX: The results of the GenQuery.
    # - Other: Status code and return message.
    def execute_genquery(self, query: str, offset: int=0, count: int=-1, case_sensitive: int=1, distinct: int=1,
                            parser: str='genquery1', sql_only: int=0, zone: str=''):
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(query, str)):
            raise TypeError('query must be a string')
        if ((not isinstance(offset, int))):
            raise TypeError('offset must be an int')
        if (not offset > 0):
            raise ValueError('offset must be greater than 0 or flag value -1')
        if ((not isinstance(count, int)) or (not count >= -1)):
            raise TypeError('count must be an int greater than 0 or flag value -1')
        if (not count > -1):
            raise ValueError('count must be greater than 0 or flag value -1')
        if (not isinstance(case_sensitive, int)):
            raise TypeError('case_sensitive must be an int 1 or 0')
        if ((not case_sensitive == 0) and (not case_sensitive == 1)):
            raise ValueError('case_sensitive must be an int 1 or 0')
        if (not isinstance(distinct, int)):
            raise TypeError('distinct must be an int 1 or 0')
        if ((not distinct == 0) and (not distinct == 1)):
            raise ValueError('distinct must be an int 1 or 0')
        if (not isinstance(parser, str)):
            raise TypeError('parser must be a string')
        if ((not parser == 'genquery1') and (not parser == 'genquery2')):
            raise ValueError('parser must be either \'genquery1\' or \'genquery2\'')
        if (not isinstance(sql_only, int)):
            raise TypeError('sql_only must be an int 1 or 0')
        if ((not sql_only == 0) and (not sql_only == 1)):
            raise ValueError('sql_only must be an int 1 or 0')
        if (not isinstance(zone, str)):
            raise TypeError('zone must be a string')

        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        params = {
            'op': 'execute_genquery',
            'query': query,
            'offset': offset,
            'parser': parser
        }

        if (count != -1):
            params['count'] = count

        if (zone != ''):
            params['zone'] = zone
        
        if (parser == 'genquery1'):
            params['case-sensitive'] = case_sensitive
            params['distinct'] = distinct
        else:
            params['sql-only'] = sql_only

        r = requests.get(self.url_base + '/query', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to execute query: iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Query executed successfully')
            
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
    

    # Excecutes a specific query and returns the results.
    # params
    # - name: The name of the query to be executed
    # - args (optional): 
    # - args_delimiter (optional): 
    # - offset (optional): Number of rows to skip. Defaults to 0.
    # - count (optional): Number of rows to return. Default set by administrator.
    # return
    # - Status code 2XX: The results of the specific query.
    # - Other: Status code and return message.
    def execute_specific_query(self, name: str, args: str='', args_delimiter: str=',', offset: int=0, count: int=-1):
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(args, str)):
            raise TypeError('args must be a string')
        if (not isinstance(args_delimiter, str)):
            raise TypeError('args_delimiter must be a string')
        if ((not isinstance(offset, int))):
            raise TypeError('offset must be an int')
        if (not offset > 0):
            raise ValueError('offset must be greater than 0 or flag value -1')
        if ((not isinstance(count, int))):
            raise TypeError('count must be an int')
        if (not count > -1):
            raise ValueError('count must be greater than 0 or flag value -1')

        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        params = {
            'op': 'execute_specific_query',
            'name': name,
            'offset': offset,
            'args-delimiter': args_delimiter
        }

        if (count != -1):
            params['count'] = count

        if (args != ''):
            params['args'] = args

        r = requests.get(self.url_base + '/query', headers=headers, params=params)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to execute query: iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Query executed successfully')
            
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
    

    # Adds a SpecificQuery to the iRODS zone.
    # params
    # - name: The name of the query to be added.
    # - sql: The SQL attached to the query.
    # return
    # - Status code and return message.
    def add_specific_query(self, name: str, sql: str):
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        if (not isinstance(sql, str)):
            raise TypeError('sql must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        data = {
            'op': 'add_specific_query',
            'name': name,
            'sql': sql
        }

        r = requests.post(self.url_base + '/query', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to add query: iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Query added successfully')
            
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
    

    # Removes a SpecificQuery from the iRODS zone.
    # params
    # - name: The name of the SpecificQuery to be removed
    # return
    # - Status code and return message.
    def remove_specific_query(self, name):
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(name, str)):
            raise TypeError('name must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        data = {
            'op': 'remove_specific_query',
            'name': name
        }

        r = requests.post(self.url_base + '/query', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to remove query: iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Query removed successfully')
            
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