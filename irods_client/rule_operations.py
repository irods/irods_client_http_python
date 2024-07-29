import requests
import json

class Rules:
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

        r = requests.post(self.url_base + '/rules', headers=headers, data=data)

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