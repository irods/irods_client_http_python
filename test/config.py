import logging
from jsonschema import validate


test_config = {
   'log_level': logging.INFO,


   'host': 'localhost',
   'port': 9001,
   'url_base': '/irods-http-api/0.3.0',


   'rodsadmin': {
       'username': 'rods',
       'password': 'rods'
   },


   'rodsuser': {
       'username': 'jeb',
       'password': 'ding'
   },


   'irods_zone': 'tempZone',
   'irods_server_hostname': 'localhost'
}


schema = {
   '$schema': 'http://json-schema.org/draft-07/schema#',
   '$id': 'https://schemas.irods.org/irods-http-api/test/0.3.0/test-schema.json',
   'type': 'object',
   'properties': {
       'host': {
           'type': 'string'
       },
       'port': {
           'type': 'number'
       },
       'url_base': {
           'type': 'string'
       },
       'rodsadmin': {
           '$ref': '#/definitions/login'
       },
       'rodsuser': {
           '$ref': '#/definitions/login'
       },
       'irods_zone': {
           'type': 'string'
       },
       'irods_server_hostname': {
           'type': 'string'
       }
   },
   'required': [
       'host',
       'port',
       'url_base',
       'rodsadmin',
       'rodsuser',
       'irods_zone',
       'irods_server_hostname'
   ],
   'definitions': {
       'login': {
           'type': 'object',
           'properties': {
               'username': {
                   'type': 'string'
               },
               'password': {
                   'type': 'string'
               }
           },
           'required': [ 'username', 'password' ]
       }
   }
}


validate(instance=test_config, schema=schema)
