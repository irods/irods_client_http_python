import config
import unittest
from irods_client.irodsClient import IrodsClient
import concurrent.futures
import os
import time
import logging


def setup_class(cls, opts):
    '''Initializes shared state needed by all test cases.


    This function is designed to be called in setUpClass().


    Arguments:
    cls -- The class to attach state to.
    opts -- A dict containing options for controlling the behavior of the function.
    '''


    # Used as a signal for determining whether setUpClass() succeeded or not.
    # If this results in being True, no tests should be allowed to run.
    cls._class_init_error = False
    cls._remove_rodsuser = False


    # Initialize the class logger.
    cls.logger = logging.getLogger(cls.__name__)


    log_level = config.test_config.get('log_level', logging.INFO)
    cls.logger.setLevel(log_level)


    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(logging.Formatter(f'[%(asctime)s] [{cls.__name__}] [%(levelname)s] %(message)s'))


    cls.logger.addHandler(ch)


    # Initialize state.


    if config.test_config.get('host', None) == None:
        cls.logger.debug('Missing configuration property: host')
        cls._class_init_error = True
        return


    if config.test_config.get('port', None) == None:
        cls.logger.debug('Missing configuration property: port')
        cls._class_init_error = True
        return


    if config.test_config.get('url_base', None) == None:
        cls.logger.debug('Missing configuration property: url_base')
        cls._class_init_error = True
        return


    cls.url_base = f"http://{config.test_config['host']}:{config.test_config['port']}{config.test_config['url_base']}"
    cls.url_endpoint = f'{cls.url_base}/{opts["endpoint_name"]}'

    cls.api = IrodsClient(cls.url_base)

    cls.zone_name = config.test_config['irods_zone']
    cls.server_hostname = config.test_config['irods_server_hostname']


    # create_rodsuser cannot be honored if init_rodsadmin is set to False.
    # Therefore, return immediately.
    if not opts.get('init_rodsadmin', True):
        cls.logger.debug('init_rodsadmin is False. Class setup complete.')
        return


    # Authenticate as a rodsadmin and store the bearer token.
    cls.rodsadmin_username = config.test_config['rodsadmin']['username']

    try:
        cls.rodsadmin_bearer_token = cls.api.authenticate(cls.rodsadmin_username, config.test_config['rodsadmin']['password'])
    except RuntimeError:
        cls._class_init_error = True
        cls.logger.debug(f'Failed to authenticate as rodsadmin [{cls.rodsadmin_username}].')
        return
    
    # Authenticate as a rodsuser and store the bearer token.
    # Should be replaced once user operations are implemented
    cls.rodsuser_username = config.test_config['rodsuser']['username']
    print(cls.rodsuser_username)
    print(config.test_config['rodsuser']['password'])
    try:
        cls.rodsuser_bearer_token = cls.api.authenticate(cls.rodsuser_username, config.test_config['rodsuser']['password'])
    except RuntimeError:
        cls._class_init_error = True
        cls.logger.debug(f'Failed to authenticate as rodsuser [{cls.rodsuser_username}].')
        return

    # Create a rodsuser for testing.
    # if not opts.get('create_rodsuser', True):
    #     cls.logger.debug('create_rodsuser is False. Class setup complete.')
    #     return


    # cls.rodsuser_username = config.test_config['rodsuser']['username']
    # headers = {'Authorization': f'Bearer {cls.rodsadmin_bearer_token}'}
    # r = requests.post(f'{cls.url_base}/users-groups', headers=headers, data={
    #     'op': 'create_user',
    #     'name': cls.rodsuser_username,
    #     'zone': cls.zone_name
    # })
    # cls.logger.debug(r.content)
    # if r.status_code != 200:
    #     cls._class_init_error = True
    #     cls.logger.debug(f'Failed to create rodsuser [{cls.rodsuser_username}].')
    #     return
    # cls._remove_rodsuser = True


    # Set the rodsuser's password.
    # r = requests.post(f'{cls.url_base}/users-groups', headers=headers, data={
    #     'op': 'set_password',
    #     'name': cls.rodsuser_username,
    #     'zone': cls.zone_name,
    #     'new-password': config.test_config['rodsuser']['password']
    # })
    # cls.logger.debug(r.content)
    # if r.status_code != 200:
    #     cls._class_init_error = True
    #     cls.logger.debug(f'Failed to set password for rodsuser [{cls.rodsuser_username}].')
    #     return


    # Authenticate as the rodsuser and store the bearer token.
    # r = requests.post(f'{cls.url_base}/authenticate', auth=(cls.rodsuser_username, config.test_config['rodsuser']['password']))
    # cls.logger.debug(r.content)
    # if r.status_code != 200:
    #     cls._class_init_error = True
    #     cls.logger.debug(f'Failed to authenticate as rodsuser [{cls.rodsuser_username}].')
    #     return
    # cls.rodsuser_bearer_token = r.text


    # cls.logger.debug('Class setup complete.')


def tear_down_class(cls):
    if cls._class_init_error:
        return

    if not cls._remove_rodsuser:
        return

    # headers = {'Authorization': f'Bearer {cls.rodsadmin_bearer_token}'}
    # r = requests.post(f'{cls.url_base}/users-groups', headers=headers, data={
    #     'op': 'remove_user',
    #     'name': cls.rodsuser_username,
    #     'zone': cls.zone_name
    # })
    # cls.logger.debug(r.content)
    # if r.status_code != 200:
    #     cls.logger.debug(f'Failed to remove rodsuser [{cls.rodsuser_username}].')
    #     return



# Tests for collections operations
class collectionsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'collections'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')

    #tests the create operation
    def testCreate(self):
        print(self.zone_name)
        self.api.setToken(self.rodsadmin_bearer_token)

        #clean up test collections
        self.api.collections.remove(f'/{self.zone_name}/home/new')
        self.api.collections.remove(f'/{self.zone_name}/home/test/folder')
        self.api.collections.remove(f'/{self.zone_name}/home/test')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.create, 0, 0)
        self.assertRaises(TypeError, self.api.collections.create, f'/{self.zone_name}/home/{self.rodsadmin_username}', '0')
        self.assertRaises(ValueError, self.api.collections.create, f'/{self.zone_name}/home/{self.rodsadmin_username}', 7)

        #test creating new collection
        response = self.api.collections.create(f'/{self.zone_name}/home/new')
        self.assertTrue(response['data']['created'])
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test creating existing collection
        response = self.api.collections.create(f'/{self.zone_name}/home/new')
        self.assertFalse(response['data']['created'])
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test invalid path
        response = self.api.collections.create('{self.zone_name}/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response['data']))

        #test create_intermediates
        response = self.api.collections.create(f'/{self.zone_name}/home/test/folder', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response['data']))
        response = self.api.collections.create(f'/{self.zone_name}/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
    

    #tests the remove operation
    def testRemove(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #clean up test collections
        self.api.collections.remove(f'/{self.zone_name}/home/new')
        self.api.collections.remove(f'/{self.zone_name}/home/test/folder')
        self.api.collections.remove(f'/{self.zone_name}/home/test')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.remove, 0, 0, 0)
        self.assertRaises(TypeError, self.api.collections.remove, f'/{self.zone_name}/home/{self.rodsadmin_username}', '0', 0)
        self.assertRaises(ValueError, self.api.collections.remove, f'/{self.zone_name}/home/{self.rodsadmin_username}', 5, 0)
        self.assertRaises(TypeError, self.api.collections.remove, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, '0')
        self.assertRaises(ValueError, self.api.collections.remove, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, 5)

        #test removing collection
        response = self.api.collections.create(f'/{self.zone_name}/home/new')
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
        response = self.api.collections.remove(f'/{self.zone_name}/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))
        #test invalid paths
        response = self.api.collections.stat(f'/{self.zone_name}/home/tensaitekinaaidorusama')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat(f'/{self.zone_name}/home/aremonainainaikoremonainainai')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat(f'/{self.zone_name}/home/binglebangledingledangle')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat(f'{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test recurse
        response = self.api.collections.create(f'/{self.zone_name}/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
        response = self.api.collections.remove(f'/{self.zone_name}/home/test', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -79000, \'status_message\': \'cannot remove non-empty collection: SYS_COLLECTION_NOT_EMPTY\'}}', str(response['data']))
        response = self.api.collections.remove(f'/{self.zone_name}/home/test', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))


    #tests the stat operation
    def testStat(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #clean up test collections
        self.api.collections.remove(f'/{self.zone_name}/home/new')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.stat, 0, 'ticket')
        self.assertRaises(TypeError, self.api.collections.stat, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0)
        
        #test invalid paths
        response = self.api.collections.stat(f'/{self.zone_name}/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('{self.zone_name}/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test valid path
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertTrue(response['data']['permissions'])
    

    #tests the list operation
    def testList(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #clean up test collections
        self.api.collections.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb')
        self.api.collections.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/albania')
        self.api.collections.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia')
        self.api.collections.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.list, 0, 'ticket')
        self.assertRaises(TypeError, self.api.collections.list, f'/{self.zone_name}/home/{self.rodsadmin_username}', '0', 'ticket')
        self.assertRaises(ValueError, self.api.collections.list, f'/{self.zone_name}/home/{self.rodsadmin_username}', 5, 'ticket')
        self.assertRaises(TypeError, self.api.collections.list, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, 0)

        #test empty collection
        response = self.api.collections.list(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual('None', str(response['data']['entries']))

        #test collection with one item
        self.api.collections.create(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia')
        response = self.api.collections.list(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia', str(response['data']['entries'][0]))

        #test collection with multiple items
        self.api.collections.create(f'/{self.zone_name}/home/{self.rodsadmin_username}/albania')
        self.api.collections.create(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia')
        response = self.api.collections.list(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/albania', str(response['data']['entries'][0]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia', str(response['data']['entries'][1]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia', str(response['data']['entries'][2]))

        #test without recursion
        self.api.collections.create(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb')
        response = self.api.collections.list(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/albania', str(response['data']['entries'][0]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia', str(response['data']['entries'][1]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia', str(response['data']['entries'][2]))
        self.assertEqual(len(response['data']['entries']), 3)

        #test with recursion
        response = self.api.collections.list(f'/{self.zone_name}/home/{self.rodsadmin_username}', 1)
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/albania', str(response['data']['entries'][0]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/bosnia', str(response['data']['entries'][1]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia', str(response['data']['entries'][2]))
        self.assertEqual(f'/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb', str(response['data']['entries'][3]))


    #tests the set permission operation
    def testSetPermission(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.set_permission, 0, 'jeb', 'read', 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, 'read', 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, f'/{self.zone_name}/home/{self.rodsadmin_username}', 'jeb', 0, 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, f'/{self.zone_name}/home/{self.rodsadmin_username}', 'jeb', 'read', '0')
        self.assertRaises(ValueError, self.api.collections.set_permission, f'/{self.zone_name}/home/{self.rodsadmin_username}', 'jeb', 'read', 5)
        
        #create new collection
        response = self.api.collections.create(f'/{self.zone_name}/home/setPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test no permission
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/setPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], -170000)

        #test set permission
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.set_permission(f'/{self.zone_name}/home/setPerms', self.rodsuser_username, 'read')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test with permission
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/setPerms')
        self.assertTrue(response['data']['permissions'])
        
        #test set permission null
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.set_permission(f'/{self.zone_name}/home/setPerms', self.rodsuser_username, 'null')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test no permission
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/setPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #remove the collection
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.remove(f'/{self.zone_name}/home/setPerms', 1, 1)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
    

    #tests the set inheritance operation
    def testSetInheritance(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.set_inheritance, 0, 0, 0)
        self.assertRaises(TypeError, self.api.collections.set_inheritance, f'/{self.zone_name}/home/{self.rodsadmin_username}', '0', 0)
        self.assertRaises(ValueError, self.api.collections.set_inheritance, f'/{self.zone_name}/home/{self.rodsadmin_username}', 5, 0)
        self.assertRaises(TypeError, self.api.collections.set_inheritance, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, '0')
        self.assertRaises(ValueError, self.api.collections.set_inheritance, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0, 5)

        #control
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertFalse(response['data']['inheritance_enabled'])

        #test enabling inheritance
        response = self.api.collections.set_inheritance(f'/{self.zone_name}/home/{self.rodsadmin_username}', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #check if changed
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertTrue(response['data']['inheritance_enabled'])

        #test disabling inheritance
        response = self.api.collections.set_inheritance(f'/{self.zone_name}/home/{self.rodsadmin_username}', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #check if changed
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertFalse(response['data']['inheritance_enabled'])

    
    #test the modify permissions operation
    def testModifyPermissions(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        ops_permissions = [
            {
                'entity_name': self.rodsuser_username,
                'acl': 'read'
            }
        ]

        ops_permissions_null = [
            {
                'entity_name': self.rodsuser_username,
                'acl': 'null'
            }
        ]

        #test param checking
        self.assertRaises(TypeError, self.api.collections.modify_permissions, 0, ops_permissions, 0)
        self.assertRaises(TypeError, self.api.collections.modify_permissions, f'/{self.zone_name}/home/{self.rodsadmin_username}', 5, 0)
        self.assertRaises(TypeError, self.api.collections.modify_permissions, f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_permissions, '0')
        self.assertRaises(ValueError, self.api.collections.modify_permissions, f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_permissions, 5)

        #create new collection
        response = self.api.collections.create(f'/{self.zone_name}/home/modPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test no permissions
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test set permissions
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.modify_permissions(f'/{self.zone_name}/home/modPerms', ops_permissions)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test with permissions
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/modPerms')
        self.assertTrue(response['data']['permissions'])

        #test set permissions nuil
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.modify_permissions(f'/{self.zone_name}/home/modPerms', ops_permissions_null)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test without permissions
        self.api.setToken(self.rodsuser_bearer_token)
        response = self.api.collections.stat(f'/{self.zone_name}/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #remove the collection
        self.api.setToken(self.rodsadmin_bearer_token)
        response = self.api.collections.remove(f'/{self.zone_name}/home/modPerms', 1, 1)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)


    #test the modify metadata operation
    def testModifyMetadata(self):
        self.api.setToken(self.rodsadmin_bearer_token)
        
        ops_metadata = [
            {
                'operation': 'add',
                'attribute': 'eyeballs',
                'value': 'itchy'
            }
        ]

        ops_metadata_remove = [
            {
                'operation': 'remove',
                'attribute': 'eyeballs',
                'value': 'itchy'
            }
        ]

        #test param checking
        self.assertRaises(TypeError, self.api.collections.modify_metadata, 0, ops_metadata, 0)
        self.assertRaises(TypeError, self.api.collections.modify_metadata, f'/{self.zone_name}/home/{self.rodsadmin_username}', 5, 0)
        self.assertRaises(TypeError, self.api.collections.modify_metadata, f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_metadata, '0')
        self.assertRaises(ValueError, self.api.collections.modify_metadata, f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_metadata, 5)

        #test adding and removing metadata
        response = self.api.collections.modify_metadata(f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_metadata)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
        response = self.api.collections.modify_metadata(f'/{self.zone_name}/home/{self.rodsadmin_username}', ops_metadata_remove)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
    

    #tests the rename operation
    def testRename(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.rename, f'/{self.zone_name}/home/{self.rodsadmin_username}', 0)
        self.assertRaises(TypeError, self.api.collections.rename, 0, f'/{self.zone_name}/home/pods')
        
        #test before move
        response = self.api.collections.stat(f'/{self.zone_name}/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertTrue(response['data']['permissions'])

        #test renaming
        response = self.api.collections.rename(f'/{self.zone_name}/home/{self.rodsadmin_username}', f'/{self.zone_name}/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test before move
        response = self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat(f'/{self.zone_name}/home/pods')
        self.assertTrue(response['data']['permissions'])

        #test renaming
        response = self.api.collections.rename(f'/{self.zone_name}/home/pods', f'/{self.zone_name}/home/{self.rodsadmin_username}')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))
    

    #tests the touch operation
    def testTouch(self):
        self.api.setToken(self.rodsadmin_bearer_token)
        
        self.assertTrue(True)

  
if __name__ == '__main__':
   unittest.main()



