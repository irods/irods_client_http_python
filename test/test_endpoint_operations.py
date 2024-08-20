import config
import unittest
from irods_http_client.irodsHttpClient import IrodsHttpClient
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

    cls.api = IrodsHttpClient(cls.url_base)

    cls.zone_name = config.test_config['irods_zone']
    cls.host = config.test_config['irods_server_hostname']


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
    # Currently, the user specified in the config file must exist before running tests
    cls.rodsuser_username = config.test_config['rodsuser']['username']
    print(cls.rodsuser_username)
    print(config.test_config['rodsuser']['password'])
    try:
        cls.rodsuser_bearer_token = cls.api.authenticate(cls.rodsuser_username, config.test_config['rodsuser']['password'])
    except RuntimeError:
        cls._class_init_error = True
        cls.logger.debug(f'Failed to authenticate as rodsuser [{cls.rodsuser_username}].')
        return

    cls.logger.debug('Class setup complete.')


def tear_down_class(cls):
    if cls._class_init_error:
        return

    if not cls._remove_rodsuser:
        return


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

  
# Tests for data object operations
class dataObjectsTests(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'data_objects'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')

    def testCommonOperations(self):
        self.api.setToken(self.rodsadmin_bearer_token)
        print(self.rodsadmin_bearer_token)

        try:
            # Create a unixfilesystem resource.
            r = self.api.resources.create('resource', 'unixfilesystem', self.host, '/tmp/resource', '')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            self.api.setToken(self.rodsuser_bearer_token)
            # Create a non-empty data object
            print(self.api.collections.stat(f'/{self.zone_name}/home/{self.rodsuser_username}'))
            r = self.api.data_objects.write('These are the bytes being written to the object', f'/{self.zone_name}/home/{self.rodsuser_username}/file.txt', 'resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Replicate the data object
            r = self.api.data_objects.replicate(f'/{self.zone_name}/home/{self.rodsuser_username}/file.txt', dst_resource='resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Show that there are two replicas
            # TODO: Implement once query operations are completed

            # Trim the first data object
            r = self.api.data_objects.trim(f'/{self.zone_name}/home/{self.rodsuser_username}/file.txt', 0)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Rename the data object
            r = self.api.data_objects.rename(f'/{self.zone_name}/home/{self.rodsuser_username}/file.txt', f'/{self.zone_name}/home/{self.rodsuser_username}/newfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Copy the data object
            r = self.api.data_objects.copy(f'/{self.zone_name}/home/{self.rodsuser_username}/newfile.txt', f'/{self.zone_name}/home/{self.rodsuser_username}/anotherfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Set permission on the object
            r = self.api.data_objects.set_permission(f'/{self.zone_name}/home/{self.rodsuser_username}/anotherfile.txt', 'rods', 'read')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Confirm that the permission has been set
            r = self.api.data_objects.stat(f'/{self.zone_name}/home/{self.rodsuser_username}/anotherfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
            self.assertIn({
                'name': 'rods',
                'zone': self.zone_name,
                'type': 'rodsadmin',
                'perm': 'read_object'
            }, r['data']['permissions'])

        finally:
            # Remove the data objects
            r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsuser_username}/anotherfile.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsuser_username}/newfile.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            self.api.setToken(self.rodsadmin_bearer_token)

            # Remove the resource
            r = self.api.resources.remove('resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)


    def testChecksums(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a unixfilesystem resource.
        r = self.api.resources.create('newresource', 'unixfilesystem', self.host, '/tmp/newresource', '')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Create a non-empty data object
        r = self.api.data_objects.write('These are the bytes being written to the object', f'/{self.zone_name}/home/{self.rodsadmin_username}/file.txt', 'newresource')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Replicate the data object
        r = self.api.data_objects.replicate(f'/{self.zone_name}/home/{self.rodsadmin_username}/file.txt', dst_resource='newresource')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that there are two replicas
        # TODO: Implement once query operations are completed

        try:
            # Calculate a checksum for the first replica
            r = self.api.data_objects.calculate_checksum(f'/{self.zone_name}/home/{self.rodsadmin_username}/file.txt', replica_number=0)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Verify checksum information across all replicas.
            r = self.api.data_objects.verify_checksum(f'/{self.zone_name}/home/{self.rodsadmin_username}/file.txt')
            print(r)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
        finally:
            # Remove the data objects
            r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/file.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Remove the resource
            r = self.api.resources.remove('newresource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
        
    
    def testTouch(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Test touching non existant data object with no_create
        r = self.api.data_objects.touch(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt', 1)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the object has not been created
        r = self.api.data_objects.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], -171000)

        # Test touching non existant object without no_create
        r = self.api.data_objects.touch(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt', 0)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the object has been created
        r = self.api.data_objects.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Test touching existing object without no_create
        r = self.api.data_objects.touch(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt', 1)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Remove the object
        r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)


    def testRegister(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a non-empty local file.
        content = 'data'
        with open('/tmp/register-demo.txt', 'w') as f:
                f.write(content)
        
        # Show the data object we want to create via registration does not exist.
        r = self.api.data_objects.stat(f'/{self.zone_name}/home/{self.rodsadmin_username}/register-demo.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], -171000)

        try:
            # Create a unixfilesystem resource.
            r = self.api.resources.create('register_resource', 'unixfilesystem', self.host, '/tmp/register_resource', '')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Register the local file into the catalog as a new data object.
            # We know we're registering a new data object because the "as-additional-replica"
            # parameter isn't set to 1.
            r = self.api.data_objects.register(f'/{self.zone_name}/home/{self.rodsadmin_username}/register-demo.txt', '/tmp/register-demo.txt', 'register_resource', data_size=len(content))
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Show a new data object exists with the expected replica information.
            # TODO: add when query operations are implemented
        finally:
            # Unregisterr the dataq object
            r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/register-demo.txt', 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Remove the resource
            r = self.api.resources.remove('register_resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

    
    def testParallelWrite(self):
        self.api.setToken(self.rodsadmin_bearer_token)
        self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt', 0, 1)

        # Open parallel write
        r = self.api.data_objects.parallel_write_init(f'/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt', 3)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        handle = r['data']['parallel_write_handle']
        
        try:
            # Write to the data object using the parallel write handle.
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                for e in enumerate(['A', 'B', 'C']):
                    count = 10
                    futures.append(executor.submit(
                        self.api.data_objects.write, bytes=e[1] * count, offset=e[0] * count, stream_index=e[0], parallel_write_handle=handle
                    ))
                for f in concurrent.futures.as_completed(futures):
                    r = f.result()
                    self.assertEqual(r['data']['irods_response']['status_code'], 0)
        finally:
            # Close parallel write
            r = self.api.data_objects.parallel_write_shutdown(handle)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Remove the object
            r = self.api.data_objects.remove(f'/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)




# Tests for resources operations
class resourcesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'resources'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')

    def testCommonOperations(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        #TEMPORARY pre-test cleanup
        #test is currently not passing, so cleanup occusrs at the beginning to allow it to be run more than once in a row
        self.api.resources.remove_child('test_repl', 'test_ufs0')
        self.api.resources.remove_child('test_repl', 'test_ufs1')
        self.api.resources.remove('test_ufs0')
        self.api.resources.remove('test_ufs1')
        self.api.resources.remove('test_repl')

        resc_repl = 'test_repl'
        resc_ufs0 = 'test_ufs0'
        resc_ufs1 = 'test_ufs1'

        # Create three resources (replication w/ two unixfilesystem resources).
        r = self.api.resources.create(resc_repl, 'replication', '', '', '')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        
        # Show the replication resource was created.
        r = self.api.resources.stat(resc_repl)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(r['data']['exists'], True)
        self.assertIn('id', r['data']['info'])
        self.assertEqual(r['data']['info']['name'], resc_repl)
        self.assertEqual(r['data']['info']['type'], 'replication')
        self.assertEqual(r['data']['info']['zone'], 'tempZone')
        self.assertEqual(r['data']['info']['host'], 'EMPTY_RESC_HOST')
        self.assertEqual(r['data']['info']['vault_path'], 'EMPTY_RESC_PATH')
        self.assertIn('status', r['data']['info'])
        self.assertIn('context', r['data']['info'])
        self.assertIn('comments', r['data']['info'])
        self.assertIn('information', r['data']['info'])
        self.assertIn('free_space', r['data']['info'])
        self.assertIn('free_space_last_modified', r['data']['info'])
        self.assertEqual(r['data']['info']['parent_id'], '')
        self.assertIn('created', r['data']['info'])
        self.assertIn('last_modified', r['data']['info'])
        self.assertIn('last_modified_millis', r['data']['info'])

        # Capture the replication resource's id.
        # This resource is going to be the parent of the unixfilesystem resources.
        # This value is needed to verify the relationship.
        resc_repl_id = r['data']['info']['id']

        for resc_name in [resc_ufs0, resc_ufs1]:
            with self.subTest(f'Create and attach resource [{resc_name}] to [{resc_repl}]'):
                vault_path = f'/tmp/{resc_name}_vault'

                # Create a unixfilesystem resource.
                r = self.api.resources.create(resc_name, 'unixfilesystem', self.host, vault_path, '')
                self.assertEqual(r['data']['irods_response']['status_code'], 0)

                # Add the unixfilesystem resource as a child of the replication resource.
                r = self.api.resources.add_child(resc_repl, resc_name)
                self.assertEqual(r['data']['irods_response']['status_code'], 0)

                # Show that the resource was created and configured successfully.
                r = self.api.resources.stat(resc_name)
                self.assertEqual(r['data']['irods_response']['status_code'], 0)
                self.assertEqual(r['data']['exists'], True)
                self.assertIn('id', r['data']['info'])
                self.assertEqual(r['data']['info']['name'], resc_name)
                self.assertEqual(r['data']['info']['type'], 'unixfilesystem')
                self.assertEqual(r['data']['info']['zone'], self.zone_name)
                self.assertEqual(r['data']['info']['host'], self.host)
                self.assertEqual(r['data']['info']['vault_path'], vault_path)
                self.assertIn('status', r['data']['info'])
                self.assertIn('context', r['data']['info'])
                self.assertIn('comments', r['data']['info'])
                self.assertIn('information', r['data']['info'])
                self.assertIn('free_space', r['data']['info'])
                self.assertIn('free_space_last_modified', r['data']['info'])
                self.assertEqual(r['data']['info']['parent_id'], resc_repl_id)
                self.assertIn('created', r['data']['info'])
                self.assertIn('last_modified', r['data']['info'])

        # Create a data object targeting the replication resource.
        data_object = f'/{self.zone_name}/home/{self.rodsadmin_username}/resource_obj'
        r = self.api.data_objects.write('These are the bytes to be written', data_object, resc_repl, 0)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show there are two replicas under the replication resource hierarchy.
        r = self.api.queries.execute_genquery(f"select DATA_NAME, RESC_NAME where DATA_NAME = '{os.path.basename(data_object)}'")
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(len(r['data']['rows']), 2)

        resc_tuple = (r['data']['rows'][0][1], r['data']['rows'][1][1])
        self.assertIn(resc_tuple, [(resc_ufs0, resc_ufs1), (resc_ufs1, resc_ufs0)])

        # Trim a replica.
        r = self.api.data_objects.trim(data_object, 0)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show there is only one replica under the replication resource hierarchy.
        r = self.api.queries.execute_genquery(f"select DATA_NAME, RESC_NAME where DATA_NAME = '{os.path.basename(data_object)}'")
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(len(r['data']['rows']), 1)

        # Launch rebalance
        r = self.api.resources.rebalance(resc_repl)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Give the rebalance operation time to complete!
        time.sleep(3)

        #
        # Clean-up
        #

        # Remove the data object.
        r = self.api.data_objects.remove(data_object, 0, 1)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

         # Remove resources.
        for resc_name in [resc_ufs0, resc_ufs1]:
            with self.subTest(f'Detach and remove resource [{resc_name}] from [{resc_repl}]'):
                # Detach ufs resource from the replication resource.
                r = self.api.resources.remove_child(resc_repl, resc_name)
                self.assertEqual(r['data']['irods_response']['status_code'], 0)

                # Remove ufs resource.
                r = self.api.resources.remove(resc_name)
                self.assertEqual(r['data']['irods_response']['status_code'], 0)

                # Show that the resource no longer exists.
                r = self.api.resources.stat(resc_name)
                self.assertEqual(r['data']['irods_response']['status_code'], 0)
                self.assertEqual(r['data']['exists'], False)
        
        # Remove replication resource.
        r = self.api.resources.remove(resc_repl)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the resource no longer exists.
        r = self.api.resources.stat(resc_repl)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(r['data']['exists'], False)


    def testModifyMetadata(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a unixfilesystem resource.
        r = self.api.resources.create('metadata_demo', 'unixfilesystem', self.host, '/tmp/metadata_demo_vault', '')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        
        operations = [
            {
                'operation': 'add',
                'attribute': 'a1',
                'value': 'v1',
                'units': 'u1'
            }
        ]

        # Add the metadata to the resource
        r = self.api.resources.modify_metadata('metadata_demo', operations)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the metadata is on the resource
        r = self.api.queries.execute_genquery("select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'")
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(r['data']['rows'][0][0], 'metadata_demo')

        # Remove the metadata from the resource.
        operations = [
            {
                'operation': 'remove',
                'attribute': 'a1',
                'value': 'v1',
                'units': 'u1'
            }
        ]

        r = self.api.resources.modify_metadata('metadata_demo', operations)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the metadata is no longer on the resource
        r = self.api.queries.execute_genquery("select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'")
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(len(r['data']['rows']), 0)

        # Remove the resource
        r = self.api.resources.remove('metadata_demo')


    def testModifyProperties(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        resource = 'properties_demo'

        # Create a new resource.
        r = self.api.resources.create(resource, 'replication', '', '', '')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        
        try:
            # The list of updates to apply in sequence.
            property_map = [
                ('name',        'test_modifying_resource_properties_renamed'),
                ('type',        'passthru'),
                ('host',        'example.org'),
                ('vault_path',  '/tmp/test_modifying_resource_properties_vault'),
                ('status',      'down'),
                ('status',      'up'),
                ('comments',    'test_modifying_resource_properties_comments'),
                ('information', 'test_modifying_resource_properties_information'),
                ('free_space',  'test_modifying_resource_properties_free_space'),
                ('context',     'test_modifying_resource_properties_context')
            ]

            # Apply each update to the resource and verify that each one results
            # in the expected results.
            for p, v in property_map:
                with self.subTest(f'Setting property [{p}] to value [{v}]'):
                    # Change a property of the resource.
                    r = self.api.resources.modify(resource, p, v)

                    # Make sure to update the "resource" variable following a successful rename.
                    if 'name' == p:
                        resource = v

                    # Show the property was modified.
                    r = self.api.resources.stat(resource)
                    self.assertEqual(r['data']['irods_response']['status_code'], 0)
                    self.assertEqual(r['data']['info'][p], v)
        finally:
            # Remove the resource
            r = self.api.resources.remove(resource)


# Tests for rule operations
class rulesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'rules'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')

    def testList(self):
        # Try listing rule engine plugins
        r = self.api.rules.list_rule_engines()

        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertGreater(len(r['data']['rule_engine_plugin_instances']), 0)
    

    def testExecuteRule(self):
        test_msg = 'This was run by the iRODS HTTP API test suite!'

        # Execute rule text against the iRODS rule language.
        r = self.api.rules.execute(f'writeLine("stdout", "{test_msg}")', 'irods_rule_engine_plugin-irods_rule_language-instance')
 
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(r['data']['stderr'], None)

        # The REP always appends a newline character to the result. While we could trim the result,
        # it is better to append a newline character to the expected result to guarantee things align.
        self.assertEqual(r['data']['stdout'], test_msg + '\n')


    def testRemoveDelayRule(self):
        rep_instance = 'irods_rule_engine_plugin-irods_rule_language-instance'

        # Schedule a delay rule to execute in the distant future.
        r = self.api.rules.execute(f'delay("<INST_NAME>{rep_instance}</INST_NAME><PLUSET>1h</PLUSET>") {{ writeLine("serverLog", "iRODS HTTP API"); }}', rep_instance)

        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Find the delay rule we just created.
        # This query assumes the test suite is running on a system where no other delay
        # rules are being created.
        r = self.api.queries.execute_genquery('select max(RULE_EXEC_ID)')

        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(len(r['data']['rows']), 1)

        print(r)

        # Remove the delay rule.
        r = self.api.rules.remove_delay_rule(int(r['data']['rows'][0][0]))
        print(r)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)


# Tests for tickets operations
class ticketsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'tickets'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')
    
    def testCreateAndRemove(self):
        self.api.setToken(self.rodsuser_bearer_token)

        # Create a write ticket.
        ticket_type = 'write'
        ticket_path = f'/{self.zone_name}/home/{self.rodsuser_username}'
        ticket_use_count = 2000
        ticket_groups = 'public'
        ticket_hosts = self.host
        r = self.api.tickets.create(ticket_path, ticket_type, use_count=ticket_use_count, seconds_until_expiration=3600, users='rods,jeb', groups=ticket_groups, hosts=ticket_hosts)
        print(r)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        ticket_string = r['data']['ticket']
        self.assertGreater(len(ticket_string), 0)

        # Show the ticket exists and has the properties we defined during creation.
        # We can use GenQuery for this, but it does seem better to provide a convenience operation
        # for this.
        r = self.api.queries.execute_genquery('select TICKET_STRING, TICKET_TYPE, TICKET_COLL_NAME, TICKET_USES_LIMIT, TICKET_ALLOWED_USER_NAME, TICKET_ALLOWED_GROUP_NAME, TICKET_ALLOWED_HOST')

        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertIn(ticket_string, r['data']['rows'][0])
        self.assertEqual(r['data']['rows'][0][1], ticket_type)
        self.assertEqual(r['data']['rows'][0][2], ticket_path)
        self.assertEqual(r['data']['rows'][0][3], str(ticket_use_count))
        self.assertIn(r['data']['rows'][0][4], ['rods', 'jeb'])
        self.assertEqual(r['data']['rows'][0][5], ticket_groups)
        self.assertGreater(len(r['data']['rows'][0][6]), 0)

        # Remove the ticket.
        r = self.api.tickets.remove(ticket_string)

        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show the ticket no longer exists.
        r = self.api.queries.execute_genquery('select TICKET_STRING')

        self.assertEqual(r['data']['irods_response']['status_code'], 0)
        self.assertEqual(len(r['data']['rows']), 0)


# Tests for user operations
class userTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_class(cls, {'endpoint_name': 'users-groups'})

    @classmethod
    def tearDownClass(cls):
        tear_down_class(cls)

    def setUp(self):
        self.assertFalse(self._class_init_error, 'Class initialization failed. Cannot continue.')
    
    def test_create_stat_and_remove_rodsuser(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        new_username = 'test_user_rodsuser'
        user_type = 'rodsuser'

        # Create a new user.
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)
 
        # Stat the user.
        r = self.api.users_groups.stat(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], True)
        self.assertIn('id', stat_info)
        self.assertEqual(stat_info['local_unique_name'], f'{new_username}#{self.zone_name}')
        self.assertEqual(stat_info['type'], user_type)

        # Remove the user.
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)
    

    def test_set_password(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        new_username = 'test_user_rodsuser'
        user_type = 'rodsuser'

        # Create a new user.
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)

        new_password = 'new_password'
        # Set a new password
        r = self.api.users_groups.set_password(new_username, self.zone_name, new_password)
        self.assertEqual(r['status_code'], 200)

        # Try to get a token for the user
        token = self.api.authenticate(new_username, new_password)
        self.assertIsInstance(token, str)

        # Remove the user.
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)


    def test_create_stat_and_remove_rodsadmin(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        new_username = 'test_user_rodsadmin'
        user_type = 'rodsadmin'
        headers = {'Authorization': 'Bearer ' + self.rodsadmin_bearer_token}

        # Create a new user.
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)
 
        # Stat the user.
        r = self.api.users_groups.stat(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], True)
        self.assertIn('id', stat_info)
        self.assertEqual(stat_info['local_unique_name'], f'{new_username}#{self.zone_name}')
        self.assertEqual(stat_info['type'], user_type)

        # Remove the user.
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)
    

    def test_create_stat_and_remove_groupadmin(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        new_username = 'test_user_groupadmin'
        user_type = 'groupadmin'

        # Create a new user.
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)
 
        # Stat the user.
        r = self.api.users_groups.stat(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], True)
        self.assertIn('id', stat_info)
        self.assertEqual(stat_info['local_unique_name'], f'{new_username}#{self.zone_name}')
        self.assertEqual(stat_info['type'], user_type)

        # Remove the user.
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)


    def test_add_remove_user_to_and_from_group(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a new group.
        new_group = 'test_group'
        r = self.api.users_groups.create_group(new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Stat the group.
        r = self.api.users_groups.stat(new_group)
        self.assertEqual(r['status_code'], 200)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], True)
        self.assertIn('id', stat_info)
        self.assertEqual(stat_info['type'], 'rodsgroup')

        # Create a new user.
        new_username = 'test_user_rodsuser'
        user_type = 'rodsuser'
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)

        # Add user to group.
        r = self.api.users_groups.add_to_group(new_username, self.zone_name, new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the user is a member of the group.
        r = self.api.users_groups.is_member_of_group(new_group, new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)
        result = r['data']
        self.assertEqual(result['irods_response']['status_code'], 0)
        self.assertEqual(result['is_member'], True)

        # Remove user from group.
        data = {'op': 'remove_from_group', 'group': new_group, 'user': new_username, 'zone': self.zone_name}
        r = self.api.users_groups.remove_from_group(new_username, self.zone_name, new_group)

        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Remove the user.
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)

        # Remove group.
        r = self.api.users_groups.remove_group(new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the group no longer exists.
        params = {'op': 'stat', 'name': new_group}
        r = self.api.users_groups.stat(new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], False)


    def test_only_a_rodsadmin_can_change_the_type_of_a_user(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a new user.
        new_username = 'test_user_rodsuser'
        user_type = 'rodsuser'
        r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that a rodsadmin can change the type of the new user.
        new_user_type = 'groupadmin'
        r = self.api.users_groups.set_user_type(new_username, self.zone_name, new_user_type)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that a non-admin cannot change the type of the new user.
        self.api.setToken(self.rodsuser_bearer_token)
        r = self.api.users_groups.set_user_type(new_user_type, self.zone_name, new_user_type)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], -13000)

        # Show that the user type matches the type set by the rodsadmin.
        params = {'op': 'stat', 'name': new_username, 'zone': self.zone_name}
        r = self.api.users_groups.stat(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)

        stat_info = r['data']
        self.assertEqual(stat_info['irods_response']['status_code'], 0)
        self.assertEqual(stat_info['exists'], True)
        self.assertEqual(stat_info['local_unique_name'], f'{new_username}#{self.zone_name}')
        self.assertEqual(stat_info['type'], new_user_type)

        # Remove the user.
        self.api.setToken(self.rodsadmin_bearer_token)
        r = self.api.users_groups.remove_user(new_username, self.zone_name)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)
    
    
    def test_listing_all_users_in_zone(self):
        self.api.setToken(self.rodsuser_bearer_token)

        r = self.api.users_groups.users()
        self.assertEqual(r['status_code'], 200)
        result = r['data']
        self.assertEqual(result['irods_response']['status_code'], 0)
        self.assertIn({'name': self.rodsadmin_username, 'zone': self.zone_name}, result['users'])
        self.assertIn({'name': self.rodsuser_username, 'zone': self.zone_name}, result['users'])
    

    def test_listing_all_groups_in_zone(self):
        self.api.setToken(self.rodsadmin_bearer_token)

        # Create a new group.
        new_group = 'test_group'
        r = self.api.users_groups.create_group(new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        self.api.setToken(self.rodsuser_bearer_token)
        # Get all groups.
        r = self.api.users_groups.groups()
        self.assertEqual(r['status_code'], 200)
        result = r['data']
        self.assertEqual(result['irods_response']['status_code'], 0)
        self.assertIn('public', result['groups'])
        self.assertIn(new_group, result['groups'])

        self.api.setToken(self.rodsadmin_bearer_token)
        # Remove the new group.
        r = self.api.users_groups.remove_group(new_group)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)


    def test_modifying_metadata_atomically(self):
        self.api.setToken(self.rodsadmin_bearer_token)
        username = self.rodsuser_username

        # Add metadata to the user.
        ops = [
                {
                    'operation': 'add',
                    'attribute': 'a1',
                    'value': 'v1',
                    'units': 'u1'
                }
            ]
        r = self.api.users_groups.modify_metadata(username, ops)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show the metadata exists on the user.
        r = self.api.queries.execute_genquery("select USER_NAME where META_USER_ATTR_NAME = 'a1' and META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'")
        self.assertEqual(r['status_code'], 200)

        result = r['data']
        self.assertEqual(result['irods_response']['status_code'], 0)
        self.assertEqual(result['rows'][0][0], username)

        # Remove the metadata from the user.
        ops = [
                {
                    'operation': 'remove',
                    'attribute': 'a1',
                    'value': 'v1',
                    'units': 'u1'
                }
            ]
        r = self.api.users_groups.modify_metadata(username, ops)
        self.assertEqual(r['status_code'], 200)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show the metadata no longer exists on the user.
        r = self.api.queries.execute_genquery("select USER_NAME where META_USER_ATTR_NAME = 'a1' and META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'")
        self.assertEqual(r['status_code'], 200)

        result = r['data']
        self.assertEqual(result['irods_response']['status_code'], 0)
        self.assertEqual(len(result['rows']), 0)

if __name__ == '__main__':
    unittest.main()