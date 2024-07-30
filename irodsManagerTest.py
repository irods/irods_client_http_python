import unittest
from irods_client.irodsClient import IrodsClient
import concurrent.futures
import os
import time

# Tests use the users rods (admin), jeb (rodsuser), and sdor (rodsuser)

HOSTNAME = 'localhost'
ZONE_NAME = 'tempZone'

# Tests for authentication operations
class authenticationTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(authenticationTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')

    def testAuth(self):
        #Test param checking
        self.assertRaises(TypeError, self.api.authenticate, 0, 'api')
        self.assertRaises(TypeError, self.api.authenticate, 'api', 0)
        self.assertRaises(TypeError, self.api.authenticate, openid_token=2)

        #test openid optional parameter
        self.assertEqual('logged in with openid', self.api.authenticate(openid_token='hello'))

        token = ''

        #test authenticating and getToken()
        try:
            token = self.api.authenticate('rods', 'rods')
        except Exception:
            self.fail('Failed to authenticate')

        self.assertEqual(token, self.api.getToken())

# Tests for collections operations
class collectionsTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(collectionsTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')
        self.adminToken = self.api.authenticate('rods', 'rods')
        self.userToken1 = self.api.authenticate('jeb', 'ding')
        self.userToken2 = self.api.authenticate('sdor', 'sdor')


    #tests the create operation
    def testCreate(self):
        self.api.setToken(self.adminToken)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')
        self.api.collections.remove('/tempZone/home/test/folder')
        self.api.collections.remove('/tempZone/home/test')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.create, 0, 0)
        self.assertRaises(TypeError, self.api.collections.create, '/tempZone/home/rods', '0')
        self.assertRaises(ValueError, self.api.collections.create, '/tempZone/home/rods', 7)

        #test creating new collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertTrue(response['data']['created'])
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test creating existing collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertFalse(response['data']['created'])
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test invalid path
        response = self.api.collections.create('tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response['data']))

        #test create_intermediates
        response = self.api.collections.create('/tempZone/home/test/folder', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response['data']))
        response = self.api.collections.create('/tempZone/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
    

    #tests the remove operation
    def testRemove(self):
        self.api.setToken(self.adminToken)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')
        self.api.collections.remove('/tempZone/home/test/folder')
        self.api.collections.remove('/tempZone/home/test')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.remove, 0, 0, 0)
        self.assertRaises(TypeError, self.api.collections.remove, '/tempZone/home/rods', '0', 0)
        self.assertRaises(ValueError, self.api.collections.remove, '/tempZone/home/rods', 5, 0)
        self.assertRaises(TypeError, self.api.collections.remove, '/tempZone/home/rods', 0, '0')
        self.assertRaises(ValueError, self.api.collections.remove, '/tempZone/home/rods', 0, 5)

        #test removing collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
        response = self.api.collections.remove('/tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))
        #test invalid paths
        response = self.api.collections.stat('/tempZone/home/tensaitekinaaidorusama')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('/tempZone/home/aremonainainaikoremonainainai')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('/tempZone/home/binglebangledingledangle')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test recurse
        response = self.api.collections.create('/tempZone/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response['data']))
        response = self.api.collections.remove('/tempZone/home/test', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -79000, \'status_message\': \'cannot remove non-empty collection: SYS_COLLECTION_NOT_EMPTY\'}}', str(response['data']))
        response = self.api.collections.remove('/tempZone/home/test', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))


    #tests the stat operation
    def testStat(self):
        self.api.setToken(self.adminToken)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.stat, 0, 'ticket')
        self.assertRaises(TypeError, self.api.collections.stat, '/tempZone/home/rods', 0)
        
        #test invalid paths
        response = self.api.collections.stat('/tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test valid path
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['data']['permissions'])
    

    #tests the list operation
    def testList(self):
        self.api.setToken(self.adminToken)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/rods/croatia/zagreb')
        self.api.collections.remove('/tempZone/home/rods/albania')
        self.api.collections.remove('/tempZone/home/rods/bosnia')
        self.api.collections.remove('/tempZone/home/rods/croatia')

        #test param checking
        self.assertRaises(TypeError, self.api.collections.list, 0, 'ticket')
        self.assertRaises(TypeError, self.api.collections.list, '/tempZone/home/rods', '0', 'ticket')
        self.assertRaises(ValueError, self.api.collections.list, '/tempZone/home/rods', 5, 'ticket')
        self.assertRaises(TypeError, self.api.collections.list, '/tempZone/home/rods', 0, 0)

        #test empty collection
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('None', str(response['data']['entries']))

        #test collection with one item
        self.api.collections.create('/tempZone/home/rods/bosnia')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['data']['entries'][0]))

        #test collection with multiple items
        self.api.collections.create('/tempZone/home/rods/albania')
        self.api.collections.create('/tempZone/home/rods/croatia')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/albania', str(response['data']['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['data']['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['data']['entries'][2]))

        #test without recursion
        self.api.collections.create('/tempZone/home/rods/croatia/zagreb')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/albania', str(response['data']['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['data']['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['data']['entries'][2]))
        self.assertEqual(len(response['data']['entries']), 3)

        #test with recursion
        response = self.api.collections.list('/tempZone/home/rods', 1)
        self.assertEqual('/tempZone/home/rods/albania', str(response['data']['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['data']['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['data']['entries'][2]))
        self.assertEqual('/tempZone/home/rods/croatia/zagreb', str(response['data']['entries'][3]))


    #tests the set permission operation
    def testSetPermission(self):
        self.api.setToken(self.adminToken)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.set_permission, 0, 'jeb', 'read', 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, '/tempZone/home/rods', 0, 'read', 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 0, 0)
        self.assertRaises(TypeError, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 'read', '0')
        self.assertRaises(ValueError, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 'read', 5)
        
        #create new collection
        response = self.api.collections.create('/tempZone/home/setPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test no permission
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/setPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], -170000)

        #test set permission
        self.api.setToken(self.adminToken)
        response = self.api.collections.set_permission('/tempZone/home/setPerms', 'jeb', 'read')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test with permission
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/setPerms')
        self.assertTrue(response['data']['permissions'])
        
        #test set permission null
        self.api.setToken(self.adminToken)
        response = self.api.collections.set_permission('/tempZone/home/setPerms', 'jeb', 'null')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test no permission
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/setPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #remove the collection
        self.api.setToken(self.adminToken)
        response = self.api.collections.remove('/tempZone/home/setPerms', 1, 1)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
    

    #tests the set inheritance operation
    def testSetInheritance(self):
        self.api.setToken(self.adminToken)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.set_inheritance, 0, 0, 0)
        self.assertRaises(TypeError, self.api.collections.set_inheritance, '/tempZone/home/rods', '0', 0)
        self.assertRaises(ValueError, self.api.collections.set_inheritance, '/tempZone/home/rods', 5, 0)
        self.assertRaises(TypeError, self.api.collections.set_inheritance, '/tempZone/home/rods', 0, '0')
        self.assertRaises(ValueError, self.api.collections.set_inheritance, '/tempZone/home/rods', 0, 5)

        #control
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertFalse(response['data']['inheritance_enabled'])

        #test enabling inheritance
        response = self.api.collections.set_inheritance('/tempZone/home/rods', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #check if changed
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['data']['inheritance_enabled'])

        #test disabling inheritance
        response = self.api.collections.set_inheritance('/tempZone/home/rods', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #check if changed
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertFalse(response['data']['inheritance_enabled'])

    
    #test the modify permissions operation
    def testModifyPermissions(self):
        self.api.setToken(self.adminToken)

        ops_permissions = [
            {
                'entity_name': 'jeb',
                'acl': 'read'
            },
            {
                'entity_name': 'sdor',
                'acl': 'read'
            }
        ]

        ops_permissions_null = [
            {
                'entity_name': 'jeb',
                'acl': 'null'
            },
            {
                'entity_name': 'sdor',
                'acl': 'null'
            }
        ]

        #test param checking
        self.assertRaises(TypeError, self.api.collections.modify_permissions, 0, ops_permissions, 0)
        self.assertRaises(TypeError, self.api.collections.modify_permissions, '/tempZone/home/rods', 5, 0)
        self.assertRaises(TypeError, self.api.collections.modify_permissions, '/tempZone/home/rods', ops_permissions, '0')
        self.assertRaises(ValueError, self.api.collections.modify_permissions, '/tempZone/home/rods', ops_permissions, 5)

        #create new collection
        response = self.api.collections.create('/tempZone/home/modPerms')
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test no permissions
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #test set permissions
        self.api.setToken(self.adminToken)
        response = self.api.collections.modify_permissions('/tempZone/home/modPerms', ops_permissions)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test with permissions
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertTrue(response['data']['permissions'])
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertTrue(response['data']['permissions'])

        #test set permissions nuil
        self.api.setToken(self.adminToken)
        response = self.api.collections.modify_permissions('/tempZone/home/modPerms', ops_permissions_null)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)

        #test without permissions
        self.api.setToken(self.userToken1)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/modPerms')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))

        #remove the collection
        self.api.setToken(self.adminToken)
        response = self.api.collections.remove('/tempZone/home/modPerms', 1, 1)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)


    #test the modify metadata operation
    def testModifyMetadata(self):
        self.api.setToken(self.adminToken)
        
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
        self.assertRaises(TypeError, self.api.collections.modify_metadata, '/tempZone/home/rods', 5, 0)
        self.assertRaises(TypeError, self.api.collections.modify_metadata, '/tempZone/home/rods', ops_metadata, '0')
        self.assertRaises(ValueError, self.api.collections.modify_metadata, '/tempZone/home/rods', ops_metadata, 5)

        #test adding and removing metadata
        response = self.api.collections.modify_metadata('/tempZone/home/rods', ops_metadata)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
        response = self.api.collections.modify_metadata('/tempZone/home/rods', ops_metadata_remove)
        self.assertEqual(response['data']['irods_response']['status_code'], 0)
    

    #tests the rename operation
    def testRename(self):
        self.api.setToken(self.adminToken)

        #test param checking
        self.assertRaises(TypeError, self.api.collections.rename, '/tempZone/home/rods', 0)
        self.assertRaises(TypeError, self.api.collections.rename, 0, '/tempZone/home/pods')
        
        #test before move
        response = self.api.collections.stat('/tempZone/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['data']['permissions'])

        #test renaming
        response = self.api.collections.rename('/tempZone/home/rods', '/tempZone/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))

        #test before move
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response['data']))
        response = self.api.collections.stat('/tempZone/home/pods')
        self.assertTrue(response['data']['permissions'])

        #test renaming
        response = self.api.collections.rename('/tempZone/home/pods', '/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response['data']))
    

    #tests the touch operation
    def testTouch(self):
        self.api.setToken(self.adminToken)
        
        self.assertTrue(True)



# Tests for data object operations
class dataObjectsTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(dataObjectsTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')
        self.adminToken = self.api.authenticate('rods', 'rods')
        self.userToken1 = self.api.authenticate('jeb', 'ding')
        self.userToken2 = self.api.authenticate('sdor', 'sdor')
    
    
    def testCommonOperations(self):
        self.api.setToken(self.adminToken)
        print(self.adminToken)

        try:
            # Create a unixfilesystem resource.
            r = self.api.resources.create('resource', 'unixfilesystem', HOSTNAME, '/tmp/resource', '')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            self.api.setToken(self.userToken1)
            # Create a non-empty data object
            print(self.api.collections.stat('/tempZone/home/jeb'))
            r = self.api.data_objects.write('These are the bytes being written to the object', '/tempZone/home/jeb/file.txt', 'resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Replicate the data object
            r = self.api.data_objects.replicate('/tempZone/home/jeb/file.txt', dst_resource='resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Show that there are two replicas
            # TODO: Implement once query operations are completed

            # Trim the first data object
            r = self.api.data_objects.trim('/tempZone/home/jeb/file.txt', 0)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Rename the data object
            r = self.api.data_objects.rename('/tempZone/home/jeb/file.txt', '/tempZone/home/jeb/newfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Copy the data object
            r = self.api.data_objects.copy('/tempZone/home/jeb/newfile.txt', '/tempZone/home/jeb/anotherfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Set permission on the object
            r = self.api.data_objects.set_permission('/tempZone/home/jeb/anotherfile.txt', 'rods', 'read')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Confirm that the permission has been set
            r = self.api.data_objects.stat('/tempZone/home/jeb/anotherfile.txt')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
            self.assertIn({
                'name': 'rods',
                'zone': ZONE_NAME,
                'type': 'rodsadmin',
                'perm': 'read_object'
            }, r['data']['permissions'])

        finally:
            # Remove the data objects
            r = self.api.data_objects.remove('/tempZone/home/jeb/anotherfile.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            r = self.api.data_objects.remove('/tempZone/home/jeb/newfile.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            self.api.setToken(self.adminToken)

            # Remove the resource
            r = self.api.resources.remove('resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)


    def testChecksums(self):
        self.api.setToken(self.adminToken)

        # Create a unixfilesystem resource.
        r = self.api.resources.create('newresource', 'unixfilesystem', HOSTNAME, '/tmp/newresource', '')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Create a non-empty data object
        r = self.api.data_objects.write('These are the bytes being written to the object', '/tempZone/home/rods/file.txt', 'newresource')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Replicate the data object
        r = self.api.data_objects.replicate('/tempZone/home/rods/file.txt', dst_resource='newresource')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that there are two replicas
        # TODO: Implement once query operations are completed

        try:
            # Calculate a checksum for the first replica
            r = self.api.data_objects.calculate_checksum('/tempZone/home/rods/file.txt', replica_number=0)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Verify checksum information across all replicas.
            r = self.api.data_objects.verify_checksum('/tempZone/home/rods/file.txt')
            print(r)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
        finally:
            # Remove the data objects
            r = self.api.data_objects.remove('/tempZone/home/rods/file.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Remove the resource
            r = self.api.resources.remove('newresource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)
        
    
    def testTouch(self):
        self.api.setToken(self.adminToken)

        # Test touching non existant data object with no_create
        r = self.api.data_objects.touch('/tempZone/home/rods/new.txt', 1)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the object has not been created
        r = self.api.data_objects.stat('/tempZone/home/rods/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], -171000)

        # Test touching non existant object without no_create
        r = self.api.data_objects.touch('/tempZone/home/rods/new.txt', 0)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Show that the object has been created
        r = self.api.data_objects.stat('/tempZone/home/rods/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Test touching existing object without no_create
        r = self.api.data_objects.touch('/tempZone/home/rods/new.txt', 1)
        self.assertEqual(r['data']['irods_response']['status_code'], 0)

        # Remove the object
        r = self.api.data_objects.remove('/tempZone/home/rods/new.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], 0)


    def testRegister(self):
        self.api.setToken(self.adminToken)

        # Create a non-empty local file.
        content = 'data'
        with open('/tmp/register-demo.txt', 'w') as f:
                f.write(content)
        
        # Show the data object we want to create via registration does not exist.
        r = self.api.data_objects.stat('/tempZone/home/rods/register-demo.txt')
        self.assertEqual(r['data']['irods_response']['status_code'], -171000)

        try:
            # Create a unixfilesystem resource.
            r = self.api.resources.create('register_resource', 'unixfilesystem', HOSTNAME, '/tmp/register_resource', '')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Register the local file into the catalog as a new data object.
            # We know we're registering a new data object because the "as-additional-replica"
            # parameter isn't set to 1.
            r = self.api.data_objects.register('/tempZone/home/rods/register-demo.txt', '/tmp/register-demo.txt', 'register_resource', data_size=len(content))
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Show a new data object exists with the expected replica information.
            # TODO: add when query operations are implemented
        finally:
            # Unregisterr the dataq object
            r = self.api.data_objects.remove('/tempZone/home/rods/register-demo.txt', 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

            # Remove the resource
            r = self.api.resources.remove('register_resource')
            self.assertEqual(r['data']['irods_response']['status_code'], 0)

    
    def testParallelWrite(self):
        self.api.setToken(self.adminToken)
        self.api.data_objects.remove('/tempZone/home/rods/parallel-write.txt', 0, 1)

        # Open parallel write
        r = self.api.data_objects.parallel_write_init('/tempZone/home/rods/parallel-write.txt', 3)
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
            r = self.api.data_objects.remove('/tempZone/home/rods/parallel-write.txt', 0, 1)
            self.assertEqual(r['data']['irods_response']['status_code'], 0)




# Tests for resources operations
class resourcesTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(resourcesTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')
        self.adminToken = self.api.authenticate('rods', 'rods')
        self.userToken1 = self.api.authenticate('jeb', 'ding')


    def testCommonOperations(self):
        self.api.setToken(self.adminToken)

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
                r = self.api.resources.create(resc_name, 'unixfilesystem', HOSTNAME, vault_path, '')
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
                self.assertEqual(r['data']['info']['zone'], ZONE_NAME)
                self.assertEqual(r['data']['info']['host'], HOSTNAME)
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
        data_object = f'/{ZONE_NAME}/home/rods/resource_obj'
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
        self.api.setToken(self.adminToken)

        # Create a unixfilesystem resource.
        r = self.api.resources.create('metadata_demo', 'unixfilesystem', HOSTNAME, '/tmp/metadata_demo_vault', '')
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
        self.api.setToken(self.adminToken)

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
    def __init__(self, *args, **kwargs):
        super(rulesTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')
        self.adminToken = self.api.authenticate('rods', 'rods')

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
    def __init__(self, *args, **kwargs):
        super(ticketsTests, self).__init__(*args, **kwargs)
        self.api = IrodsClient(f'http://{HOSTNAME}:9001/irods-http-api/0.3.0')
        self.adminToken = self.api.authenticate('rods', 'rods')
        self.userToken = self.api.authenticate('jeb', 'ding')

    
    def testCreateAndRemove(self):
        self.api.setToken(self.userToken)

        # Create a write ticket.
        ticket_type = 'write'
        ticket_path = f'/{ZONE_NAME}/home/jeb'
        ticket_use_count = 2000
        ticket_groups = 'public'
        ticket_hosts = HOSTNAME
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

if __name__ == '__main__':
    unittest.main()