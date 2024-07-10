import unittest
import irodsManager

# Tests use the users rods (admin), jeb (rodsuser), and sdor (rodsuser)

# Tests for authentication operations
class authenticationTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(authenticationTests, self).__init__(*args, **kwargs)
        self.api = irodsManager.manager('http://localhost:9001/irods-http-api/0.3.0')

    def testAuth(self):
        #Test param checking
        self.assertRaises(Exception, self.api.authenticate, 0, 'api')
        self.assertRaises(Exception, self.api.authenticate, 'api', 0)
        self.assertRaises(Exception, self.api.authenticate, openid_token=2)

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
        self.api = irodsManager.manager('http://localhost:9001/irods-http-api/0.3.0')
        self.userToken1 = self.api.authenticate('rods', 'rods')
        self.userToken2 = self.api.authenticate('jeb', 'ding')
        self.userToken3 = self.api.authenticate('sdor', 'sdor')


    #tests the create operation
    def testCreate(self):
        self.api.setToken(self.userToken1)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')
        self.api.collections.remove('/tempZone/home/test/folder')
        self.api.collections.remove('/tempZone/home/test')

        #test param checking
        self.assertRaises(Exception, self.api.collections.create, 0, 0)
        self.assertRaises(Exception, self.api.collections.create, '/tempZone/home/rods', '0')
        self.assertRaises(Exception, self.api.collections.create, '/tempZone/home/rods', 7)

        #test creating new collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response))

        #test creating existing collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertEqual('{\'created\': False, \'irods_response\': {\'status_code\': 0}}', str(response))

        #test invalid path
        response = self.api.collections.create('tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response))

        #test create_intermediates
        response = self.api.collections.create('/tempZone/home/test/folder', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response))
        response = self.api.collections.create('/tempZone/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response))

        #test creating existing collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertEqual('{\'created\': False, \'irods_response\': {\'status_code\': 0}}', str(response))

        #test invalid path
        response = self.api.collections.create('tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response))

        #test create_intermediates
        response = self.api.collections.create('/tempZone/home/test/folder', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -358000, \'status_message\': \'path does not exist: OBJ_PATH_DOES_NOT_EXIST\'}}', str(response))
        response = self.api.collections.create('/tempZone/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response))
    

    #tests the remove operation
    def testRemove(self):
        self.api.setToken(self.userToken1)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')
        self.api.collections.remove('/tempZone/home/test/folder')
        self.api.collections.remove('/tempZone/home/test')

        #test param checking
        self.assertRaises(Exception, self.api.collections.remove, 0, 0, 0)
        self.assertRaises(Exception, self.api.collections.remove, '/tempZone/home/rods', '0', 0)
        self.assertRaises(Exception, self.api.collections.remove, '/tempZone/home/rods', 5, 0)
        self.assertRaises(Exception, self.api.collections.remove, '/tempZone/home/rods', 0, '0')
        self.assertRaises(Exception, self.api.collections.remove, '/tempZone/home/rods', 0, 5)

        #test removing collection
        response = self.api.collections.create('/tempZone/home/new')
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response))
        response = self.api.collections.remove('/tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test invalid paths
        response = self.api.collections.stat('/tempZone/home/tensaitekinaaidorusama')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('/tempZone/home/aremonainainaikoremonainainai')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('/tempZone/home/binglebangledingledangle')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))

        #test recurse
        response = self.api.collections.create('/tempZone/home/test/folder', 1)
        self.assertEqual('{\'created\': True, \'irods_response\': {\'status_code\': 0}}', str(response))
        response = self.api.collections.remove('/tempZone/home/test', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': -79000, \'status_message\': \'cannot remove non-empty collection: SYS_COLLECTION_NOT_EMPTY\'}}', str(response))
        response = self.api.collections.remove('/tempZone/home/test', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))


    #tests the stat operation
    def testStat(self):
        self.api.setToken(self.userToken1)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/new')

        #test param checking
        self.assertRaises(Exception, self.api.collections.stat, 0, 'ticket')
        self.assertRaises(Exception, self.api.collections.stat, '/tempZone/home/rods', 0)
        
        #test invalid paths
        response = self.api.collections.stat('/tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('tempZone/home/new')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))

        #test valid path
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['permissions'])
    

    #tests the list operation
    def testList(self):
        self.api.setToken(self.userToken1)

        #clean up test collections
        self.api.collections.remove('/tempZone/home/rods/croatia/zagreb')
        self.api.collections.remove('/tempZone/home/rods/albania')
        self.api.collections.remove('/tempZone/home/rods/bosnia')
        self.api.collections.remove('/tempZone/home/rods/croatia')

        #test param checking
        self.assertRaises(Exception, self.api.collections.stat, 0, 'ticket')
        self.assertRaises(Exception, self.api.collections.stat, '/tempZone/home/rods', '0', 'ticket')
        self.assertRaises(Exception, self.api.collections.stat, '/tempZone/home/rods', 5, 'ticket')
        self.assertRaises(Exception, self.api.collections.stat, '/tempZone/home/rods', 0, 0)

        #test empty collection
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('None', str(response['entries']))

        #test collection with one item
        self.api.collections.create('/tempZone/home/rods/bosnia')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['entries'][0]))

        #test collection with multiple items
        self.api.collections.create('/tempZone/home/rods/albania')
        self.api.collections.create('/tempZone/home/rods/croatia')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/albania', str(response['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['entries'][2]))

        #test without recursion
        self.api.collections.create('/tempZone/home/rods/croatia/zagreb')
        response = self.api.collections.list('/tempZone/home/rods')
        self.assertEqual('/tempZone/home/rods/albania', str(response['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['entries'][2]))
        self.assertEqual(len(response['entries']), 3)

        #test with recursion
        response = self.api.collections.list('/tempZone/home/rods', 1)
        self.assertEqual('/tempZone/home/rods/albania', str(response['entries'][0]))
        self.assertEqual('/tempZone/home/rods/bosnia', str(response['entries'][1]))
        self.assertEqual('/tempZone/home/rods/croatia', str(response['entries'][2]))
        self.assertEqual('/tempZone/home/rods/croatia/zagreb', str(response['entries'][3]))


    #tests the set permission operation
    def testSetPermission(self):
        self.api.setToken(self.userToken2)

        #test param checking
        self.assertRaises(Exception, self.api.collections.set_permission, 0, 'jeb', 'read', 0)
        self.assertRaises(Exception, self.api.collections.set_permission, '/tempZone/home/rods', 0, 'read', 0)
        self.assertRaises(Exception, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 0, 0)
        self.assertRaises(Exception, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 'read', '0')
        self.assertRaises(Exception, self.api.collections.set_permission, '/tempZone/home/rods', 'jeb', 'read', 5)

        #test no permission
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))

        #test set permission
        self.api.setToken(self.userToken1)
        response = self.api.collections.set_permission('/tempZone/home/rods', 'jeb', 'read')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test with permission
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['permissions'])
        
        #test set permission null
        self.api.setToken(self.userToken1)
        response = self.api.collections.set_permission('/tempZone/home/rods', 'jeb', 'null')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test no permission
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
    

    #tests the set inheritance operation
    def testSetInheritance(self):
        self.api.setToken(self.userToken1)

        #test param checking
        self.assertRaises(Exception, self.api.collections.set_inheritance, 0, 0, 0)
        self.assertRaises(Exception, self.api.collections.set_inheritance, '/tempZone/home/rods', '0', 0)
        self.assertRaises(Exception, self.api.collections.set_inheritance, '/tempZone/home/rods', 5, 0)
        self.assertRaises(Exception, self.api.collections.set_inheritance, '/tempZone/home/rods', 0, '0')
        self.assertRaises(Exception, self.api.collections.set_inheritance, '/tempZone/home/rods', 0, 5)

        #control
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertFalse(response['inheritance_enabled'])

        #test enabling inheritance
        response = self.api.collections.set_inheritance('/tempZone/home/rods', 1)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #check if changed
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['inheritance_enabled'])

        #test disabling inheritance
        response = self.api.collections.set_inheritance('/tempZone/home/rods', 0)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #check if changed
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertFalse(response['inheritance_enabled'])

    
    #test the modify permissions operation
    def testModifyPermissions(self):
        self.api.setToken(self.userToken2)

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
        self.assertRaises(Exception, self.api.collections.modify_permissions, 0, ops_permissions, 0)
        self.assertRaises(Exception, self.api.collections.modify_permissions, '/tempZone/home/rods', 5, 0)
        self.assertRaises(Exception, self.api.collections.modify_permissions, '/tempZone/home/rods', ops_permissions, '0')
        self.assertRaises(Exception, self.api.collections.modify_permissions, '/tempZone/home/rods', ops_permissions, 5)

        #test no permissions
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        self.api.setToken(self.userToken3)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))

        #test set permissions
        self.api.setToken(self.userToken1)
        response = self.api.collections.modify_permissions('/tempZone/home/rods', ops_permissions)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test with permissions
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['permissions'])
        self.api.setToken(self.userToken3)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['permissions'])

        #test set permissions nuil
        self.api.setToken(self.userToken1)
        response = self.api.collections.modify_permissions('/tempZone/home/rods', ops_permissions_null)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test without permissions
        self.api.setToken(self.userToken2)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        self.api.setToken(self.userToken3)
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))


    #test the modify metadata operation
    def testModifyMetadata(self):
        self.api.setToken(self.userToken1)
        
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
        self.assertRaises(Exception, self.api.collections.modify_metadata, 0, ops_metadata, 0)
        self.assertRaises(Exception, self.api.collections.modify_metadata, '/tempZone/home/rods', 5, 0)
        self.assertRaises(Exception, self.api.collections.modify_metadata, '/tempZone/home/rods', ops_metadata, '0')
        self.assertRaises(Exception, self.api.collections.modify_metadata, '/tempZone/home/rods', ops_metadata, 5)

        #test adding and removing metadata
        response = self.api.collections.modify_metadata('/tempZone/home/rods', ops_metadata)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))
        response = self.api.collections.modify_metadata('/tempZone/home/rods', ops_metadata_remove)
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))
    

    #tests the rename operation
    def testRename(self):
        self.api.setToken(self.userToken1)

        #test param checking
        self.assertRaises(Exception, self.api.collections.rename, '/tempZone/home/rods', 0)
        self.assertRaises(Exception, self.api.collections.rename, 0, '/tempZone/home/pods')
        
        #test before move
        response = self.api.collections.stat('/tempZone/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertTrue(response['permissions'])

        #test renaming
        response = self.api.collections.rename('/tempZone/home/rods', '/tempZone/home/pods')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))

        #test before move
        response = self.api.collections.stat('/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': -170000}}', str(response))
        response = self.api.collections.stat('/tempZone/home/pods')
        self.assertTrue(response['permissions'])

        #test renaming
        response = self.api.collections.rename('/tempZone/home/pods', '/tempZone/home/rods')
        self.assertEqual('{\'irods_response\': {\'status_code\': 0}}', str(response))
    

    #tests the touch operation
    def testTouch(self):
        self.api.setToken(self.userToken1)
        
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()