import irodsManager

#manager = irodsManager.collections_manager('rods', 'rods', 'http://localhost:9001/irods-http-api/', '0.3.0')
#print(manager.stat('/tempZone/home/rods'))

rods = irodsManager.manager('rods', 'rods', 'http://localhost:9001/irods-http-api/', '0.3.0')
jeb = irodsManager.manager('jebediah', 'dingleberry', 'http://localhost:9001/irods-http-api/', '0.3.0')

params = {
    'jebediah': 'null',
    'scott': 'read'
}

ops_permissions = [
    {
        'entity_name': 'jebediah',
        'acl': 'read'
    },
    {
        'entity_name': 'scott',
        'acl': 'read'
    }
]

ops_metadata = [
    {
        'operation': 'add',
        'attribute': 'eyeballs',
        'value': 'itchy'
    }
]

print(rods.collections.list('/tempZone/home/rods'))
#print(rods.collections.stat('/tempZone/home/rods'))

#rods.collections.modify_permissions('/tempZone/home/rods', ops_permissions)

#statDict = jeb.collections.stat('/tempZone/home/rods')

#print(statDict['permissions'])

#print(statDict.registered)
#print(rods.collections.touch('/tempZone/home/rods'))
#print(rods.collections.stat('/tempZone/home/rods'))
#print(rods.collections.touch('/tempZone/home/rods', 1000))
#print(rods.collections.stat('/tempZone/home/rods'))
#print(rods.collections.touch('/tempZone/home/rods', -1, '/tempZone/home/nyu'))
#print(rods.collections.stat('/tempZone/home/rods'))
