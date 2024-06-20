import irodsManager

#manager = irodsManager.collections_manager('rods', 'rods', 'http://localhost:9001/irods-http-api/', '0.3.0')
#print(manager.stat('/tempZone/home/rods'))

api = irodsManager.manager('http://localhost:9001/irods-http-api/0.3.0')
token = api.authenticate('rods', 'rods')
api.setToken(token)

# params = {
#     'jebediah': 'null',
#     'scott': 'read'
# }

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

print(api.collections.stat('/tempZone/home/rods'))

