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