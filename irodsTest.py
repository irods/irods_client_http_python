import irodsManager

#manager = irodsManager.collections_manager('rods', 'rods', 'http://localhost:9001/irods-http-api/', '0.3.0')
#print(manager.stat('/tempZone/home/rods'))

m = irodsManager.manager('rods', 'rods', 'http://localhost:9001/irods-http-api/', '0.3.0')
print(m.collections.stat('/tempZone/home/rods'))
#print(m.collections.remove('/tempZone/home/new', 0, 0))
#print(m.collections.stat('/tempZone/home/new'))
print(m.collections.create('/tempZone/home/new', 0))