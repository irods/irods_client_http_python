import wrapperTest

wrapperTest.initialize('http://localhost:9001/irods-http-api/', '0.3.0')

token = wrapperTest.authenticate('rods', 'rods')

print(token)

response = wrapperTest.collections_stat(token, '/tempZone/home/rods')

print(response)

response = wrapperTest.collections_rename('', '/tempZone/home/rods', '/tempZone/home/pods')

print(response)

response = wrapperTest.collections_stat(token, '/tempZone/home/rods')
print(response)
response = wrapperTest.collections_stat(token, '/tempZone/home/pods')
print(response)


response = wrapperTest.collections_rename('', '/tempZone/home/pods', '/tempZone/home/rods')

print(response)

response = wrapperTest.collections_stat(token, '/tempZone/home/pods')
print(response)
response = wrapperTest.collections_stat(token, '/tempZone/home/rods')
print(response)