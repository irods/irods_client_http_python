import requests
import json

class DataObjects:
    # Initializes data_objects_manager with variables from the parent class.
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.token = None

    # Updates mtime for an existing data object or creates a new one
        # params
        # - lpath: The absolute logical path of the collection being touched.
        # - no_create (optional): Set to 1 to prevent creating a new object, otherwise set to 0.
        # - replica_number (optional): The replica number of the target replica.
        # - leaf_resources (optional): The resource holding an existing replica. If one does not exist, creates one.
        # - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
        # - reference (optional): The absolute logical path of the collection to use as a reference for mtime.
        # returns
        # - Status code and response message.
    def touch(self, lpath, no_create: int=0, replica_number: int=-1, leaf_resources: str='', seconds_since_epoch=-1, reference=''):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if ((not no_create == 0) and (not no_create == 1)):
            raise Exception('no_create must be an int 1 or 0')
        if (not isinstance(replica_number, int)):
            raise Exception('replica_number must be an int')
        if (not isinstance(leaf_resources, str)):
            raise Exception('leaf_resources must be a string')
        if (not isinstance(seconds_since_epoch, int)):
            raise Exception('seconds_since_epoch must be an int')
        if (not isinstance(reference, str)):
            raise Exception('reference must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'touch',
            'lpath': lpath,
            'no-create': no_create
        }

        if (seconds_since_epoch != -1):
            data['seconds-since-epoch'] = seconds_since_epoch

        if (replica_number != -1):
            data['replica-number'] = replica_number
        
        if (leaf_resources != ''):
            data['leaf-resources'] = leaf_resources
        
        if (reference != ''):
            data['reference'] = reference

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to touch data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Data object \'' + lpath + '\' touched successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Removes an existing data object.
    # params
    # - lpath: The absolute logical path of the data object to be removed.
    # - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
    # - no_trash (optional): Set to 1 to move the data object to trash, 0 to permanently remove. Defaults to 0.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def remove(self, lpath: str, catalog_only: int=0, no_trash: int=0, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if ((not catalog_only == 0) and (not catalog_only == 1)):
            raise Exception('recurse must be an int 1 or 0')
        if ((not no_trash == 0) and (not no_trash == 1)):
            raise Exception('no_trash must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'remove',
            'lpath': lpath,
            'catalog-only': catalog_only,
            'no-trash': no_trash,
            'admin': admin
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to remove data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Data object \'' + lpath + '\' removed successfully')
            
            return(rdict)
        elif (r.status_code / 100 == 4):
            rdict = r.json()
            print('Failed to remove data object \'' + lpath + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']))

            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
    

    # Calculates the checksum for a data object.
    # params
    # - lpath: The absolute logical path of the data object to be removed.
    # - resource (optional): The resource holding the existing replica.
    # - replica_number (optional): The replica number of the target replica.
    # - force (optional): Set to 1 to replace the existing checksum, otherwise set to 0. Defaults to 0.
    # - all (optional): Set to 1 to calculate the checksum for all replicas, otherwise set to 0. Defaults to 0.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def calculate_checksum(self, lpath: str, resource: str='', replica_number: int=-1, force: int=0, all: int=0, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(resource, str)):
            raise Exception('resource must be a string')
        if (not isinstance(replica_number, int)):
            raise Exception('replica_number must be an int')
        if ((not force == 0) and (not force == 1)):
            raise Exception('force must be an int 1 or 0')
        if ((not all == 0) and (not all == 1)):
            raise Exception('all must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'calculate_checksum',
            'lpath': lpath,
            'force': force,
            'all': all,
            'admin': admin
        }

        if (resource != ''):
            data['resource'] = resource

        if (replica_number != -1):
            data['replica-number'] = replica_number
        

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to calculate checksum for data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Checksum for data object \'' + lpath + '\' calculated successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Verifies the checksum for a data object.
    # params
    # - lpath: The absolute logical path of the data object to be removed.
    # - resource (optional): The resource holding the existing replica.
    # - replica_number (optional): The replica number of the target replica.
    # - compute_checksums (optional): Set to 1 to skip checksum calculation, otherwise set to 0. Defaults to 0.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def verify_checksum(self, lpath: str, resource: str='', replica_number: int=-1, compute_checksums: int=0, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(resource, str)):
            raise Exception('resource must be a string')
        if (not isinstance(replica_number, int)):
            raise Exception('replica_number must be an int')
        if ((not compute_checksums == 0) and (not compute_checksums == 1)):
            raise Exception('force must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'calculate_checksum',
            'lpath': lpath,
            'compute-checksums': compute_checksums,
            'admin': admin
        }

        if (resource != ''):
            data['resource'] = resource

        if (replica_number != -1):
            data['replica-number'] = replica_number
        

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to verify checksum for data object \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Checksum for data object \'' + lpath + '\' verified successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Gives information about a data object.
    # params
    # - lpath: The absolute logical path of the data object being queried.
    # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
    # return
    # - Status code 2XX: Dictionary containing data object information.
    # - Other: Status code and return message.
    def stat(self, lpath: str, ticket: str=''):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(ticket, str)):
            raise Exception('ticket must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
        }

        params = {
            'op': 'stat',
            'lpath': lpath,
            'ticket': ticket
        }

        r = requests.get(self.url_base + '/data-objects', params=params, headers=headers)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to retrieve information for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Information for \'' + lpath + '\' retrieved successfully')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Renames or moves a data object.
    # params
    # - old_lpath: The current absolute logical path of the data object.
    # - new_lpath: The absolute logical path of the destination for the data object.
    # returns
    # - Status code and response message.
    def rename(self, old_lpath: str, new_lpath: str):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(old_lpath, str)):
            raise Exception('old_lpath must be a string')
        if (not isinstance(new_lpath, str)):
            raise Exception('new_lpath must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'rename',
            'old-lpath': old_lpath,
            'new-lpath': new_lpath
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to rename \'' + old_lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + old_lpath + '\' renamed to \'' + new_lpath + '\'')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Copies a data object.
    # params
    # - src_lpath: The absolute logical path of the source data object.
    # - dst_lpath: The absolute logical path of the destination.
    # - src_resource: The absolute logical path of the source resource.
    # - dst_resource: The absolute logical path of the destination resource.
    # - overwrite: set to 1 to overwrite an existing objject, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def copy(self, src_lpath: str, dst_lpath: str, src_resource: str='', dst_resource: str='', overwrite: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(src_lpath, str)):
            raise Exception('src_lpath must be a string')
        if (not isinstance(dst_lpath, str)):
            raise Exception('dst_lpath must be a string')
        if (not isinstance(src_resource, str)):
            raise Exception('src_resource must be a string')
        if (not isinstance(dst_resource, str)):
            raise Exception('dst_lpath must be a string')
        if ((not overwrite == 0) and (not overwrite == 1)):
            raise Exception('overwrite must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'copy',
            'src-lpath': src_lpath,
            'dst-lpath': dst_lpath,
            'overwrite': overwrite
        }

        if (src_resource != ''):
            data['src-resource'] = src_resource

        if (dst_resource != ''):
            data['dst-resource'] = dst_resource

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to copy \'' + src_lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + src_lpath + '\' copied to \'' + dst_lpath + '\'')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Replicates a data object from one resource to another.
    # params
    # - lpath: The  absolute logical path of the data object to be replicated.
    # - src_resource: The absolute logical path of the source resource.
    # - dst_resource: The absolute logical path of the destination resource.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def replicate(self, lpath: str, src_resource: str='', dst_resource: str='', admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(src_resource, str)):
            raise Exception('src_resource must be a string')
        if (not isinstance(dst_resource, str)):
            raise Exception('dst_lpath must be a string')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'replicate',
            'lpath': lpath,
            'admin': admin
        }

        if (src_resource != ''):
            data['src-resource'] = src_resource

        if (dst_resource != ''):
            data['dst-resource'] = dst_resource

        print(data)

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()
            if rdict['irods_response']['status_code']:
                print('Failed to replicate \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + lpath + '\' replicated from \'' + src_resource + '\' to \'' + dst_resource + '\'')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        
    # Trims an existing replica or removes its catalog entry.
    # params
    # - lpath: The  absolute logical path of the data object to be replicated.
    # - replica_number: The replica number of the target replica.
    # - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def trim(self, lpath: str, replica_number: int, catalog_only: int=0, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(replica_number, int)):
            raise Exception('replica_number must be an int')
        if ((not catalog_only == 0) and (not catalog_only == 1)):
            raise Exception('catalog_only must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'trim',
            'lpath': lpath,
            'replica-number': replica_number,
            'catalog-only': catalog_only,
            'admin': admin
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to trim \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully trimmed \'' + lpath + '\'')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Registers a data object/replica into the catalog.
    # params
    # - lpath: The  absolute logical path of the data object to be registered.
    # - ppath: The  absolute physical path of the data object to be registered.
    # - resource: The resource that will own the replica.
    # - as_additional_replica (optional): Set to 1 to register as a replica of an existing object, otherwise set to 0. Defaults to 0.
    # - data_size (optional): The size of the replica in bytes.
    # - checksum (optional): The checksum to associate with the replica.
    # returns
    # - Status code and response message.
    def register(self, lpath: str, ppath: str, resource: str, as_additional_replica: int=0, data_size: int=-1, checksum: str=''):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(ppath, str)):
            raise Exception('ppath must be a string')
        if (not isinstance(resource, str)):
            raise Exception('resource must be a string')
        if ((not as_additional_replica == 0) and (not as_additional_replica == 1)):
            raise Exception('as_additional_replica must be an int 1 or 0')
        if (not isinstance(data_size, int)):
            raise Exception('data_size must be an int')
        if (not isinstance(checksum, str)):
            raise Exception('checksum must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'register',
            'lpath': lpath,
            'ppath': ppath,
            'resource': resource,
            'as_additional_replica': as_additional_replica
        }

        if (data_size != -1):
            data['data-size'] = data_size

        if (checksum != ''):
            data['checksum'] = checksum

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to register \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully registered \'' + lpath + '\'')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Reads bytes from a data object.
    # params
    # - lpath: The absolute logical path of the data object to be read from.
    # - offset (optional): The number of bytes to skip. Defaults to 0.
    # - count (optional): The number of bytes to read.
    # - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.
    # returns
    # - Status code and response message.
    def read(self, lpath: str, offset: int=0, count: int=-1, ticket: str=''):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(offset, int)):
            raise Exception('offset must be an int')
        if (not isinstance(count, int)):
            raise Exception('count must be an int')
        if (not isinstance(ticket, str)):
            raise Exception('ticket must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        params = {
            'op': 'read',
            'lpath': lpath,
            'offset': offset
        }

        if (count != -1):
            params['count'] = count

        if (ticket != ''):
            params['ticket'] = ticket

        r = requests.get(self.url_base + '/data-objects',params=params , headers=headers)

        if (r.status_code / 100 == 2):
            print('Sucessfully read \'' + lpath + '\'')
            return(r.text)
        else:
            print('Error: ' + r.text)

            return(r)
    

    # Writes bytes to a data object.
    # params
    # - lpath: The absolute logical path of the data object to be read from.
    # - bytes: The bytes to be written.
    # - resource (optional): The root resource to write to.
    # - offset (optional): The number of bytes to skip. Defaults to 0.
    # - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
    # - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
    # - parallel_write_handle (optional): The handle to be used when writing in parallel.
    # - stream_index (optional): The stream to use when writing in parallel.
    # returns
    # - Status code and response message.
    def write(self, bytes, lpath: str='', resource: str='', offset: int=0, truncate: int=1, append: int=0, parallel_write_handle: str='', stream_index: int=-1):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(resource, str)):
            raise Exception('resource must be a string')
        if (not isinstance(offset, int)):
            raise Exception('offset must be an int')
        if ((not truncate == 0) and (not truncate == 1)):
            raise Exception('truncate must be an int 1 or 0')
        if ((not append == 0) and (not append == 1)):
            raise Exception('append must be an int 1 or 0')
        if (not isinstance(parallel_write_handle, str)):
            raise Exception('parallel_write_handle must be a string')
        if (not isinstance(stream_index, int)):
            raise Exception('stream_index must be an int')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'write',
            'offset': offset,
            'truncate': truncate,
            'append': append,
            'bytes': bytes
        }

        if (parallel_write_handle != ''):
            data['parallel-write-handle'] = parallel_write_handle
        else:
            data['lpath'] = lpath

        if (resource != ''):
            data['resource'] = resource

        if (stream_index != -1):
            data['stream-index'] = stream_index

        #print(data)

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to write to \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully wrote to \'' + lpath + '\'')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Initializes server-side state for parallel writing.
    # params
    # - lpath: The absolute logical path of the data object to be read from.
    # - stream_count: THe number of streams to open.
    # - resource (optional): The root resource to write to.
    # - offset (optional): The number of bytes to skip. Defaults to 0.s
    # - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
    # - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
    # - ticket (optional):  Ticket to be enabled before the operation. Defaults to an empty string.
    # returns
    # - Status code and response message.
    def parallel_write_init(self, lpath: str, stream_count: int, truncate: int=1, append: int=0, ticket: str=''):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(stream_count, int)):
            raise Exception('stream_count must be an int')
        if ((not truncate == 0) and (not truncate == 1)):
            raise Exception('truncate must be an int 1 or 0')
        if ((not append == 0) and (not append == 1)):
            raise Exception('append must be an int 1 or 0')
        if (not isinstance(ticket, str)):
            raise Exception('ticket must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'parallel_write_init',
            'lpath': lpath,
            'stream-count': stream_count,
            'truncate': truncate,
            'append': append
        }

        if (ticket != ''):
            data['ticket'] = ticket

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to open parallel write to \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully opened parallel write to \'' + lpath + '\'')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        

    # Initializes server-side state for parallel writing.
    # params
    # - parallel_write_handle: Handle obtained from parallel_write_init
    # returns
    # - Status code and response message.
    def parallel_write_shutdown(self, parallel_write_handle: str):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(parallel_write_handle, str)):
            raise Exception('parallel_write_handle must be a string')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'parallel_write_shutdown',
            'parallel-write-handle': parallel_write_handle
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to close parallel write: iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully closed parallel write.')
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(r)
        
    
    # Modifies the metadata for a data object
    # params
    # - lpath: The absolute logical path of the collection to have its inheritance set.
    # - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def modify_metadata(self, lpath: str, operations: list, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(operations, list)):
            raise Exception('operations must be a list of dictionaries')
        if (not isinstance(operations[0], dict)):
            raise Exception('operations must be a list of dictionaries')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')

        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'modify_metadata',
            'lpath': lpath,
            'operations': json.dumps(operations),
            'admin': admin
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify metadata for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Metadata for \'' + lpath + '\' modified successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(rdict)
        

    # Sets the permission of a user for a given collection
    # params
    # - lpath: The absolute logical path of the collection to have a permission set.
    # - entity_name: The name of the user or group having its permission set.
    # - permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def set_permission(self, lpath: str, entity_name: str, permission: str, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(entity_name, str)):
            raise Exception('entity_name must be a string')
        if (not isinstance(permission, str)):
            raise Exception('permission must be a string (\'null\', \'read\', \'write\', or \'own\')')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'set_permission',
            'lpath': lpath,
            'entity-name': entity_name,
            'permission': permission,
            'admin': admin
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to set permission for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Permission for \'' + lpath + '\' set successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(rdict)
    
    
    # Modifies permissions for multiple users or groups for a data object.
    # params
    # - lpath: The absolute logical path of the data object to have its permissions modified.
    # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def modify_permissions(self, lpath: str, operations: list, admin: int=0):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(operations, list)):
            raise Exception('operations must be a list of dictionaries')
        if (not isinstance(operations[0], dict)):
            raise Exception('operations must be a list of dictionaries')
        if ((not admin == 0) and (not admin == 1)):
            raise Exception('admin must be an int 1 or 0')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'modify_permissions',
            'lpath': lpath,
            'operations': json.dumps(operations),
            'admin': admin
        }

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify permissions for \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Permissions for \'' + lpath + '\' modified successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(rdict)
        
    
    # Modifies properties of a single replica
    # WARNING: This operation requires rodsadmin level privileges and should only be used when there isn't a safer option.
    #          Misuse can lead to catalog inconsistencies and unexpected behavior.
    # params
    # - lpath: The absolute logical path of the data object to have its permissions modified.
    # - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
    # - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.
    # returns
    # - Status code and response message.
    def modify_replica(self, lpath: str, resource_hierarchy: str='', replica_number: int=-1, new_data_checksum: str='',
                        new_data_comments: str='', new_data_create_time: int=-1, new_data_expiry: int=-1,
                        new_data_mode: str='', new_data_modify_time: str='', new_data_path: str='',
                        new_data_replica_number: int=-1, new_data_replica_status: int=-1, new_data_resource_id: int=-1,
                        new_data_size: int=-1, new_data_status: str='', new_data_type_name: str='', new_data_version: int=-1):
        if (self.token == None):
            raise Exception('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise Exception('lpath must be a string')
        if (not isinstance(resource_hierarchy, str)):
            raise Exception('resource_hierarchy must be a string')
        if (not isinstance(replica_number, int)):
            raise Exception('replica_number must be an int')
        if ((resource_hierarchy != '') and (replica_number != -1)):
            raise Exception('replica_hierarchy and replica_number are mutually exclusive')
        if (not isinstance(new_data_checksum, str)):
            raise Exception('new_data_checksum must be a string')
        if (not isinstance(new_data_comments, str)):
            raise Exception('new_data_comments must be a string')
        if (not isinstance(new_data_create_time, int)):
            raise Exception('new_data_create_time must be an int')
        if (not isinstance(new_data_expiry, int)):
            raise Exception('new_data_expiry must be an int')
        if (not isinstance(new_data_mode, str)):
            raise Exception('new_data_mode must be a string')
        if (not isinstance(new_data_modify_time, str)):
            raise Exception('new_data_modify_time must be a string')
        if (not isinstance(new_data_path, str)):
            raise Exception('new_data_path must be a string')
        if (not isinstance(new_data_replica_number, int)):
            raise Exception('new_data_replica_number must be an int')
        if (not isinstance(new_data_replica_status, int)):
            raise Exception('new_data_replica_status must be an int')
        if (not isinstance(new_data_resource_id, int)):
            raise Exception('new_data_resource_id must be an int')
        if (not isinstance(new_data_size, int)):
            raise Exception('new_data_size must be an int')
        if (not isinstance(new_data_status, str)):
            raise Exception('new_data_status must be a string')
        if (not isinstance(new_data_type_name, str)):
            raise Exception('new_data_type_name must be a string')
        if (not isinstance(new_data_version, int)):
            raise Exception('new_data_version must be an int')
        
        headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        data = {
            'op': 'modify_permissions',
            'lpath': lpath
        }

        if (resource_hierarchy != ''):
            data['resource-hierarchy'] = resource_hierarchy
        
        if (replica_number != -1):
            data['replica-numbeer'] = replica_number

        # Boolean for checking if the user passed in any new_data parameters
        no_params = True

        if (new_data_checksum != ''):
            data['new-data-checksum'] = new_data_checksum
            no_params = False
        
        if (new_data_comments != ''):
            data['new-data-comments'] = new_data_comments
            no_params = False
        
        if (new_data_create_time != -1):
            data['new-data-create-time'] = new_data_create_time
            no_params = False
        
        if (new_data_expiry != -1):
            data['new-data-expiry'] = new_data_expiry
            no_params = False

        if (new_data_mode != ''):
            data['new-data-mode'] = new_data_mode
            no_params = False

        if (new_data_modify_time != ''):
            data['new-data-modify-time'] = new_data_modify_time
            no_params = False

        if (new_data_path != ''):
            data['new-data-path'] = new_data_path
            no_params = False

        if (new_data_replica_number != -1):
            data['new-data-replica-number'] = new_data_replica_number
            no_params = False

        if (new_data_replica_status != -1):
            data['new-data-replica-status'] = new_data_replica_status
            no_params = False

        if (new_data_resource_id != -1):
            data['new-data-resource-id'] = new_data_resource_id
            no_params = False

        if (new_data_size != -1):
            data['new-data-size'] = new_data_size
            no_params = False
        
        if (new_data_status != ''):
            data['new-data-status'] = new_data_status
            no_params = False
        
        if (new_data_type_name != ''):
            data['new-data-type-name'] = new_data_type_name
            no_params = False
        
        if (new_data_version != ''):
            data['new-data-version'] = new_data_version
            no_params = False

        if (no_params):
            raise Exception('At least one new data parameter must be given.')    

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + lpath + '\' modified successfully')
            
            return(rdict)
        else:
            print('Error: ' + r.text)

            return(rdict)