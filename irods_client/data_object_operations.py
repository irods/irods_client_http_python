import requests
import json

class DataObjects:
    def __init__(self, url_base: str):
        """" 
        Initializes DataObjects with a base url. 
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None


    def touch(self, lpath, no_create: int=0, replica_number: int=-1, leaf_resources: str='', seconds_since_epoch=-1, reference=''):
        """
        Updates mtime for an existing data object or creates a new one

        Parameters
        - lpath: The absolute logical path of the data object being touched.
        - no_create (optional): Set to 1 to prevent creating a new object, otherwise set to 0.
        - replica_number (optional): The replica number of the target replica.
        - leaf_resources (optional): The resource holding an existing replica. If one does not exist, creates one.
        - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
        - reference (optional): The absolute logical path of the data object to use as a reference for mtime.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(no_create, int)):
            raise TypeError('no_create must be an int 1 or 0')
        if ((not no_create == 0) and (not no_create == 1)):
            raise ValueError('no_create must be an int 1 or 0')
        if (not isinstance(replica_number, int)):
            raise TypeError('replica_number must be an int')
        if (not replica_number >= -1):
            raise ValueError('replica_number must be greater than or equal to 0 or flag value -1')
        if (not isinstance(leaf_resources, str)):
            raise TypeError('leaf_resources must be a string')
        if (not isinstance(seconds_since_epoch, int)):
            raise TypeError('seconds_since_epoch must be an int')
        if (not seconds_since_epoch >= -1):
            raise ValueError('seconds_since_epoch must be greater than or equal to 0 or flag value -1')
        if (not isinstance(reference, str)):
            raise TypeError('reference must be a string')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def remove(self, lpath: str, catalog_only: int=0, no_trash: int=0, admin: int=0):
        """
        Removes an existing data object.

        Parameters
        - lpath: The absolute logical path of the data object to be removed.
        - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
        - no_trash (optional): Set to 1 to move the data object to trash, 0 to permanently remove. Defaults to 0.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(catalog_only, int)):
            raise TypeError('catalog_only must be an int 1 or 0')
        if ((not catalog_only == 0) and (not catalog_only == 1)):
            raise ValueError('catalog_only must be an int 1 or 0')
        if (not isinstance(no_trash, int)):
            raise TypeError('no_trash must be an int 1 or 0')
        if ((not no_trash == 0) and (not no_trash == 1)):
            raise ValueError('no_trash must be an int 1 or 0')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')

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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        elif (r.status_code / 100 == 4):
            rdict = r.json()
            print('Failed to remove data object \'' + lpath + '\': iRODS Status Code ' + str(rdict['irods_response']['status_code']))

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
    

    def calculate_checksum(self, lpath: str, resource: str='', replica_number: int=-1, force: int=0, all: int=0, admin: int=0):
        """
        Calculates the checksum for a data object.

        Parameters
        - lpath: The absolute logical path of the data object to have its checksum calculated.
        - resource (optional): The resource holding the existing replica.
        - replica_number (optional): The replica number of the target replica.
        - force (optional): Set to 1 to replace the existing checksum, otherwise set to 0. Defaults to 0.
        - all (optional): Set to 1 to calculate the checksum for all replicas, otherwise set to 0. Defaults to 0.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise T('lpath must be a string')
        if (not isinstance(resource, str)):
            raise T('resource must be a string')
        if (not isinstance(replica_number, int)):
            raise T('replica_number must be an int')
        if (not replica_number >= -1):
            raise ValueError('replica number must be greater than or equal to 0 or flag value -1')
        if (not isinstance(force, int)):
            raise TypeError('force must be an int 1 or 0')
        if ((not force == 0) and (not force == 1)):
            raise ValueError('force must be an int 1 or 0')
        if (not isinstance(all, int)):
            raise TypeError('all must be an int 1 or 0')
        if ((not all == 0) and (not all == 1)):
            raise ValueError('all must be an int 1 or 0')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')

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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def verify_checksum(self, lpath: str, resource: str='', replica_number: int=-1, compute_checksums: int=0, admin: int=0):
        """
        Verifies the checksum for a data object.

        Parameters
        - lpath: The absolute logical path of the data object to have its checksum verified.
        - resource (optional): The resource holding the existing replica.
        - replica_number (optional): The replica number of the target replica.
        - compute_checksums (optional): Set to 1 to skip checksum calculation, otherwise set to 0. Defaults to 0.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(resource, str)):
            raise TypeError('resource must be a string')
        if (not isinstance(replica_number, int)):
            raise TypeError('replica_number must be an int')
        if (not replica_number >= -1):
            raise ValueError('replica_number must be greater than or equal to 0 or flag value -1')
        if (not isinstance(compute_checksums, int)):
            raise TypeError('compute_checksums must be an int 1 or 0')
        if ((not compute_checksums == 0) and (not compute_checksums == 1)):
            raise ValueError('force must be an int 1 or 0')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')

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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def stat(self, lpath: str, ticket: str=''):
        """
        Gives information about a data object.

        Parameters
        - lpath: The absolute logical path of the data object being accessed.
        - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(ticket, str)):
            raise TypeError('ticket must be a string')
        
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
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def rename(self, old_lpath: str, new_lpath: str):
        """
        Renames or moves a data object.

        Parameters
        - old_lpath: The current absolute logical path of the data object.
        - new_lpath: The absolute logical path of the destination for the data object.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(old_lpath, str)):
            raise TypeError('old_lpath must be a string')
        if (not isinstance(new_lpath, str)):
            raise TypeError('new_lpath must be a string')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def copy(self, src_lpath: str, dst_lpath: str, src_resource: str='', dst_resource: str='', overwrite: int=0):
        """
        Copies a data object.

        Parameters
        - src_lpath: The absolute logical path of the source data object.
        - dst_lpath: The absolute logical path of the destination.
        - src_resource: The absolute logical path of the source resource.
        - dst_resource: The absolute logical path of the destination resource.
        - overwrite: set to 1 to overwrite an existing objject, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(src_lpath, str)):
            raise TypeError('src_lpath must be a string')
        if (not isinstance(dst_lpath, str)):
            raise TypeError('dst_lpath must be a string')
        if (not isinstance(src_resource, str)):
            raise TypeError('src_resource must be a string')
        if (not isinstance(dst_resource, str)):
            raise TypeError('dst_lpath must be a string')
        if (not isinstance(overwrite, int)):
            raise TypeError('overwrite must be an int 1 or 0')
        if ((not overwrite == 0) and (not overwrite == 1)):
            raise ValueError('overwrite must be an int 1 or 0')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def replicate(self, lpath: str, src_resource: str='', dst_resource: str='', admin: int=0):
        """
        Replicates a data object from one resource to another.

        Parameters
        - lpath: The  absolute logical path of the data object to be replicated.
        - src_resource: The absolute logical path of the source resource.
        - dst_resource: The absolute logical path of the destination resource.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(src_resource, str)):
            raise TypeError('src_resource must be a string')
        if (not isinstance(dst_resource, str)):
            raise TypeError('dst_lpath must be a string')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def trim(self, lpath: str, replica_number: int, catalog_only: int=0, admin: int=0):
        """
        Trims an existing replica or removes its catalog entry.

        Parameters
        - lpath: The  absolute logical path of the data object to be trimmed.
        - replica_number: The replica number of the target replica.
        - catalog_only (optional): Set to 1 to remove only the catalog entry, otherwise set to 0. Defaults to 0.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise T('lpath must be a string')
        if (not isinstance(replica_number, int)):
            raise T('replica_number must be an int')
        if (not isinstance(catalog_only, int)):
            raise TypeError('catalog_only must be an int 1 or 0')
        if ((not catalog_only == 0) and (not catalog_only == 1)):
            raise ValueError('catalog_only must be an int 1 or 0')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')
        
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
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def register(self, lpath: str, ppath: str, resource: str, as_additional_replica: int=0, data_size: int=-1, checksum: str=''):
        """
        Registers a data object/replica into the catalog.

        Parameters
        - lpath: The  absolute logical path of the data object to be registered.
        - ppath: The  absolute physical path of the data object to be registered.
        - resource: The resource that will own the replica.
        - as_additional_replica (optional): Set to 1 to register as a replica of an existing object, otherwise set to 0. Defaults to 0.
        - data_size (optional): The size of the replica in bytes.
        - checksum (optional): The checksum to associate with the replica.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(ppath, str)):
            raise TypeError('ppath must be a string')
        if (not isinstance(resource, str)):
            raise TypeError('resource must be a string')
        if (not isinstance(as_additional_replica, int)):
            raise TypeError('as_additional_replica must be an int 1 or 0')
        if ((not as_additional_replica == 0) and (not as_additional_replica == 1)):
            raise ValueError('as_additional_replica must be an int 1 or 0')
        if (not isinstance(data_size, int)):
            raise TypeError('data_size must be an int')
        if (not data_size >= -1):
            raise ValueError('data_size must be greater than or equal to 0 or flag value -1')
        if (not isinstance(checksum, str)):
            raise TypeError('checksum must be a string')
        
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
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def read(self, lpath: str, offset: int=0, count: int=-1, ticket: str=''):
        """
        Reads bytes from a data object.

        Parameters
        - lpath: The absolute logical path of the data object to be read from.
        - offset (optional): The number of bytes to skip. Defaults to 0.
        - count (optional): The number of bytes to read.
        - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(offset, int)):
            raise TypeError('offset must be an int')
        if (not isinstance(count, int)):
            raise TypeError('count must be an int')
        if (not count >= -1):
            raise ValueError('count must be greater than or equal to 0 or flag value -1')
        if (not isinstance(ticket, str)):
            raise TypeError('ticket must be a string')
        
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
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
    

    def write(self, bytes, lpath: str='', resource: str='', offset: int=0, truncate: int=1, append: int=0, parallel_write_handle: str='', stream_index: int=-1):
        """
        Writes bytes to a data object.

        Parameters
        - lpath: The absolute logical path of the data object to be written to.
        - bytes: The bytes to be written.
        - resource (optional): The root resource to write to.
        - offset (optional): The number of bytes to skip. Defaults to 0.
        - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
        - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
        - parallel_write_handle (optional): The handle to be used when writing in parallel.
        - stream_index (optional): The stream to use when writing in parallel.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(resource, str)):
            raise TypeError('resource must be a string')
        if (not isinstance(offset, int)):
            raise TypeError('offset must be an int')
        if (not offset >= 0):
            raise ValueError('offset must be greater than or equal to 0')
        if (not isinstance(truncate, int)):
            raise TypeError('truncate must be an int 1 or 0')
        if ((not truncate == 0) and (not truncate == 1)):
            raise ValueError('truncate must be an int 1 or 0')
        if (not isinstance(append, int)):
            raise TypeError('append must be an int 1 or 0')
        if ((not append == 0) and (not append == 1)):
            raise ValueError('append must be an int 1 or 0')
        if (not isinstance(parallel_write_handle, str)):
            raise TypeError('parallel_write_handle must be a string')
        if (not isinstance(stream_index, int)):
            raise TypeError('stream_index must be an int')
        if (not stream_index >= -1):
            raise ValueError('stream_index must be greater than or equal to 0 or flag value -1')
        
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

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to write to \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('Sucessfully wrote to \'' + lpath + '\'')
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def parallel_write_init(self, lpath: str, stream_count: int, truncate: int=1, append: int=0, ticket: str=''):
        """
        Initializes server-side state for parallel writing.

        Parameters
        - lpath: The absolute logical path of the data object to be initialized for parallel write.
        - stream_count: THe number of streams to open.
        - offset (optional): The number of bytes to skip. Defaults to 0.
        - truncate (optional): Set to 1 to truncate the data object before writing, otherwise set to 0. Defaults to 1.
        - append (optional): Set to 1 to append bytes to the data objectm otherwise set to 0. Defaults to 0.
        - ticket (optional):  Ticket to be enabled before the operation. Defaults to an empty string.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(stream_count, int)):
            raise TypeError('stream_count must be an int')
        if (not stream_count >= 0):
            raise ValueError('stream_count must be greater than or equal to 0 or flag value -1')
        if (not isinstance(truncate, int)):
            raise TypeError('truncate must be an int 1 or 0')
        if ((not truncate == 0) and (not truncate == 1)):
            raise ValueError('truncate must be an int 1 or 0')
        if (not isinstance(append, int)):
            raise TypeError('append must be an int 1 or 0')
        if ((not append == 0) and (not append == 1)):
            raise ValueError('append must be an int 1 or 0')
        if (not isinstance(ticket, str)):
            raise TypeError('ticket must be a string')
        
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
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def parallel_write_shutdown(self, parallel_write_handle: str):
        """
        Shuts down the parallel write state in the server.

        Parameters
        - parallel_write_handle: Handle obtained from parallel_write_init

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(parallel_write_handle, str)):
            raise TypeError('parallel_write_handle must be a string')
        
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
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        
    
    def modify_metadata(self, lpath: str, operations: list, admin: int=0):
        """
        Modifies the metadata for a data object

        Parameters
        - lpath: The absolute logical path of the data object to have its inheritance set.
        - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(operations, list)):
            raise TypeError('operations must be a list of dictionaries')
        if (not isinstance(operations[0], dict)):
            raise TypeError('operations must be a list of dictionaries')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')

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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        

    def set_permission(self, lpath: str, entity_name: str, permission: str, admin: int=0):
        """
        Sets the permission of a user for a given data object

        Parameters
        - lpath: The absolute logical path of the data object to have a permission set.
        - entity_name: The name of the user or group having its permission set.
        - permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(entity_name, str)):
            raise TypeError('entity_name must be a string')
        if (not isinstance(permission, str)):
            raise TypeError('permission must be a string (\'null\', \'read\', \'write\', or \'own\')')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
    
    
    def modify_permissions(self, lpath: str, operations: list, admin: int=0):
        """
        Modifies permissions for multiple users or groups for a data object.

        Parameters
        - lpath: The absolute logical path of the data object to have its permissions modified.
        - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(operations, list)):
            raise TypeError('operations must be a list of dictionaries')
        if (not isinstance(operations[0], dict)):
            raise TypeError('operations must be a list of dictionaries')
        if (not isinstance(admin, int)):
            raise TypeError('admin must be an int 1 or 0')
        if ((not admin == 0) and (not admin == 1)):
            raise ValueError('admin must be an int 1 or 0')
        
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
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        
    
    def modify_replica(self, lpath: str, resource_hierarchy: str='', replica_number: int=-1, new_data_checksum: str='',
                        new_data_comments: str='', new_data_create_time: int=-1, new_data_expiry: int=-1,
                        new_data_mode: str='', new_data_modify_time: str='', new_data_path: str='',
                        new_data_replica_number: int=-1, new_data_replica_status: int=-1, new_data_resource_id: int=-1,
                        new_data_size: int=-1, new_data_status: str='', new_data_type_name: str='', new_data_version: int=-1):
        """
        Modifies properties of a single replica.

        WARNING: 
        This operation requires rodsadmin level privileges and should only be used when there isn't a safer option.
        Misuse can lead to catalog inconsistencies and unexpected behavior.

        Parameters
        - lpath: The absolute logical path of the data object to have a replica modified.
        - resource_hierarchy: The hierarchy containing the resource to be modified. Mutually exclusive with replica_number.
        - replica_number: The number of the replica to be modified. mutually exclusive with resource_hierarchy.

        Note:
        At least one of the following optional parameters must be passed in

        - new_data_checksum (optional): The new checksum to be set.
        - new_data_comments (optional): The new comments to be set.
        - new_data_create_time (optional): The new create time to be set.
        - new_data_expiry (optional): The new expiry to be set.
        - new_data_mode (optional): The new mode to be set.
        - new_data_modify_time (optional): The new modify time to be set.
        - new_data_path (optional): The new path to be set.
        - new_data_replica_number (optional): The new replica number to be set.
        - new_data_replica_status (optional): The new replica status to be set.
        - new_data_resource_id (optional): The new resource id to be set
        - new_data_size (optional): The new size to be set.
        - new_data_status (optional): The new data status to be set.
        - new_data_type_name (optional): The new type name to be set.
        - new_data_version (optional): The new version to be set.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        if (self.token == None):
            raise RuntimeError('No token set. Use setToken() to set the auth token to be used')
        if (not isinstance(lpath, str)):
            raise TypeError('lpath must be a string')
        if (not isinstance(resource_hierarchy, str)):
            raise TypeError('resource_hierarchy must be a string')
        if (not isinstance(replica_number, int)):
            raise TypeError('replica_number must be an int')
        if ((resource_hierarchy != '') and (replica_number != -1)):
            raise ValueError('replica_hierarchy and replica_number are mutually exclusive')
        if (not isinstance(new_data_checksum, str)):
            raise TypeError('new_data_checksum must be a string')
        if (not isinstance(new_data_comments, str)):
            raise TypeError('new_data_comments must be a string')
        if (not isinstance(new_data_create_time, int)):
            raise TypeError('new_data_create_time must be an int')
        if (not new_data_create_time >= -1):
            raise ValueError('new_data_create_time must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_expiry, int)):
            raise TypeError('new_data_expiry must be an int')
        if (not new_data_expiry >= -1):
            raise ValueError('new_data_expiry must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_mode, str)):
            raise TypeError('new_data_mode must be a string')
        if (not isinstance(new_data_modify_time, str)):
            raise TypeError('new_data_modify_time must be a string')
        if (not isinstance(new_data_path, str)):
            raise TypeError('new_data_path must be a string')
        if (not isinstance(new_data_replica_number, int)):
            raise TypeError('new_data_replica_number must be an int')
        if (not new_data_replica_number >= -1):
            raise ValueError('new_data_replica_number must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_replica_status, int)):
            raise TypeError('new_data_replica_status must be an int')
        if (not new_data_replica_status >= -1):
            raise ValueError('new_data_replica_status must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_resource_id, int)):
            raise TypeError('new_data_resource_id must be an int')
        if (not new_data_resource_id >= -1):
            raise ValueError('new_data_resource_id must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_size, int)):
            raise TypeError('new_data_size must be an int')
        if (not new_data_size >= -1):
            raise ValueError('new_data_size must be greater than or equal to 0 or flag value -1')
        if (not isinstance(new_data_status, str)):
            raise TypeError('new_data_status must be a string')
        if (not isinstance(new_data_type_name, str)):
            raise TypeError('new_data_type_name must be a string')
        if (not isinstance(new_data_version, int)):
            raise TypeError('new_data_version must be an int')
        if (not new_data_version >= -1):
            raise ValueError('new_data_version must be greater than or equal to 0 or flag value -1')
        
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
            raise RuntimeError('At least one new data parameter must be given.')    

        r = requests.post(self.url_base + '/data-objects', headers=headers, data=data)

        if (r.status_code / 100 == 2):
            rdict = r.json()

            if rdict['irods_response']['status_code']:
                print('Failed to modify \'' + lpath + '\': iRODS Status Code' + str(rdict['irods_response']['status_code']))
            else:
                print('\'' + lpath + '\' modified successfully')
            
            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )
        else:
            irods_err = ''
            rdict = None
            if (r.text != ''):
                rdict = r.json()
                irods_err = ': iRods Status Code' + str(rdict['irods_response'])
            print(f'Error <{r.status_code}>{irods_err}')

            return(
                {
                    'status_code': r.status_code,
                    'data': rdict
                }
            )