from . import common
import json
import requests

class Collections:

    def __init__(self, url_base: str):
        """
        Initializes Collections with a base url.
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None

    def create(self, lpath: str, create_intermediates: int = 0):
        """
        Creates a new collection.

        Parameters
        - lpath: The absolute logical path of the collection to be created.
        - create_intermediates (optional): Set to 1 to create intermediates, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_0_or_1(create_intermediates)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "create",
            "lpath": lpath,
            "create-intermediates": create_intermediates,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def remove(self, lpath: str, recurse: int = 0, no_trash: int = 0):
        """
        Removes an existing collection.

        Parameters
        - lpath: The absolute logical path of the collection to be removed.
        - recurse (optional): Set to 1 to remove contents of the collection, otherwise set to 0. Defaults to 0.
        - no_trash (optional): Set to 1 to move the collection to trash, 0 to permanently remove. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_0_or_1(recurse)
        common.validate_0_or_1(no_trash)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "remove",
            "lpath": lpath,
            "recurse": recurse,
            "no-trash": no_trash,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def stat(self, lpath: str, ticket: str = ""):
        """
        Gives information about a collection.

        Parameters
        - lpath: The absolute logical path of the collection being accessed.
        - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_instance(ticket, str)

        headers = {
            "Authorization": "Bearer " + self.token,
        }

        params = {"op": "stat", "lpath": lpath, "ticket": ticket}

        r = requests.get(self.url_base + "/collections", params=params, headers=headers)
        return common.process_response(r)

    def list(self, lpath: str, recurse: int = 0, ticket: str = ""):
        """
        Shows the contents of a collection

        Parameters
        - lpath: The absolute logical path of the collection to have its contents listed.
        - recurse (optional): Set to 1 to list the contents of objects in the collection, otherwise set to 0. Defaults to 0.
        - ticket (optional): Ticket to be enabled before the operation. Defaults to an empty string.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_0_or_1(recurse)
        common.validate_instance(ticket, str)

        headers = {
            "Authorization": "Bearer " + self.token,
        }

        params = {"op": "list", "lpath": lpath, "recurse": recurse, "ticket": ticket}

        r = requests.get(self.url_base + "/collections", params=params, headers=headers)
        return common.process_response(r)

    def set_permission(
        self, lpath: str, entity_name: str, permission: str, admin: int = 0
    ):
        """
        Sets the permission of a user for a given collection.

        Parameters
        - lpath: The absolute logical path of the collection to have a permission set.
        - entity_name: The name of the user or group having its permission set.
        - permission: The permission level being set. Either 'null', 'read', 'write', or 'own'.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_instance(entity_name, str)
        common.validate_instance(permission, str)
        if permission not in ["null", "read", "write", "own"]:
            raise ValueError(
                "permission must be either 'null', 'read', 'write', or 'own'"
            )
        common.validate_0_or_1(admin)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "set_permission",
            "lpath": lpath,
            "entity-name": entity_name,
            "permission": permission,
            "admin": admin,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def set_inheritance(self, lpath: str, enable: int, admin: int = 0):
        """
        Sets the inheritance for a collection.

        Parameters
        - lpath: The absolute logical path of the collection to have its inheritance set.
        - enable: Set to 1 to enable inheritance, or 0 to disable.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_0_or_1(enable)
        common.validate_0_or_1(admin)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "set_inheritance",
            "lpath": lpath,
            "enable": enable,
            "admin": admin,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def modify_permissions(self, lpath: str, operations: dict, admin: int = 0):
        """
        Modifies permissions for multiple users or groups for a collection.

        Parameters
        - lpath: The absolute logical path of the collection to have its permissions modified.
        - operations: Dictionary containing the operations to carry out. Should contain names and permissions for all operations.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_instance(operations, list)
        common.validate_instance(operations[0], dict)
        common.validate_0_or_1(admin)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "modify_permissions",
            "lpath": lpath,
            "operations": json.dumps(operations),
            "admin": admin,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def modify_metadata(self, lpath: str, operations: dict, admin: int = 0):
        """
        Modifies the metadata for a collection.

        Parameters
        - lpath: The absolute logical path of the collection to have its metadata modified.
        - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_instance(operations, list)
        common.validate_instance(operations[0], dict)
        common.validate_0_or_1(admin)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "modify_metadata",
            "lpath": lpath,
            "operations": json.dumps(operations),
            "admin": admin,
        }

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def rename(self, old_lpath: str, new_lpath: str):
        """
        Renames or moves a collection

        Parameters
        - old_lpath: The current absolute logical path of the collection.
        - new_lpath: The absolute logical path of the destination for the collection.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(old_lpath, str)
        common.validate_instance(new_lpath, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "rename", "old-lpath": old_lpath, "new-lpath": new_lpath}

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)

    def touch(self, lpath, seconds_since_epoch=-1, reference=""):
        """
        Updates mtime for a collection

        Parameters
        - lpath: The absolute logical path of the collection being touched.
        - seconds_since_epoch (optional): The value to set mtime to, defaults to -1 as a flag.
        - reference (optional): The absolute logical path of the collection to use as a reference for mtime.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_gte_minus1(seconds_since_epoch)
        common.validate_instance(reference, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "touch", "lpath": lpath}

        if seconds_since_epoch != -1:
            data["seconds-since-epoch"] = seconds_since_epoch

        if reference != "":
            data["reference"] = reference

        r = requests.post(self.url_base + "/collections", headers=headers, data=data)
        return common.process_response(r)
