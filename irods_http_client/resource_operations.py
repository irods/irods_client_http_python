from . import common
import json
import requests

class Resources:

    def __init__(self, url_base: str):
        """
        Initializes DataObjects with a base url.
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None

    def create(self, name: str, type: str, host: str, vault_path: str, context: str):
        """
        Creates a new resource.

        Parameters
        - name: The name of the resource to be created.
        - type: The type of the resource to be created.
        - host: The host of the resource to be created. May or may not be required depending on the resource type.
        - vault_path: Path to the storage vault for the resource. May or may not be required depending on the resource type.
        - context:  May or may not be required depending on the resource type.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)
        common.validate_instance(type, str)
        common.validate_instance(host, str)
        common.validate_instance(vault_path, str)
        common.validate_instance(context, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "create", "name": name, "type": type}

        if host != "":
            data["host"] = host

        if vault_path != "":
            data["vault-path"] = vault_path

        if context != "":
            data["context"] = context

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def remove(self, name: str):
        """
        Removes an existing resource.

        Parameters
        - name: The name of the resource to be removed.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "remove", "name": name}

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def modify(self, name: str, property: str, value: str):
        """
        Modifies a property for a resource.

        Parameters
        - name: The name of the resource to be modified.
        - property: The property to be modified.
        - value: The new value to be set.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)
        common.validate_instance(property, str)
        if property not in [
            "name",
            "type",
            "host",
            "vault_path",
            "context",
            "status",
            "free_space",
            "comments",
            "information",
        ]:
            raise ValueError(
                "Invalid property. Valid properties:\n - name\n - type\n - host\n - "
                "vault_path\n - context"
                + "\n - status\n - free_space\n - comments\n - information"
            )
        common.validate_instance(value, str)
        if (property == "status") and (value != "up") and (value != "down"):
            raise ValueError("status must be either 'up' or 'down'")

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "modify", "name": name, "property": property, "value": value}

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def add_child(self, parent_name: str, child_name: str, context: str = ""):
        """
        Creates a parent-child relationship between two resources.

        Parameters
        - parent_name: The name of the parent resource.
        - child_name: The name of the child resource.
        - context (optional): Additional information for the zone.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(parent_name, str)
        common.validate_instance(child_name, str)
        common.validate_instance(context, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "add_child", "parent-name": parent_name, "child-name": child_name}

        if context != "":
            data["context"] = context

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def remove_child(self, parent_name: str, child_name: str):
        """
        Removes a parent-child relationship between two resources.

        Parameters
        - parent_name: The name of the parent resource.
        - child_name: The name of the child resource.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(parent_name, str)
        common.validate_instance(child_name, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "remove_child",
            "parent-name": parent_name,
            "child-name": child_name,
        }

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def rebalance(self, name: str):
        """
        Rebalances a resource hierarchy.

        Parameters
        - name: The name of the resource to be rebalanced.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "rebalance", "name": name}

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)

    def stat(self, name: str):
        """
        Retrieves information for a resource.

        Parameters
        - name: The name of the resource to be accessed.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        params = {"op": "stat", "name": name}

        r = requests.get(self.url_base + "/resources", headers=headers, params=params)
        return common.process_response(r)

    def modify_metadata(self, name: str, operations: dict, admin: int = 0):
        """
        Modifies the metadata for a resource.

        Parameters
        - name: The absolute logical path of the resource to have its metadata modified.
        - operations: Dictionary containing the operations to carry out. Should contain the operation, attribute, value, and optionally units.
        - admin (optional): Set to 1 to run this operation as an admin, otherwise set to 0. Defaults to 0.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(name, str)
        common.validate_instance(operations, list)
        common.validate_instance(operations[0], dict)
        common.validate_0_or_1(admin)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "op": "modify_metadata",
            "name": name,
            "operations": json.dumps(operations),
            "admin": admin,
        }

        r = requests.post(self.url_base + "/resources", headers=headers, data=data)
        return common.process_response(r)
