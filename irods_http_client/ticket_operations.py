from . import common
import requests

class Tickets:

    def __init__(self, url_base: str):
        """
        Initializes Tickets with a base url.
        Token is set to None initially, and updated when setToken() is called in irodsClient.
        """
        self.url_base = url_base
        self.token = None

    def create(
        self,
        lpath: str,
        type: str = "read",
        use_count: int = -1,
        write_data_object_count: int = -1,
        write_byte_count: int = -1,
        seconds_until_expiration: int = -1,
        users: str = "",
        groups: str = "",
        hosts: str = "",
    ):
        """
        Creates a new ticket for a collection or data object.

        Parameters
        - lpath: Absolute logical path to a data object or collection.
        - type (optional): Read or write. Defaults to read.
        - use_count (optional): Number of times the ticket can be used.
        - write_data_object_count (optional): Max number of writes that can be performed.
        - write_byte_count (optional): Max number of bytes that can be written.
        - seconds_until_expiration (optional): Number of seconds before the ticket expires.
        - users (optional): Comma-delimited list of users allowed to use the ticket.
        - groups (optional): Comma-delimited list of groups allowed to use the ticket.
        - hosts (optional): Comma-delimited list of hosts allowed to use the ticket.

        Returns
        - A dict containing the HTTP status code and iRODS response.
        - The iRODS response is only valid if no error occurred during HTTP communication.
        """
        common.check_token(self.token)
        common.validate_instance(lpath, str)
        common.validate_instance(type, str)
        if type not in ["read", "write"]:
            raise ValueError("type must be either read or write")
        common.validate_gte_minus1(use_count)
        common.validate_gte_minus1(write_data_object_count)
        common.validate_gte_minus1(write_byte_count)
        common.validate_gte_minus1(seconds_until_expiration)
        common.validate_instance(users, str)
        common.validate_instance(groups, str)
        common.validate_instance(hosts, str)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {"op": "create", "lpath": lpath, "type": type}

        if use_count != -1:
            data["use-count"] = use_count
        if write_data_object_count != -1:
            data["write-data-object-count"] = write_data_object_count
        if write_byte_count != -1:
            data["write-byte-count"] = write_byte_count
        if seconds_until_expiration != -1:
            data["seconds-until-expiration"] = seconds_until_expiration
        if users != "":
            data["users"] = users
        if groups != "":
            data["groups"] = groups
        if hosts != "":
            data["hosts"] = hosts

        r = requests.post(self.url_base + "/tickets", headers=headers, data=data)
        return common.process_response(r)

    def remove(self, name: str):
        """
        Removes an existing ticket.

        Parameters
        - name: The ticket to be removed.

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

        r = requests.post(self.url_base + "/tickets", headers=headers, data=data)
        return common.process_response(r)
