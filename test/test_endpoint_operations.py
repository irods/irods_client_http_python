"""
Integration tests for iRODS HTTP API endpoint operations.

Tests cover all major operation categories: collections, data objects,
resources, rules, queries, tickets, users/groups, and zones.
"""

import concurrent.futures
import logging
import pathlib
import time
import unittest

import config

from irods_http_client import IRODSHTTPClient


def setup_class(cls, opts):
	"""
	Initialize shared state needed by all test cases.

	This function is designed to be called in setUpClass().

	Args:
	    cls: The class to attach state to.
	    opts: A dict containing options for controlling the behavior of the function.
	"""
	# Used as a signal for determining whether setUpClass() succeeded or not.
	# If this results in being True, no tests should be allowed to run.
	cls._class_init_error = False

	# Initialize the class logger.
	cls.logger = logging.getLogger(cls.__name__)

	log_level = config.test_config.get("log_level", logging.INFO)
	cls.logger.setLevel(log_level)

	ch = logging.StreamHandler()
	ch.setLevel(log_level)
	ch.setFormatter(logging.Formatter(f"[%(asctime)s] [{cls.__name__}] [%(levelname)s] %(message)s"))

	cls.logger.addHandler(ch)

	# Initialize state.

	if config.test_config.get("host", None) is None:
		cls.logger.debug("Missing configuration property: host")
		cls._class_init_error = True
		return

	if config.test_config.get("port", None) is None:
		cls.logger.debug("Missing configuration property: port")
		cls._class_init_error = True
		return

	if config.test_config.get("url_base", None) is None:
		cls.logger.debug("Missing configuration property: url_base")
		cls._class_init_error = True
		return

	cls.url_base = f"http://{config.test_config['host']}:{config.test_config['port']}{config.test_config['url_base']}"
	cls.url_endpoint = f'{cls.url_base}/{opts["endpoint_name"]}'

	cls.api = IRODSHTTPClient(cls.url_base)

	cls.zone_name = config.test_config["irods_zone"]
	cls.host = config.test_config["irods_server_hostname"]

	# create_rodsuser cannot be honored if init_rodsadmin is set to False.
	# Therefore, return immediately.
	if not opts.get("init_rodsadmin", True):
		cls.logger.debug("init_rodsadmin is False. Class setup complete.")
		return

	# Authenticate as a rodsadmin and store the bearer token.
	cls.rodsadmin_username = config.test_config["rodsadmin"]["username"]

	try:
		cls.rodsadmin_bearer_token = cls.api.authenticate(
			cls.rodsadmin_username, config.test_config["rodsadmin"]["password"]
		)
	except RuntimeError:
		cls._class_init_error = True
		cls.logger.debug("Failed to authenticate as rodsadmin [%].", cls.rodsadmin_username)
		return

	# Authenticate as a rodsuser and store the bearer token.
	cls.rodsuser_username = config.test_config["rodsuser"]["username"]

	try:
		cls.api.users_groups.create_user(cls.rodsuser_username, cls.zone_name, "rodsuser")
		cls.api.users_groups.set_password(
			cls.rodsuser_username,
			cls.zone_name,
			config.test_config["rodsuser"]["password"],
		)
		cls.rodsuser_bearer_token = cls.api.authenticate(
			cls.rodsuser_username, config.test_config["rodsuser"]["password"]
		)
	except RuntimeError:
		cls._class_init_error = True
		cls.logger.debug("Failed to authenticate as rodsuser [%].", cls.rodsuser_username)
		return

	cls.logger.debug("Class setup complete.")


def tear_down_class(cls):
	"""
	Clean up shared state after test class execution.

	Removes the rodsuser created during setup.

	Args:
	    cls: The class to clean up state from.
	"""
	if cls._class_init_error:
		return

	cls.api.users_groups.remove_user(cls.rodsuser_username, cls.zone_name)


# Tests for library
class LibraryTests(unittest.TestCase):
	"""Test library-level operations (info, get_token)."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for library tests."""
		setup_class(cls, {"endpoint_name": "collections"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	# tests the info operation
	def test_info(self):
		"""Test the info operation to retrieve server information."""
		self.api.info()

	# tests the getToken operation
	def test_get_token(self):
		"""Test the get_token operation to retrieve the current authentication token."""
		self.api.get_token()


# Tests for collections operations
class CollectionsTests(unittest.TestCase):
	"""Test iRODS collection operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for collection tests."""
		setup_class(cls, {"endpoint_name": "collections"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	# tests the create operation
	def test_create(self):
		"""Test collection creation operations and parameter validation."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# clean up test collections
		self.api.collections.remove(f"/{self.zone_name}/home/new")
		self.api.collections.remove(f"/{self.zone_name}/home/test/folder")
		self.api.collections.remove(f"/{self.zone_name}/home/test")

		# test param checking
		self.assertRaises(TypeError, self.api.collections.create, 0, 0)
		self.assertRaises(
			TypeError,
			self.api.collections.create,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.create,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			7,
		)

		# test creating new collection
		response = self.api.collections.create(f"/{self.zone_name}/home/new")
		self.assertTrue(response["data"]["created"])
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test creating existing collection
		response = self.api.collections.create(f"/{self.zone_name}/home/new")
		self.assertFalse(response["data"]["created"])
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test invalid path
		response = self.api.collections.create(f"{self.zone_name}/home/new")
		self.assertEqual(
			"{'irods_response': {'status_code': -358000, "
			"'status_message': 'path does not exist: OBJ_PATH_DOES_NOT_EXIST'}}",
			str(response["data"]),
		)

		# test create_intermediates
		response = self.api.collections.create(f"/{self.zone_name}/home/test/folder", 0)
		self.assertEqual(
			"{'irods_response': {'status_code': -358000, "
			"'status_message': 'path does not exist: OBJ_PATH_DOES_NOT_EXIST'}}",
			str(response["data"]),
		)
		response = self.api.collections.create(f"/{self.zone_name}/home/test/folder", 1)
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)

	# tests the remove operation
	def test_remove(self):
		"""Test collection removal operations and parameter validation."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# clean up test collections
		self.api.collections.remove(f"/{self.zone_name}/home/new")
		self.api.collections.remove(f"/{self.zone_name}/home/test/folder")
		self.api.collections.remove(f"/{self.zone_name}/home/test")

		# test param checking
		self.assertRaises(TypeError, self.api.collections.remove, 0, 0, 0)
		self.assertRaises(
			TypeError,
			self.api.collections.remove,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			0,
		)
		self.assertRaises(
			ValueError,
			self.api.collections.remove,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.remove,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.remove,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			5,
		)

		# test removing collection
		response = self.api.collections.create(f"/{self.zone_name}/home/new")
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)
		response = self.api.collections.remove(f"/{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))
		# test invalid paths
		response = self.api.collections.stat(f"/{self.zone_name}/home/tensaitekinaaidorusama")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"/{self.zone_name}/home/aremonainainaikoremonainainai")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"/{self.zone_name}/home/binglebangledingledangle")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test recurse
		response = self.api.collections.create(f"/{self.zone_name}/home/test/folder", 1)
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)
		response = self.api.collections.remove(f"/{self.zone_name}/home/test", 0)
		self.assertEqual(
			"{'irods_response': {'status_code': -79000, "
			"'status_message': 'cannot remove non-empty collection: "
			"SYS_COLLECTION_NOT_EMPTY'}}",
			str(response["data"]),
		)
		response = self.api.collections.remove(f"/{self.zone_name}/home/test", 1)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

	# tests the stat operation
	def test_stat(self):
		"""Test collection stat operation to retrieve metadata."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# clean up test collections
		self.api.collections.remove(f"/{self.zone_name}/home/new")

		# test param checking
		self.assertRaises(TypeError, self.api.collections.stat, 0, "ticket")
		self.assertRaises(
			TypeError,
			self.api.collections.stat,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
		)

		# test invalid paths
		response = self.api.collections.stat(f"/{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test valid path
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["permissions"])

	# tests the list operation
	def test_list(self):
		"""Test collection list operation to enumerate contents."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# clean up test collections
		self.api.collections.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb")
		self.api.collections.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/albania")
		self.api.collections.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia")
		self.api.collections.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia")

		# test param checking
		self.assertRaises(TypeError, self.api.collections.list, 0, "ticket")
		self.assertRaises(
			TypeError,
			self.api.collections.list,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			"ticket",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.list,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			"ticket",
		)
		self.assertRaises(
			TypeError,
			self.api.collections.list,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			0,
		)

		# test empty collection
		response = self.api.collections.list(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual("None", str(response["data"]["entries"]))

		# test collection with one item
		self.api.collections.create(f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia")
		response = self.api.collections.list(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia",
			str(response["data"]["entries"][0]),
		)

		# test collection with multiple items
		self.api.collections.create(f"/{self.zone_name}/home/{self.rodsadmin_username}/albania")
		self.api.collections.create(f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia")
		response = self.api.collections.list(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/albania",
			str(response["data"]["entries"][0]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia",
			str(response["data"]["entries"][1]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia",
			str(response["data"]["entries"][2]),
		)

		# test without recursion
		self.api.collections.create(f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb")
		response = self.api.collections.list(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/albania",
			str(response["data"]["entries"][0]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia",
			str(response["data"]["entries"][1]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia",
			str(response["data"]["entries"][2]),
		)
		self.assertEqual(len(response["data"]["entries"]), 3)

		# test with recursion
		response = self.api.collections.list(f"/{self.zone_name}/home/{self.rodsadmin_username}", 1)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/albania",
			str(response["data"]["entries"][0]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia",
			str(response["data"]["entries"][1]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia",
			str(response["data"]["entries"][2]),
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb",
			str(response["data"]["entries"][3]),
		)

	# tests the set permission operation
	def test_set_permission(self):
		"""Test setting permissions on collections."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# test param checking
		self.assertRaises(TypeError, self.api.collections.set_permission, 0, "jeb", "read", 0)
		self.assertRaises(
			TypeError,
			self.api.collections.set_permission,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"read",
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.set_permission,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			0,
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.set_permission,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			"read",
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.set_permission,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			"read",
			5,
		)

		# create new collection
		response = self.api.collections.create(f"/{self.zone_name}/home/setPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test no permission
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/setPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], -170000)

		# test set permission
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.set_permission(
			f"/{self.zone_name}/home/setPerms", self.rodsuser_username, "read"
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test with permission
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/setPerms")
		self.assertTrue(response["data"]["permissions"])

		# test set permission null
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.set_permission(
			f"/{self.zone_name}/home/setPerms", self.rodsuser_username, "null"
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test no permission
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/setPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# remove the collection
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.remove(f"/{self.zone_name}/home/setPerms", 1, 1)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# tests the set inheritance operation
	def test_set_inheritance(self):
		"""Test setting inheritance for collection permissions."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# test param checking
		self.assertRaises(TypeError, self.api.collections.set_inheritance, 0, 0, 0)
		self.assertRaises(
			TypeError,
			self.api.collections.set_inheritance,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			0,
		)
		self.assertRaises(
			ValueError,
			self.api.collections.set_inheritance,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.set_inheritance,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.set_inheritance,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			5,
		)

		# control
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertFalse(response["data"]["inheritance_enabled"])

		# test enabling inheritance
		response = self.api.collections.set_inheritance(f"/{self.zone_name}/home/{self.rodsadmin_username}", 1)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# check if changed
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["inheritance_enabled"])

		# test disabling inheritance
		response = self.api.collections.set_inheritance(f"/{self.zone_name}/home/{self.rodsadmin_username}", 0)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# check if changed
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertFalse(response["data"]["inheritance_enabled"])

	# test the modify permissions operation
	def test_modify_permissions(self):
		"""Test modifying permissions on collections."""
		self.api.set_token(self.rodsadmin_bearer_token)

		ops_permissions = [{"entity_name": self.rodsuser_username, "acl": "read"}]

		ops_permissions_null = [{"entity_name": self.rodsuser_username, "acl": "null"}]

		# test param checking
		self.assertRaises(TypeError, self.api.collections.modify_permissions, 0, ops_permissions, 0)
		self.assertRaises(
			TypeError,
			self.api.collections.modify_permissions,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.modify_permissions,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_permissions,
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.modify_permissions,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_permissions,
			5,
		)

		# create new collection
		response = self.api.collections.create(f"/{self.zone_name}/home/modPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test no permissions
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/modPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test set permissions
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.modify_permissions(f"/{self.zone_name}/home/modPerms", ops_permissions)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test with permissions
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/modPerms")
		self.assertTrue(response["data"]["permissions"])

		# test set permissions nuil
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.modify_permissions(f"/{self.zone_name}/home/modPerms", ops_permissions_null)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test without permissions
		self.api.set_token(self.rodsuser_bearer_token)
		response = self.api.collections.stat(f"/{self.zone_name}/home/modPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# remove the collection
		self.api.set_token(self.rodsadmin_bearer_token)
		response = self.api.collections.remove(f"/{self.zone_name}/home/modPerms", 1, 1)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# test the modify metadata operation
	def test_modify_metadata(self):
		"""Test modifying metadata on collections."""
		self.api.set_token(self.rodsadmin_bearer_token)

		ops_metadata = [{"operation": "add", "attribute": "eyeballs", "value": "itchy"}]

		ops_metadata_remove = [{"operation": "remove", "attribute": "eyeballs", "value": "itchy"}]

		# test param checking
		self.assertRaises(TypeError, self.api.collections.modify_metadata, 0, ops_metadata, 0)
		self.assertRaises(
			TypeError,
			self.api.collections.modify_metadata,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			self.api.collections.modify_metadata,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata,
			"0",
		)
		self.assertRaises(
			ValueError,
			self.api.collections.modify_metadata,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata,
			5,
		)

		# test adding and removing metadata
		response = self.api.collections.modify_metadata(
			f"/{self.zone_name}/home/{self.rodsadmin_username}", ops_metadata
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)
		response = self.api.collections.modify_metadata(
			f"/{self.zone_name}/home/{self.rodsadmin_username}", ops_metadata_remove
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# tests the rename operation
	def test_rename(self):
		"""Test renaming collections."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# test param checking
		self.assertRaises(
			TypeError,
			self.api.collections.rename,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
		)
		self.assertRaises(TypeError, self.api.collections.rename, 0, f"/{self.zone_name}/home/pods")

		# test before move
		response = self.api.collections.stat(f"/{self.zone_name}/home/pods")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["permissions"])

		# test renaming
		response = self.api.collections.rename(
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			f"/{self.zone_name}/home/pods",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test before move
		response = self.api.collections.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = self.api.collections.stat(f"/{self.zone_name}/home/pods")
		self.assertTrue(response["data"]["permissions"])

		# test renaming
		response = self.api.collections.rename(
			f"/{self.zone_name}/home/pods",
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

	# tests the touch operation
	def test_touch(self):
		"""Test touch operation to update collection timestamps."""
		self.api.set_token(self.rodsadmin_bearer_token)
		self.api.collections.touch(
			f"/{self.zone_name}/home/{self.rodsadmin_username}", reference=f"/{self.zone_name}/home"
		)


# Tests for data object operations
class DataObjectsTests(unittest.TestCase):
	"""Test iRODS data object operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for data object tests."""
		setup_class(cls, {"endpoint_name": "data_objects"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_common_operations(self):
		"""Test common data object operations (write, read, replicate, etc.)."""
		self.api.set_token(self.rodsadmin_bearer_token)

		f1 = f"/{self.zone_name}/home/{self.rodsuser_username}/f1.txt"
		f2 = f"/{self.zone_name}/home/{self.rodsuser_username}/f2.txt"
		f3 = f"/{self.zone_name}/home/{self.rodsuser_username}/f3.txt"
		resc = "resource"

		try:
			# Create a unixfilesystem resource
			r = self.api.resources.create(
				resc,
				"unixfilesystem",
				self.host,
				"/tmp/resource",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			self.api.set_token(self.rodsuser_bearer_token)
			# Create a non-empty data object
			r = self.api.data_objects.write("These are the bytes being written to the object", f1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Read the data object
			r = self.api.data_objects.read(f1, offset=6)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertIn('being written', r["data"]['irods_response']['bytes'].decode('utf-8'))

			# Add metadata to the data object
			r = self.api.data_objects.modify_metadata(
				f1, operations=[{'operation': 'add', 'attribute': 'a', 'value': 'v', 'units': 'u'}]
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Modify the replica
			self.api.set_token(self.rodsadmin_bearer_token)
			r = self.api.data_objects.modify_replica(f1, replica_number=0, new_data_comments="awesome")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.api.set_token(self.rodsuser_bearer_token)

			# Replicate the data object
			r = self.api.data_objects.replicate(
				f1,
				dst_resource=resc,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show that there are two replicas
			r = self.api.queries.execute_genquery(
				f"select DATA_NAME, DATA_REPL_NUM where DATA_NAME = '{f1.rsplit('/', maxsplit=1)[-1]}'"
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(len(r["data"]["rows"]), 2)

			# Trim the first data object
			r = self.api.data_objects.trim(f1, 0)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Rename the data object
			r = self.api.data_objects.rename(f1, f2)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Copy the data object
			r = self.api.data_objects.copy(f2, f3)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Set permission on the object
			r = self.api.data_objects.set_permission(
				f3,
				"rods",
				"read",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Confirm that the permission has been set
			r = self.api.data_objects.stat(f3)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertIn(
				{
					"name": "rods",
					"zone": self.zone_name,
					"type": "rodsadmin",
					"perm": "read_object",
				},
				r["data"]["permissions"],
			)

			# Modify permission on the object
			r = self.api.data_objects.modify_permissions(f3, operations=[{'entity_name': 'rods', 'acl': 'write'}])
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			# Remove the data objects
			r = self.api.data_objects.remove(f1, 0, 1)

			r = self.api.data_objects.remove(f2, 0, 1)

			r = self.api.data_objects.remove(f3, 0, 1)

			# Remove the resource
			self.api.set_token(self.rodsadmin_bearer_token)
			r = self.api.resources.remove(resc)

	def test_checksums(self):
		"""Test checksum calculation and verification for data objects."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a unixfilesystem resource.
		r = self.api.resources.create(
			"newresource",
			"unixfilesystem",
			self.host,
			"/tmp/newresource",  # noqa: S108
			"",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Create a non-empty data object
		r = self.api.data_objects.write(
			"These are the bytes being written to the object",
			f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Replicate the data object
		r = self.api.data_objects.replicate(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt",
			dst_resource="newresource",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that there are two replicas
		r = self.api.queries.execute_genquery("select DATA_NAME, DATA_REPL_NUM where DATA_NAME = 'file.txt'")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 2)

		try:
			# Calculate a checksum for the first replica
			r = self.api.data_objects.calculate_checksum(
				f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt",
				replica_number=0,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Verify checksum information across all replicas.
			r = self.api.data_objects.verify_checksum(f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		finally:
			# Remove the data objects
			r = self.api.data_objects.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt", 0, 1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Remove the resource
			r = self.api.resources.remove("newresource")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_touch(self):
		"""Test touch operation on data objects."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Test touching non existant data object with no_create
		r = self.api.data_objects.touch(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt", 1)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the object has not been created
		r = self.api.data_objects.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt")
		self.assertEqual(r["data"]["irods_response"]["status_code"], -171000)

		# Test touching non existant object without no_create
		r = self.api.data_objects.touch(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt", 0)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the object has been created
		r = self.api.data_objects.stat(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Test touching existing object without no_create
		r = self.api.data_objects.touch(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt", 1)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Remove the object
		r = self.api.data_objects.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_register(self):
		"""Test registering existing files as iRODS data objects."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a non-empty local file.
		filename = f"/{self.zone_name}/home/{self.rodsadmin_username}/register-demo.txt"
		content = "data"

		with pathlib.Path("/tmp/register-demo.txt").open("w") as f:  # noqa: S108
			f.write(content)

		# Show the data object we want to create via registration does not exist.
		r = self.api.data_objects.stat(filename)
		self.assertEqual(r["data"]["irods_response"]["status_code"], -171000)

		try:
			# Create a unixfilesystem resource.
			r = self.api.resources.create(
				"register_resource",
				"unixfilesystem",
				self.host,
				"/tmp/register_resource",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Register the local file into the catalog as a new data object.
			# We know we're registering a new data object because the "as-additional-replica"
			# parameter isn't set to 1.
			r = self.api.data_objects.register(
				filename,
				"/tmp/register-demo.txt",  # noqa: S108
				"register_resource",
				data_size=len(content),
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show a new data object exists with the expected replica information.
			r = self.api.queries.execute_genquery(
				"select DATA_NAME, DATA_PATH, RESC_NAME where DATA_NAME = 'register-demo.txt'"
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(len(r["data"]["rows"]), 1)
			self.assertEqual(r["data"]["rows"][0][1], "/tmp/register-demo.txt")  # noqa: S108
			self.assertEqual(r["data"]["rows"][0][2], "register_resource")

		finally:
			# Unregister the data object
			r = self.api.data_objects.remove(filename, 1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Remove the resource
			r = self.api.resources.remove("register_resource")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_parallel_write(self):
		"""Test parallel writing to data objects."""
		self.api.set_token(self.rodsadmin_bearer_token)
		self.api.data_objects.remove(f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt", 0, 1)

		# Open parallel write
		r = self.api.data_objects.parallel_write_init(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt", 3
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		handle = r["data"]["parallel_write_handle"]

		try:
			# Write to the data object using the parallel write handle.
			futures = []
			with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
				for e in enumerate(["A", "B", "C"]):
					count = 10
					futures.append(
						executor.submit(
							self.api.data_objects.write,
							bytes_=e[1] * count,
							offset=e[0] * count,
							stream_index=e[0],
							parallel_write_handle=handle,
						)
					)
				for f in concurrent.futures.as_completed(futures):
					r = f.result()
					self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		finally:
			# Close parallel write
			r = self.api.data_objects.parallel_write_shutdown(handle)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Remove the object
			r = self.api.data_objects.remove(
				f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt",
				0,
				1,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)


# Tests for resources operations
class ResourcesTests(unittest.TestCase):
	"""Test iRODS resource operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for resource tests."""
		setup_class(cls, {"endpoint_name": "resources"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_common_operations(self):
		"""Test common resource operations (create, list, stat, etc.)."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# TEMPORARY pre-test cleanup
		# test is currently not passing, so cleanup occurs at the beginning to allow it
		# to be run more than once in a row
		self.api.resources.remove_child("test_repl", "test_ufs0")
		self.api.resources.remove_child("test_repl", "test_ufs1")
		self.api.resources.remove("test_ufs0")
		self.api.resources.remove("test_ufs1")
		self.api.resources.remove("test_repl")

		resc_repl = "test_repl"
		resc_ufs0 = "test_ufs0"
		resc_ufs1 = "test_ufs1"

		# Create three resources (replication w/ two unixfilesystem resources).
		r = self.api.resources.create(resc_repl, "replication", "", "", "")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the replication resource was created.
		r = self.api.resources.stat(resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["exists"], True)
		self.assertIn("id", r["data"]["info"])
		self.assertEqual(r["data"]["info"]["name"], resc_repl)
		self.assertEqual(r["data"]["info"]["type"], "replication")
		self.assertEqual(r["data"]["info"]["zone"], "tempZone")
		self.assertEqual(r["data"]["info"]["host"], "EMPTY_RESC_HOST")
		self.assertEqual(r["data"]["info"]["vault_path"], "EMPTY_RESC_PATH")
		self.assertIn("status", r["data"]["info"])
		self.assertIn("context", r["data"]["info"])
		self.assertIn("comments", r["data"]["info"])
		self.assertIn("information", r["data"]["info"])
		self.assertIn("free_space", r["data"]["info"])
		self.assertIn("free_space_last_modified", r["data"]["info"])
		self.assertEqual(r["data"]["info"]["parent_id"], "")
		self.assertIn("created", r["data"]["info"])
		self.assertIn("last_modified", r["data"]["info"])
		self.assertIn("last_modified_millis", r["data"]["info"])

		# Capture the replication resource's id.
		# This resource is going to be the parent of the unixfilesystem resources.
		# This value is needed to verify the relationship.
		resc_repl_id = r["data"]["info"]["id"]

		for resc_name in [resc_ufs0, resc_ufs1]:
			with self.subTest(f"Create and attach resource [{resc_name}] to [{resc_repl}]"):
				vault_path = f"/tmp/{resc_name}_vault"  # noqa: S108

				# Create a unixfilesystem resource.
				r = self.api.resources.create(resc_name, "unixfilesystem", self.host, vault_path, "")
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Add the unixfilesystem resource as a child of the replication resource.
				r = self.api.resources.add_child(resc_repl, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Show that the resource was created and configured successfully.
				r = self.api.resources.stat(resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
				self.assertEqual(r["data"]["exists"], True)
				self.assertIn("id", r["data"]["info"])
				self.assertEqual(r["data"]["info"]["name"], resc_name)
				self.assertEqual(r["data"]["info"]["type"], "unixfilesystem")
				self.assertEqual(r["data"]["info"]["zone"], self.zone_name)
				self.assertEqual(r["data"]["info"]["host"], self.host)
				self.assertEqual(r["data"]["info"]["vault_path"], vault_path)
				self.assertIn("status", r["data"]["info"])
				self.assertIn("context", r["data"]["info"])
				self.assertIn("comments", r["data"]["info"])
				self.assertIn("information", r["data"]["info"])
				self.assertIn("free_space", r["data"]["info"])
				self.assertIn("free_space_last_modified", r["data"]["info"])
				self.assertEqual(r["data"]["info"]["parent_id"], resc_repl_id)
				self.assertIn("created", r["data"]["info"])
				self.assertIn("last_modified", r["data"]["info"])

		# Create a data object targeting the replication resource.
		data_object = f"/{self.zone_name}/home/{self.rodsadmin_username}/resource_obj"
		r = self.api.data_objects.write("These are the bytes to be written", data_object, resc_repl, 0)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show there are two replicas under the replication resource hierarchy.
		r = self.api.queries.execute_genquery(
			f"select DATA_NAME, RESC_NAME where DATA_NAME = '{pathlib.Path(data_object).name}'"
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 2)

		resc_tuple = (r["data"]["rows"][0][1], r["data"]["rows"][1][1])
		self.assertIn(resc_tuple, [(resc_ufs0, resc_ufs1), (resc_ufs1, resc_ufs0)])

		# Trim a replica.
		r = self.api.data_objects.trim(data_object, 0)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show there is only one replica under the replication resource hierarchy.
		r = self.api.queries.execute_genquery(
			f"select DATA_NAME, RESC_NAME where DATA_NAME = '{pathlib.Path(data_object).name}'"
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 1)

		# Launch rebalance
		r = self.api.resources.rebalance(resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Give the rebalance operation time to complete!
		time.sleep(3)

		#
		# Clean-up
		#

		# Remove the data object.
		r = self.api.data_objects.remove(data_object, 0, 1)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Remove resources.
		for resc_name in [resc_ufs0, resc_ufs1]:
			with self.subTest(f"Detach and remove resource [{resc_name}] from [{resc_repl}]"):
				# Detach ufs resource from the replication resource.
				r = self.api.resources.remove_child(resc_repl, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Remove ufs resource.
				r = self.api.resources.remove(resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Show that the resource no longer exists.
				r = self.api.resources.stat(resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
				self.assertEqual(r["data"]["exists"], False)

		# Remove replication resource.
		r = self.api.resources.remove(resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the resource no longer exists.
		r = self.api.resources.stat(resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["exists"], False)

	def test_modify_metadata(self):
		"""Test modifying metadata on resources."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a unixfilesystem resource.
		r = self.api.resources.create(
			"metadata_demo",
			"unixfilesystem",
			self.host,
			"/tmp/metadata_demo_vault",  # noqa: S108
			"",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		operations = [{"operation": "add", "attribute": "a1", "value": "v1", "units": "u1"}]

		# Add the metadata to the resource
		r = self.api.resources.modify_metadata("metadata_demo", operations)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the metadata is on the resource
		r = self.api.queries.execute_genquery(
			"select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and "
			"META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'"
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["rows"][0][0], "metadata_demo")

		# Remove the metadata from the resource.
		operations = [{"operation": "remove", "attribute": "a1", "value": "v1", "units": "u1"}]

		r = self.api.resources.modify_metadata("metadata_demo", operations)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the metadata is no longer on the resource
		r = self.api.queries.execute_genquery(
			"select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and "
			"META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'"
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 0)

		# Remove the resource
		r = self.api.resources.remove("metadata_demo")

	def test_modify_properties(self):
		"""Test modifying resource properties."""
		self.api.set_token(self.rodsadmin_bearer_token)

		resource = "properties_demo"

		# Create a new resource.
		r = self.api.resources.create(resource, "replication", "", "", "")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		try:
			# The list of updates to apply in sequence.
			property_map = [
				("name", "test_modifying_resource_properties_renamed"),
				("type", "passthru"),
				("host", "example.org"),
				("vault_path", "/tmp/test_modifying_resource_properties_vault"),  # noqa: S108
				("status", "down"),
				("status", "up"),
				("comments", "test_modifying_resource_properties_comments"),
				("information", "test_modifying_resource_properties_information"),
				("free_space", "test_modifying_resource_properties_free_space"),
				("context", "test_modifying_resource_properties_context"),
			]

			# Apply each update to the resource and verify that each one results
			# in the expected results.
			for p, v in property_map:
				with self.subTest(f"Setting property [{p}] to value [{v}]"):
					# Change a property of the resource.
					r = self.api.resources.modify(resource, p, v)

					# Make sure to update the "resource" variable following a successful rename.
					if p == "name":
						resource = v

					# Show the property was modified.
					r = self.api.resources.stat(resource)
					self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
					self.assertEqual(r["data"]["info"][p], v)
		finally:
			# Remove the resource
			r = self.api.resources.remove(resource)


# Tests for rule operations
class RulesTests(unittest.TestCase):
	"""Test iRODS rule operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for rule tests."""
		setup_class(cls, {"endpoint_name": "rules"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_list(self):
		"""Test listing rule engine plugins."""
		# Try listing rule engine plugins
		r = self.api.rules.list_rule_engines()

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertGreater(len(r["data"]["rule_engine_plugin_instances"]), 0)

	def test_execute_rule(self):
		"""Test executing iRODS rules."""
		test_msg = "This was run by the iRODS HTTP API test suite!"

		# Execute rule text against the iRODS rule language.
		r = self.api.rules.execute(
			f'writeLine("stdout", "{test_msg}")',
			"irods_rule_engine_plugin-irods_rule_language-instance",
		)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["stderr"], None)

		# The REP always appends a newline character to the result. While we could trim the result,
		# it is better to append a newline character to the expected result to guarantee things align.
		self.assertEqual(r["data"]["stdout"], test_msg + "\n")

	def test_remove_delay_rule(self):
		"""Test removing delayed execution rules."""
		rep_instance = "irods_rule_engine_plugin-irods_rule_language-instance"

		# Schedule a delay rule to execute in the distant future.
		r = self.api.rules.execute(
			f'delay("<INST_NAME>{rep_instance}</INST_NAME><PLUSET>1h</PLUSET>") '
			f'{{ writeLine("serverLog", "iRODS HTTP API"); }}',
			rep_instance,
		)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Find the delay rule we just created.
		# This query assumes the test suite is running on a system where no other delay
		# rules are being created.
		r = self.api.queries.execute_genquery("select max(RULE_EXEC_ID)")

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 1)

		# Remove the delay rule.
		r = self.api.rules.remove_delay_rule(int(r["data"]["rows"][0][0]))
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)


# Tests for query operations
class QueryTests(unittest.TestCase):
	"""Test iRODS query operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for query tests."""
		setup_class(cls, {"endpoint_name": "query"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_create_execute_remove_specific_query(self):
		"""Test creating, executing, and removing specific queries."""
		try:
			# As rodsadmin, create a specific query
			self.api.set_token(self.rodsadmin_bearer_token)

			name = "get_users_count"
			sql = "select count(*) from r_user_main"
			r = self.api.queries.add_specific_query(name=name, sql=sql)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Switch to rodsuser and execute it
			self.api.set_token(self.rodsuser_bearer_token)
			r = self.api.queries.execute_specific_query(name=name)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(r["data"]["rows"][0][0], "3")

		finally:
			# Switch to rodsadmin and remove it
			self.api.set_token(self.rodsadmin_bearer_token)
			r = self.api.queries.remove_specific_query(name=name)


# Tests for tickets operations
class TicketsTests(unittest.TestCase):
	"""Test iRODS ticket operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for ticket tests."""
		setup_class(cls, {"endpoint_name": "tickets"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_create_and_remove(self):
		"""Test creating and removing tickets."""
		self.api.set_token(self.rodsuser_bearer_token)

		# Create a write ticket.
		ticket_type = "write"
		ticket_path = f"/{self.zone_name}/home/{self.rodsuser_username}"
		ticket_use_count = 2000
		ticket_groups = "public"
		ticket_hosts = self.host
		r = self.api.tickets.create(
			ticket_path,
			ticket_type,
			use_count=ticket_use_count,
			seconds_until_expiration=3600,
			users="rods,jeb",
			groups=ticket_groups,
			hosts=ticket_hosts,
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		ticket_string = r["data"]["ticket"]
		self.assertGreater(len(ticket_string), 0)

		# Show the ticket exists and has the properties we defined during creation.
		# We can use GenQuery for this, but it does seem better to provide a convenience
		# operation for this.
		r = self.api.queries.execute_genquery(
			"select TICKET_STRING, TICKET_TYPE, TICKET_COLL_NAME, TICKET_USES_LIMIT, "
			"TICKET_ALLOWED_USER_NAME, TICKET_ALLOWED_GROUP_NAME, TICKET_ALLOWED_HOST"
		)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertIn(ticket_string, r["data"]["rows"][0])
		self.assertEqual(r["data"]["rows"][0][1], ticket_type)
		self.assertEqual(r["data"]["rows"][0][2], ticket_path)
		self.assertEqual(r["data"]["rows"][0][3], str(ticket_use_count))
		self.assertIn(r["data"]["rows"][0][4], ["rods", "jeb"])
		self.assertEqual(r["data"]["rows"][0][5], ticket_groups)
		self.assertGreater(len(r["data"]["rows"][0][6]), 0)

		# Remove the ticket.
		r = self.api.tickets.remove(ticket_string)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the ticket no longer exists.
		r = self.api.queries.execute_genquery("select TICKET_STRING")

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 0)


# Tests for user operations
class UserTests(unittest.TestCase):
	"""Test iRODS user and group operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for user tests."""
		setup_class(cls, {"endpoint_name": "users-groups"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_create_stat_and_remove_rodsuser(self):
		"""Test creating, querying, and removing rodsuser users."""
		self.api.set_token(self.rodsadmin_bearer_token)

		new_username = "test_user_rodsuser"
		user_type = "rodsuser"

		# Create a new user.
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = self.api.users_groups.stat(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_set_password(self):
		"""Test setting user passwords."""
		self.api.set_token(self.rodsadmin_bearer_token)

		new_username = "test_user_rodsuser"
		user_type = "rodsuser"

		# Create a new user.
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		new_password = "new_password"  # noqa: S105
		# Set a new password
		r = self.api.users_groups.set_password(new_username, self.zone_name, new_password)
		self.assertEqual(r["status_code"], 200)

		# Try to get a token for the user
		token = self.api.authenticate(new_username, new_password)
		self.assertIsInstance(token, str)

		# Remove the user.
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_create_stat_and_remove_rodsadmin(self):
		"""Test creating, querying, and removing rodsadmin users."""
		self.api.set_token(self.rodsadmin_bearer_token)

		new_username = "test_user_rodsadmin"
		user_type = "rodsadmin"

		# Create a new user.
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = self.api.users_groups.stat(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_create_stat_and_remove_groupadmin(self):
		"""Test creating, querying, and removing groupadmin users."""
		self.api.set_token(self.rodsadmin_bearer_token)

		new_username = "test_user_groupadmin"
		user_type = "groupadmin"

		# Create a new user.
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = self.api.users_groups.stat(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_add_remove_user_to_and_from_group(self):
		"""Test adding and removing users from groups."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a new group.
		new_group = "test_group"
		r = self.api.users_groups.create_group(new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Stat the group.
		r = self.api.users_groups.stat(new_group)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["type"], "rodsgroup")

		# Create a new user.
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Add user to group.
		r = self.api.users_groups.add_to_group(new_username, self.zone_name, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the user is a member of the group.
		r = self.api.users_groups.is_member_of_group(new_group, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertEqual(result["is_member"], True)

		# Remove user from group.
		r = self.api.users_groups.remove_from_group(new_username, self.zone_name, new_group)

		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Remove the user.
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		# Remove group.
		r = self.api.users_groups.remove_group(new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the group no longer exists.
		r = self.api.users_groups.stat(new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], False)

	def test_only_a_rodsadmin_can_change_the_type_of_a_user(self):
		"""Test that only rodsadmin users can change user type."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a new user.
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"
		r = self.api.users_groups.create_user(new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that a rodsadmin can change the type of the new user.
		new_user_type = "groupadmin"
		r = self.api.users_groups.set_user_type(new_username, self.zone_name, new_user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that a non-admin cannot change the type of the new user.
		self.api.set_token(self.rodsuser_bearer_token)
		r = self.api.users_groups.set_user_type(new_user_type, self.zone_name, new_user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], -13000)

		# Show that the user type matches the type set by the rodsadmin.
		r = self.api.users_groups.stat(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], new_user_type)

		# Remove the user.
		self.api.set_token(self.rodsadmin_bearer_token)
		r = self.api.users_groups.remove_user(new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_listing_all_users_in_zone(self):
		"""Test listing all users in the zone."""
		self.api.set_token(self.rodsuser_bearer_token)

		r = self.api.users_groups.users()
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertIn({"name": self.rodsadmin_username, "zone": self.zone_name}, result["users"])
		self.assertIn({"name": self.rodsuser_username, "zone": self.zone_name}, result["users"])

	def test_listing_all_groups_in_zone(self):
		"""Test listing all groups in the zone."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Create a new group.
		new_group = "test_group"
		r = self.api.users_groups.create_group(new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		self.api.set_token(self.rodsuser_bearer_token)
		# Get all groups.
		r = self.api.users_groups.groups()
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertIn("public", result["groups"])
		self.assertIn(new_group, result["groups"])

		self.api.set_token(self.rodsadmin_bearer_token)
		# Remove the new group.
		r = self.api.users_groups.remove_group(new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_modifying_metadata_atomically(self):
		"""Test atomically modifying user metadata."""
		self.api.set_token(self.rodsadmin_bearer_token)
		username = self.rodsuser_username

		# Add metadata to the user.
		ops = [{"operation": "add", "attribute": "a1", "value": "v1", "units": "u1"}]
		r = self.api.users_groups.modify_metadata(username, ops)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the metadata exists on the user.
		r = self.api.queries.execute_genquery(
			"select USER_NAME where META_USER_ATTR_NAME = 'a1' and "
			"META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'"
		)
		self.assertEqual(r["status_code"], 200)

		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertEqual(result["rows"][0][0], username)

		# Remove the metadata from the user.
		ops = [{"operation": "remove", "attribute": "a1", "value": "v1", "units": "u1"}]
		r = self.api.users_groups.modify_metadata(username, ops)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the metadata no longer exists on the user.
		r = self.api.queries.execute_genquery(
			"select USER_NAME where META_USER_ATTR_NAME = 'a1' and "
			"META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'"
		)
		self.assertEqual(r["status_code"], 200)

		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertEqual(len(result["rows"]), 0)


# Tests for zone operations
class ZoneTests(unittest.TestCase):
	"""Test iRODS zone operations."""

	@classmethod
	def setUpClass(cls):
		"""Set up class-level resources for zone tests."""
		setup_class(cls, {"endpoint_name": "zones"})

	@classmethod
	def tearDownClass(cls):
		"""Tear down class-level resources."""
		tear_down_class(cls)

	def setUp(self):
		"""Check that class initialization succeeded before each test."""
		self.assertFalse(self._class_init_error, "Class initialization failed. Cannot continue.")

	def test_report_operation(self):
		"""Test the zone report operation."""
		self.api.set_token(self.rodsadmin_bearer_token)
		r = self.api.zones.report()
		self.assertEqual(r["status_code"], 200)

		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)

		zone_report = result["zone_report"]
		self.assertIn("zones", zone_report)
		self.assertGreaterEqual(len(zone_report["zones"]), 1)
		self.assertIn("schema_version", zone_report["zones"][0]["servers"][0]["server_config"])

	def test_adding_removing_and_modifying_zones(self):
		"""Test adding, removing, and modifying zones."""
		self.api.set_token(self.rodsadmin_bearer_token)

		# Add a remote zone to the local zone.
		# The new zone will not have any connection information or anything else.
		zone_name = "other_zone"
		r = self.api.zones.add(zone_name)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		try:
			# Show the new zone exists by executing the stat operation on it.
			r = self.api.zones.stat(zone_name)
			self.assertEqual(r["status_code"], 200)

			result = r["data"]
			self.assertEqual(result["irods_response"]["status_code"], 0)
			self.assertEqual(result["exists"], True)
			self.assertEqual(result["info"]["name"], zone_name)
			self.assertEqual(result["info"]["type"], "remote")
			self.assertEqual(result["info"]["connection_info"], "")
			self.assertEqual(result["info"]["comment"], "")

			# The properties to update.
			property_map = [
				("name", "other_zone_renamed"),
				("connection_info", "example.org:1247"),
				("comment", "updated comment"),
			]

			# Change the properties of the new zone.
			for p, v in property_map:
				with self.subTest(f"Setting property [{p}] to value [{v}]"):
					r = self.api.zones.modify(zone_name, p, v)
					self.assertEqual(r["status_code"], 200)
					self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

					# Capture the new name of the zone following its renaming.
					if p == "name":
						zone_name = v

					# Show the new zone was modified successfully.
					r = self.api.zones.stat(zone_name)
					self.assertEqual(r["status_code"], 200)

					result = r["data"]
					self.assertEqual(result["irods_response"]["status_code"], 0)
					self.assertEqual(result["exists"], True)
					self.assertEqual(result["info"][p], v)

		finally:
			# Remove the remote zone.
			r = self.api.zones.remove(zone_name)
			self.assertEqual(r["status_code"], 200)


if __name__ == "__main__":
	unittest.main()
