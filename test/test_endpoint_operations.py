"""
Integration tests for iRODS HTTP API endpoint operations.

Tests cover all major operation categories: collections, data objects,
queries, resources, rules, tickets, users/groups, and zones.
"""

import concurrent.futures
import logging
import pathlib
import time
import unittest

import config

from irods_http import (
	authenticate,
	collections,
	common,
	data_objects,
	get_server_info,
	queries,
	resources,
	rules,
	tickets,
	users_groups,
	zones,
)


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

	cls.zone_name = config.test_config["irods_zone"]
	cls.host = config.test_config["irods_server_hostname"]

	# create_rodsuser cannot be honored if init_rodsadmin is set to False.
	# Therefore, return immediately.
	if not opts.get("init_rodsadmin", True):
		cls.logger.debug("init_rodsadmin is False. Class setup complete.")
		return

	# Authenticate as a rodsadmin and store the session.
	cls.rodsadmin_username = config.test_config["rodsadmin"]["username"]

	try:
		cls.rodsadmin_session = authenticate(
			cls.url_base, cls.rodsadmin_username, config.test_config["rodsadmin"]["password"]
		)
	except RuntimeError:
		cls._class_init_error = True
		cls.logger.debug("Failed to authenticate as rodsadmin [%].", cls.rodsadmin_username)
		return

	# Authenticate as a rodsuser and store the session.
	cls.rodsuser_username = config.test_config["rodsuser"]["username"]

	try:
		users_groups.create_user(cls.rodsadmin_session, cls.rodsuser_username, cls.zone_name, "rodsuser")
		users_groups.set_password(
			cls.rodsadmin_session,
			cls.rodsuser_username,
			cls.zone_name,
			config.test_config["rodsuser"]["password"],
		)
		cls.rodsuser_session = authenticate(
			cls.url_base, cls.rodsuser_username, config.test_config["rodsuser"]["password"]
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

	users_groups.remove_user(cls.rodsadmin_session, cls.rodsuser_username, cls.zone_name)


# Tests for library
class LibraryTests(unittest.TestCase):
	"""Test library-level operations."""

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
		get_server_info(self.rodsadmin_session)

	# tests the validators
	def test_validators(self):
		"""Test the validate functions in common."""
		self.assertRaises(ValueError, common.validate_not_none, None)
		self.assertRaises(ValueError, common.validate_gte_zero, -1)
		self.assertRaises(ValueError, common.validate_gte_minus1, -2)


# Tests for collections operations
class CollectionTests(unittest.TestCase):
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
		# clean up test collections
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test/folder")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test")

		# test param checking
		self.assertRaises(TypeError, collections.create, self.rodsadmin_session, 0, 0)
		self.assertRaises(
			TypeError,
			collections.create,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.create,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			7,
		)

		# test creating new collection
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		self.assertTrue(response["data"]["created"])
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test creating existing collection
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		self.assertFalse(response["data"]["created"])
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test invalid path
		response = collections.create(self.rodsadmin_session, f"{self.zone_name}/home/new")
		self.assertEqual(
			"{'irods_response': {'status_code': -358000, "
			"'status_message': 'path does not exist: OBJ_PATH_DOES_NOT_EXIST'}}",
			str(response["data"]),
		)

		# test create_intermediates
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/test/folder", 0)
		self.assertEqual(
			"{'irods_response': {'status_code': -358000, "
			"'status_message': 'path does not exist: OBJ_PATH_DOES_NOT_EXIST'}}",
			str(response["data"]),
		)
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/test/folder", 1)
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)

	# tests the remove operation
	def test_remove(self):
		"""Test collection removal operations and parameter validation."""
		# clean up test collections
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test/folder")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test")

		# test param checking
		self.assertRaises(TypeError, collections.remove, self.rodsadmin_session, 0, 0, 0)
		self.assertRaises(
			TypeError,
			collections.remove,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			0,
		)
		self.assertRaises(
			ValueError,
			collections.remove,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			collections.remove,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.remove,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			5,
		)

		# test removing collection
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)
		response = collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))
		# test invalid paths
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/tensaitekinaaidorusama")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/aremonainainaikoremonainainai")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/binglebangledingledangle")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test recurse
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/test/folder", 1)
		self.assertEqual(
			"{'created': True, 'irods_response': {'status_code': 0}}",
			str(response["data"]),
		)
		response = collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test", 0)
		self.assertEqual(
			"{'irods_response': {'status_code': -79000, "
			"'status_message': 'cannot remove non-empty collection: "
			"SYS_COLLECTION_NOT_EMPTY'}}",
			str(response["data"]),
		)
		response = collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/test", 1)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

	# tests the stat operation
	def test_stat(self):
		"""Test collection stat operation to retrieve metadata."""
		# clean up test collections
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/new")

		# test param checking
		self.assertRaises(TypeError, collections.stat, self.rodsadmin_session, 0, "ticket")
		self.assertRaises(
			TypeError,
			collections.stat,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
		)

		# test invalid paths
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"{self.zone_name}/home/new")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test valid path
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["permissions"])

	# tests the list operation
	def test_list(self):
		"""Test collection list operation to enumerate contents."""
		# clean up test collections
		collections.remove(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb",
		)
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/albania")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia")
		collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia")

		# test param checking
		self.assertRaises(TypeError, collections.list_collection, self.rodsadmin_session, 0, "ticket")
		self.assertRaises(
			TypeError,
			collections.list_collection,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			"ticket",
		)
		self.assertRaises(
			ValueError,
			collections.list_collection,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			"ticket",
		)
		self.assertRaises(
			TypeError,
			collections.list_collection,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			0,
		)

		# test empty collection
		response = collections.list_collection(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}"
		)
		self.assertEqual("None", str(response["data"]["entries"]))

		# test collection with one item
		collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia")
		response = collections.list_collection(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}"
		)
		self.assertEqual(
			f"/{self.zone_name}/home/{self.rodsadmin_username}/bosnia",
			str(response["data"]["entries"][0]),
		)

		# test collection with multiple items
		collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/albania")
		collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia")
		response = collections.list_collection(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}"
		)
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
		collections.create(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}/croatia/zagreb",
		)
		response = collections.list_collection(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}"
		)
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
		response = collections.list_collection(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}", 1
		)
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
		# test param checking
		self.assertRaises(TypeError, collections.set_permission, self.rodsadmin_session, 0, "jeb", "read", 0)
		self.assertRaises(
			TypeError,
			collections.set_permission,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"read",
			0,
		)
		self.assertRaises(
			TypeError,
			collections.set_permission,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			0,
			0,
		)
		self.assertRaises(
			ValueError,
			collections.set_permission,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			"badperm",
			0,
		)
		self.assertRaises(
			TypeError,
			collections.set_permission,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			"read",
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.set_permission,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"jeb",
			"read",
			5,
		)

		# create new collection
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/setPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test no permission
		response = collections.stat(self.rodsuser_session, f"/{self.zone_name}/home/setPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], -170000)

		# test set permission
		response = collections.set_permission(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/setPerms",
			self.rodsuser_username,
			"read",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test with permission
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/setPerms")
		self.assertTrue(response["data"]["permissions"])

		# test set permission null
		response = collections.set_permission(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/setPerms",
			self.rodsuser_username,
			"null",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test no permission
		response = collections.stat(self.rodsuser_session, f"/{self.zone_name}/home/setPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# remove the collection
		response = collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/setPerms", 1, 1)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# tests the set inheritance operation
	def test_set_inheritance(self):
		"""Test setting inheritance for collection permissions."""
		# test param checking
		self.assertRaises(TypeError, collections.set_inheritance, self.rodsadmin_session, 0, 0, 0)
		self.assertRaises(
			TypeError,
			collections.set_inheritance,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			"0",
			0,
		)
		self.assertRaises(
			ValueError,
			collections.set_inheritance,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			collections.set_inheritance,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.set_inheritance,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
			5,
		)

		# control
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertFalse(response["data"]["inheritance_enabled"])

		# test enabling inheritance
		response = collections.set_inheritance(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}", 1
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# check if changed
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["inheritance_enabled"])

		# test disabling inheritance
		response = collections.set_inheritance(
			self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}", 0
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# check if changed
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertFalse(response["data"]["inheritance_enabled"])

	# test the modify permissions operation
	def test_modify_permissions(self):
		"""Test modifying permissions on collections."""
		ops_permissions = [{"entity_name": self.rodsuser_username, "acl": "read"}]

		ops_permissions_null = [{"entity_name": self.rodsuser_username, "acl": "null"}]

		# test param checking
		self.assertRaises(TypeError, collections.modify_permissions, self.rodsadmin_session, 0, ops_permissions, 0)
		self.assertRaises(
			TypeError,
			collections.modify_permissions,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			collections.modify_permissions,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_permissions,
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.modify_permissions,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_permissions,
			5,
		)

		# create new collection
		response = collections.create(self.rodsadmin_session, f"/{self.zone_name}/home/modPerms")
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test no permissions
		response = collections.stat(self.rodsuser_session, f"/{self.zone_name}/home/modPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# test set permissions
		response = collections.modify_permissions(
			self.rodsadmin_session, f"/{self.zone_name}/home/modPerms", ops_permissions
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test with permissions
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/modPerms")
		self.assertTrue(response["data"]["permissions"])

		# test set permissions nuil
		response = collections.modify_permissions(
			self.rodsadmin_session, f"/{self.zone_name}/home/modPerms", ops_permissions_null
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

		# test without permissions
		response = collections.stat(self.rodsuser_session, f"/{self.zone_name}/home/modPerms")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))

		# remove the collection
		response = collections.remove(self.rodsadmin_session, f"/{self.zone_name}/home/modPerms", 1, 1)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# test the modify metadata operation
	def test_modify_metadata(self):
		"""Test modifying metadata on collections."""
		ops_metadata = [{"operation": "add", "attribute": "eyeballs", "value": "itchy"}]

		ops_metadata_remove = [{"operation": "remove", "attribute": "eyeballs", "value": "itchy"}]

		# test param checking
		self.assertRaises(TypeError, collections.modify_metadata, self.rodsadmin_session, 0, ops_metadata, 0)
		self.assertRaises(
			TypeError,
			collections.modify_metadata,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			5,
			0,
		)
		self.assertRaises(
			TypeError,
			collections.modify_metadata,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata,
			"0",
		)
		self.assertRaises(
			ValueError,
			collections.modify_metadata,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata,
			5,
		)

		# test adding and removing metadata
		response = collections.modify_metadata(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata,
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)
		response = collections.modify_metadata(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			ops_metadata_remove,
		)
		self.assertEqual(response["data"]["irods_response"]["status_code"], 0)

	# tests the rename operation
	def test_rename(self):
		"""Test renaming collections."""
		# test param checking
		self.assertRaises(
			TypeError,
			collections.rename,
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			0,
		)
		self.assertRaises(TypeError, collections.rename, 0, f"/{self.zone_name}/home/pods")

		# test before move
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/pods")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertTrue(response["data"]["permissions"])

		# test renaming
		response = collections.rename(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			f"/{self.zone_name}/home/pods",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

		# test before move
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/{self.rodsadmin_username}")
		self.assertEqual("{'irods_response': {'status_code': -170000}}", str(response["data"]))
		response = collections.stat(self.rodsadmin_session, f"/{self.zone_name}/home/pods")
		self.assertTrue(response["data"]["permissions"])

		# test renaming
		response = collections.rename(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/pods",
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
		)
		self.assertEqual("{'irods_response': {'status_code': 0}}", str(response["data"]))

	# tests the touch operation
	def test_touch(self):
		"""Test touch operation to update collection timestamps."""
		collections.touch(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			reference=f"/{self.zone_name}/home",
		)
		collections.touch(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}",
			seconds_since_epoch=9000,
		)


# Tests for data object operations
class DataObjectTests(unittest.TestCase):
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

	def test_bad_write(self):
		"""Test writing non-bytes does not work."""
		# Exercise a bad write
		f = f"/{self.zone_name}/home/{self.rodsuser_username}/bad_write.txt"
		self.assertRaises(TypeError, data_objects.write, self.rodsuser_session, 4, f)

	def test_empty_write(self):
		"""Test writing an empty string works."""
		try:
			# Exercise an empty write
			f = f"/{self.zone_name}/home/{self.rodsuser_username}/empty_write.txt"
			r = data_objects.write(self.rodsuser_session, "", f)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			data_objects.remove(self.rodsuser_session, f, no_trash=1)

	def test_common_operations(self):
		"""Test common data object operations (write, read, copy, replicate, etc.)."""
		f1 = f"/{self.zone_name}/home/{self.rodsuser_username}/f1.txt"
		f2 = f"/{self.zone_name}/home/{self.rodsuser_username}/f2.txt"
		f3 = f"/{self.zone_name}/home/{self.rodsuser_username}/f3.txt"
		resc = "resource"

		try:
			# Create a unixfilesystem resource
			r = resources.create(
				self.rodsadmin_session,
				resc,
				"unixfilesystem",
				self.host,
				"/tmp/resource",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Create a non-empty data object
			r = data_objects.write(self.rodsuser_session, "These are the bytes being written to the object", f1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Read the data object
			r = data_objects.read(self.rodsuser_session, f1, offset=6, count=13)
			self.assertEqual(r["status_code"], 200)
			self.assertNotIn('There ', r["data"].decode('utf-8'))
			self.assertEqual('are the bytes', r["data"].decode('utf-8'))

			# Add metadata to the data object
			r = data_objects.modify_metadata(
				self.rodsuser_session,
				f1,
				operations=[{'operation': 'add', 'attribute': 'a', 'value': 'v', 'units': 'u'}],
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Modify the replica
			r = data_objects.modify_replica(self.rodsadmin_session, f1, replica_number=0, new_data_comments="awesome")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Replicate the data object
			r = data_objects.replicate(
				self.rodsuser_session,
				f1,
				src_resource="demoResc",
				dst_resource=resc,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show that there are two replicas
			r = queries.execute_genquery(
				self.rodsuser_session,
				f"select DATA_NAME, DATA_REPL_NUM where DATA_NAME = '{f1.rsplit('/', maxsplit=1)[-1]}'",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(len(r["data"]["rows"]), 2)

			# Trim the data object
			r = data_objects.trim(self.rodsuser_session, f1, replica_number=0)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Rename the data object
			r = data_objects.rename(self.rodsuser_session, f1, f2)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Copy the data object
			r = data_objects.copy(self.rodsuser_session, f2, f3)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Copy the data object again with parameters
			r = data_objects.copy(
				self.rodsuser_session, f2, f3, src_resource=resc, dst_resource="demoResc", overwrite=1
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Exercise a bad permission
			self.assertRaises(ValueError, data_objects.set_permission, self.rodsuser_session, f3, "rods", "bad")

			# Set permission on the object
			r = data_objects.set_permission(
				self.rodsuser_session,
				f3,
				"rods",
				"read",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Confirm that the permission has been set
			r = data_objects.stat(self.rodsuser_session, f3)
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
			r = data_objects.modify_permissions(
				self.rodsuser_session, f3, operations=[{'entity_name': 'rods', 'acl': 'write'}]
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			# Remove the data objects
			r = data_objects.remove(self.rodsuser_session, f1, 0, 1)

			r = data_objects.remove(self.rodsuser_session, f2, 0, 1)

			r = data_objects.remove(self.rodsuser_session, f3, 0, 1)

			# Remove the resource
			r = resources.remove(self.rodsadmin_session, resc)

	def test_modify_replica(self):
		"""Test modify replica options."""
		f = f"/{self.zone_name}/home/{self.rodsadmin_username}/modify-replica-test.txt"

		try:
			# Create a data object
			r = data_objects.write(self.rodsadmin_session, "some words", f)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Save the physical path
			r = queries.execute_genquery(
				self.rodsadmin_session, "SELECT DATA_PATH where DATA_NAME = 'modify-replica-test.txt'"
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			phypath = r["data"]["rows"][0][0]

			# Save the resource id
			r = queries.execute_genquery(self.rodsadmin_session, "SELECT RESC_ID where RESC_NAME = 'demoResc'")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			rescid = int(r["data"]["rows"][0][0])

			# Exercise modify replica error, incompatible params
			self.assertRaises(
				ValueError,
				data_objects.modify_replica,
				self.rodsadmin_session,
				f,
				replica_number=0,
				resource_hierarchy="demoResc",
			)

			# Exercise modify replica error, no new data
			self.assertRaises(
				RuntimeError,
				data_objects.modify_replica,
				self.rodsadmin_session,
				f,
			)

			# Modify the replica
			r = data_objects.modify_replica(
				self.rodsadmin_session,
				f,
				resource_hierarchy="demoResc",
				new_data_checksum="not a real checksum",
				new_data_create_time="1000",
				new_data_expiry="3000",
				new_data_mode="greatmode",
				new_data_modify_time="2000",
				new_data_path="/tmp/deleteme",  # noqa: S108
				new_data_replica_number=5,
				new_data_replica_status=0,
				new_data_resource_id=rescid,
				new_data_size=50,
				new_data_status="warm",
				new_data_type_name="html",
				new_data_version=3,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Restore the physical path so cleanup succeeds
			r = data_objects.modify_replica(
				self.rodsadmin_session,
				f,
				resource_hierarchy="demoResc",
				new_data_path=phypath,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			data_objects.remove(self.rodsadmin_session, f, no_trash=1)

	def test_checksums(self):
		"""Test checksum calculation and verification for data objects."""
		# Create a unixfilesystem resource.
		r = resources.create(
			self.rodsadmin_session,
			"newresource",
			"unixfilesystem",
			self.host,
			"/tmp/newresource",  # noqa: S108
			"",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		f = f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt"
		# Create a non-empty data object
		r = data_objects.write(
			self.rodsadmin_session,
			"These are the bytes being written to the object",
			f,
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Replicate the data object
		r = data_objects.replicate(
			self.rodsadmin_session,
			f,
			dst_resource="newresource",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that there are two replicas
		r = queries.execute_genquery(
			self.rodsadmin_session, "select DATA_NAME, DATA_REPL_NUM where DATA_NAME = 'file.txt'"
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 2)

		try:
			# Calculate a checksum for the first replica
			r = data_objects.calculate_checksum(
				self.rodsadmin_session,
				f,
				replica_number=0,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Calculate a checksum for the second replica
			r = data_objects.calculate_checksum(
				self.rodsadmin_session,
				f,
				resource="newresource",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Verify checksum on first replica
			r = data_objects.verify_checksum(
				self.rodsadmin_session,
				f,
				replica_number=0,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Verify checksum on second replica
			r = data_objects.verify_checksum(
				self.rodsadmin_session,
				f,
				resource="newresource",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			# Remove the data objects
			data_objects.remove(
				self.rodsadmin_session,
				f"/{self.zone_name}/home/{self.rodsadmin_username}/file.txt",
				catalog_only=0,
				no_trash=1,
			)

			# Remove the resource
			resources.remove(self.rodsadmin_session, "newresource")

	def test_touch(self):
		"""Test touch operation on data objects."""
		f = f"/{self.zone_name}/home/{self.rodsadmin_username}/new.txt"

		try:
			# Test touching non existant data object with no_create
			r = data_objects.touch(self.rodsadmin_session, f, no_create=1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show that the object has not been created
			r = data_objects.stat(self.rodsadmin_session, f)
			self.assertEqual(r["data"]["irods_response"]["status_code"], -171000)

			# Test touching non existant object without no_create
			r = data_objects.touch(self.rodsadmin_session, f, no_create=0)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show that the object has been created
			r = data_objects.stat(self.rodsadmin_session, f)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Test touching existing object without no_create
			r = data_objects.touch(self.rodsadmin_session, f, no_create=0)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Test parameter options
			r = data_objects.touch(self.rodsadmin_session, f, seconds_since_epoch=5000)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			r = data_objects.stat(self.rodsadmin_session, f)
			self.assertEqual(r["data"]["modified_at"], 5000)

			r = data_objects.touch(self.rodsadmin_session, f, replica_number=0)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			r = data_objects.touch(self.rodsadmin_session, f, leaf_resources="demoResc")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			r = data_objects.touch(self.rodsadmin_session, f, reference=f)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		finally:
			# Remove the object
			data_objects.remove(self.rodsadmin_session, f)

	def test_register(self):
		"""Test registering existing files as iRODS data objects."""
		filename = f"/{self.zone_name}/home/{self.rodsadmin_username}/register-demo.txt"

		# Show the data object we want to create via registration does not exist.
		r = data_objects.stat(self.rodsadmin_session, filename)
		self.assertEqual(r["data"]["irods_response"]["status_code"], -171000)

		try:
			# Create a unixfilesystem resource.
			r = resources.create(
				self.rodsadmin_session,
				"register_resource",
				"unixfilesystem",
				self.host,
				"/tmp/register_resource",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Create a non-empty data object.
			content = "bytes in the server"
			r = data_objects.write(self.rodsadmin_session, content, filename)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Query and save the physical path on the server.
			r = queries.execute_genquery(
				self.rodsadmin_session, "SELECT DATA_PATH where DATA_NAME = 'register-demo.txt'"
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			phyfile = r["data"]["rows"][0][0]

			# Unregister the logical path to leave the physical file on the server.
			r = data_objects.remove(self.rodsadmin_session, filename, 1)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Register the leftover local file into the catalog as a new data object.
			# We know we're registering a new data object because the "as-additional-replica"
			# parameter isn't set to 1.
			r = data_objects.register(
				self.rodsadmin_session,
				filename,
				phyfile,
				"register_resource",
				data_size=len(content),
				checksum=1,
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show a new data object exists with the expected replica information.
			r = queries.execute_genquery(
				self.rodsadmin_session,
				"select DATA_NAME, DATA_PATH, DATA_CHECKSUM, RESC_NAME where DATA_NAME = 'register-demo.txt'",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(len(r["data"]["rows"]), 1)
			self.assertEqual(r["data"]["rows"][0][1], phyfile)
			self.assertNotEqual(r["data"]["rows"][0][2], "")
			self.assertEqual(r["data"]["rows"][0][3], "register_resource")

		finally:
			# Unregister the data object
			data_objects.remove(self.rodsadmin_session, filename, 1)

			# Remove the resource
			resources.remove(self.rodsadmin_session, "register_resource")

	def test_parallel_write(self):
		"""Test parallel writing to data objects."""
		data_objects.remove(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt",
			0,
			1,
		)

		# Open parallel write
		r = data_objects.parallel_write_init(
			self.rodsadmin_session,
			f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt",
			3,
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
							data_objects.write,
							self.rodsadmin_session,
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
			data_objects.parallel_write_shutdown(self.rodsadmin_session, handle)

			# Remove the object
			data_objects.remove(
				self.rodsadmin_session,
				f"/{self.zone_name}/home/{self.rodsadmin_username}/parallel-write.txt",
				0,
				1,
			)


# Tests for resources operations
class ResourceTests(unittest.TestCase):
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
		# TEMPORARY pre-test cleanup
		# test is currently not passing, so cleanup occurs at the beginning to allow it
		# to be run more than once in a row
		resources.remove_child(self.rodsadmin_session, "test_repl", "test_ufs0")
		resources.remove_child(self.rodsadmin_session, "test_repl", "test_ufs1")
		resources.remove(self.rodsadmin_session, "test_ufs0")
		resources.remove(self.rodsadmin_session, "test_ufs1")
		resources.remove(self.rodsadmin_session, "test_repl")

		resc_repl = "test_repl"
		resc_ufs0 = "test_ufs0"
		resc_ufs1 = "test_ufs1"

		# Create three resources (replication w/ two unixfilesystem resources).
		r = resources.create(self.rodsadmin_session, resc_repl, "replication", "", "", "")
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the replication resource was created.
		r = resources.stat(self.rodsadmin_session, resc_repl)
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
				r = resources.create(self.rodsadmin_session, resc_name, "unixfilesystem", self.host, vault_path, "")
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Add the unixfilesystem resource as a child of the replication resource.
				r = resources.add_child(self.rodsadmin_session, resc_repl, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Show that the resource was created and configured successfully.
				r = resources.stat(self.rodsadmin_session, resc_name)
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
		r = data_objects.write(self.rodsadmin_session, "These are the bytes to be written", data_object, resc_repl, 0)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show there are two replicas under the replication resource hierarchy.
		r = queries.execute_genquery(
			self.rodsadmin_session,
			f"select DATA_NAME, RESC_NAME where DATA_NAME = '{pathlib.Path(data_object).name}'",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 2)

		resc_tuple = (r["data"]["rows"][0][1], r["data"]["rows"][1][1])
		self.assertIn(resc_tuple, [(resc_ufs0, resc_ufs1), (resc_ufs1, resc_ufs0)])

		# Trim a replica.
		r = data_objects.trim(self.rodsadmin_session, data_object, 0)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show there is only one replica under the replication resource hierarchy.
		r = queries.execute_genquery(
			self.rodsadmin_session,
			f"select DATA_NAME, RESC_NAME where DATA_NAME = '{pathlib.Path(data_object).name}'",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 1)

		# Launch rebalance
		r = resources.rebalance(self.rodsadmin_session, resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Give the rebalance operation time to complete!
		time.sleep(3)

		#
		# Clean-up
		#

		# Remove the data object.
		r = data_objects.remove(self.rodsadmin_session, data_object, 0, 1)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Remove resources.
		for resc_name in [resc_ufs0, resc_ufs1]:
			with self.subTest(f"Detach and remove resource [{resc_name}] from [{resc_repl}]"):
				# Detach ufs resource from the replication resource.
				r = resources.remove_child(self.rodsadmin_session, resc_repl, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Remove ufs resource.
				r = resources.remove(self.rodsadmin_session, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

				# Show that the resource no longer exists.
				r = resources.stat(self.rodsadmin_session, resc_name)
				self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
				self.assertEqual(r["data"]["exists"], False)

		# Remove replication resource.
		r = resources.remove(self.rodsadmin_session, resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the resource no longer exists.
		r = resources.stat(self.rodsadmin_session, resc_repl)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["exists"], False)

	def test_modify_failures(self):
		"""Test modifying resources, poorly."""
		badresc = "badresc"
		try:
			# Create a unixfilesystem resource.
			r = resources.create(
				self.rodsadmin_session,
				badresc,
				"unixfilesystem",
				self.host,
				"/tmp/badresc_vault",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Exercise bad modify property
			self.assertRaises(ValueError, resources.modify, self.rodsadmin_session, badresc, "badoption", "2")

			# Exercise bad modify status
			self.assertRaises(ValueError, resources.modify, self.rodsadmin_session, badresc, "status", "nope")

		finally:
			resources.remove(self.rodsadmin_session, badresc)

	def test_add_child_context(self):
		"""Test adding child resource context."""
		resc = 'thechild'
		try:
			# Create a unixfilesystem resource.
			r = resources.create(
				self.rodsadmin_session,
				resc,
				"unixfilesystem",
				self.host,
				"/tmp/resc_vault",  # noqa: S108
				"",
			)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Exercise add child with context
			r = resources.add_child(self.rodsadmin_session, "demoResc", resc, context="neat")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Confirm
			r = resources.stat(self.rodsadmin_session, resc)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			# Uncomment once parent_context is available
			# https://github.com/irods/irods_client_http_api/issues/473
		# self.assertEqual(r["data"]["info"]["parent_context"], "neat")

		finally:
			resources.remove_child(self.rodsadmin_session, "demoResc", resc)
			resources.remove(self.rodsadmin_session, resc)

	def test_modify_metadata(self):
		"""Test modifying metadata on resources."""
		# Create a unixfilesystem resource.
		r = resources.create(
			self.rodsadmin_session,
			"metadata_demo",
			"unixfilesystem",
			self.host,
			"/tmp/metadata_demo_vault",  # noqa: S108
			"ignoreme",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		operations = [{"operation": "add", "attribute": "a1", "value": "v1", "units": "u1"}]

		# Add the metadata to the resource
		r = resources.modify_metadata(self.rodsadmin_session, "metadata_demo", operations)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the metadata is on the resource
		r = queries.execute_genquery(
			self.rodsadmin_session,
			"select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and "
			"META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(r["data"]["rows"][0][0], "metadata_demo")

		# Remove the metadata from the resource.
		operations = [{"operation": "remove", "attribute": "a1", "value": "v1", "units": "u1"}]

		r = resources.modify_metadata(self.rodsadmin_session, "metadata_demo", operations)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the metadata is no longer on the resource
		r = queries.execute_genquery(
			self.rodsadmin_session,
			"select RESC_NAME where META_RESC_ATTR_NAME = 'a1' and "
			"META_RESC_ATTR_VALUE = 'v1' and META_RESC_ATTR_UNITS = 'u1'",
		)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 0)

		# Remove the resource
		r = resources.remove(self.rodsadmin_session, "metadata_demo")

	def test_modify_properties(self):
		"""Test modifying resource properties."""
		resource = "properties_demo"

		# Create a new resource.
		r = resources.create(self.rodsadmin_session, resource, "replication", "", "", "")
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
					r = resources.modify(self.rodsadmin_session, resource, p, v)

					# Make sure to update the "resource" variable following a successful rename.
					if p == "name":
						resource = v

					# Show the property was modified.
					r = resources.stat(self.rodsadmin_session, resource)
					self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
					self.assertEqual(r["data"]["info"][p], v)
		finally:
			# Remove the resource
			r = resources.remove(self.rodsadmin_session, resource)


# Tests for rule operations
class RuleTests(unittest.TestCase):
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
		r = rules.list_rule_engines(
			self.rodsadmin_session,
		)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertGreater(len(r["data"]["rule_engine_plugin_instances"]), 0)

	def test_execute_rule(self):
		"""Test executing iRODS rules."""
		test_msg = "This was run by the iRODS HTTP API test suite!"

		# Execute rule text against the iRODS rule language.
		r = rules.execute(
			self.rodsadmin_session,
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
		r = rules.execute(
			self.rodsadmin_session,
			f'delay("<INST_NAME>{rep_instance}</INST_NAME><PLUSET>1h</PLUSET>") '
			f'{{ writeLine("serverLog", "iRODS HTTP API"); }}',
			rep_instance,
		)

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Find the delay rule we just created.
		# This query assumes the test suite is running on a system where no other delay
		# rules are being created.
		r = queries.execute_genquery(self.rodsadmin_session, "select max(RULE_EXEC_ID)")

		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		self.assertEqual(len(r["data"]["rows"]), 1)

		# Remove the delay rule.
		r = rules.remove_delay_rule(self.rodsadmin_session, int(r["data"]["rows"][0][0]))
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

	def test_bad_query_type(self):
		"""Test with a query type that does not exist."""
		self.assertRaises(
			ValueError, queries.execute_genquery, self.rodsadmin_session, "SELECT ZONE_NAME", parser="bad"
		)

	def test_query_parameters(self):
		"""Test with queries that exercise the options."""
		# genquery
		queries.execute_genquery(self.rodsadmin_session, "SELECT ZONE_NAME", count=1)
		queries.execute_genquery(self.rodsadmin_session, "SELECT ZONE_NAME", zone=self.zone_name)
		queries.execute_genquery(self.rodsadmin_session, "SELECT ZONE_NAME", parser="genquery2", sql_only=1)

		# specific query
		queries.execute_specific_query(self.rodsadmin_session, "ls", count=1)
		queries.execute_specific_query(self.rodsadmin_session, "lsl", args="ls")

	def test_create_execute_remove_specific_query(self):
		"""Test creating, executing, and removing specific queries."""
		try:
			# As rodsadmin, create a specific query

			name = "get_users_count"
			sql = "select count(*) from r_user_main"
			r = queries.add_specific_query(self.rodsadmin_session, name=name, sql=sql)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Switch to rodsuser and execute it
			r = queries.execute_specific_query(self.rodsuser_session, name=name)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertEqual(r["data"]["rows"][0][0], "3")

		finally:
			# Switch to rodsadmin and remove it
			r = queries.remove_specific_query(self.rodsadmin_session, name=name)


# Tests for tickets operations
class TicketTests(unittest.TestCase):
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

	def test_create_failures(self):
		"""Test ticket create failure modes."""
		p = f"/{self.zone_name}/home/{self.rodsuser_username}"
		# bad type
		self.assertRaises(ValueError, tickets.create, self.rodsadmin_session, p, type_="bad")

		# bad object count
		self.assertRaises(
			ValueError,
			tickets.create,
			self.rodsadmin_session,
			p,
			type_="write",
			write_data_object_count=-5,
		)

		# bad byte count
		self.assertRaises(
			ValueError,
			tickets.create,
			self.rodsadmin_session,
			p,
			type_="write",
			write_byte_count=-2,
		)

	def test_create_and_remove(self):
		"""Test creating and removing tickets."""
		try:
			# Create a write ticket.
			ticket_type = "write"
			ticket_path = f"/{self.zone_name}/home/{self.rodsuser_username}"
			ticket_use_count = 2000
			ticket_write_data_object_count = 4
			ticket_write_byte_count = 1000
			ticket_groups = "public"
			ticket_hosts = self.host
			r = tickets.create(
				self.rodsuser_session,
				ticket_path,
				ticket_type,
				use_count=ticket_use_count,
				write_data_object_count=ticket_write_data_object_count,
				write_byte_count=ticket_write_byte_count,
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
			r = queries.execute_genquery(
				self.rodsadmin_session,
				"select TICKET_STRING, TICKET_TYPE, TICKET_COLL_NAME, TICKET_USES_LIMIT, "
				"TICKET_WRITE_FILE_LIMIT, TICKET_WRITE_BYTE_LIMIT, "
				"TICKET_ALLOWED_USER_NAME, TICKET_ALLOWED_GROUP_NAME, TICKET_ALLOWED_HOST",
			)

			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
			self.assertIn(ticket_string, r["data"]["rows"][0])
			self.assertEqual(r["data"]["rows"][0][1], ticket_type)
			self.assertEqual(r["data"]["rows"][0][2], ticket_path)
			self.assertEqual(r["data"]["rows"][0][3], str(ticket_use_count))
			self.assertEqual(r["data"]["rows"][0][4], str(ticket_write_data_object_count))
			self.assertEqual(r["data"]["rows"][0][5], str(ticket_write_byte_count))
			self.assertIn(r["data"]["rows"][0][6], ["rods", "jeb"])
			self.assertEqual(r["data"]["rows"][0][7], ticket_groups)
			self.assertGreater(len(r["data"]["rows"][0][6]), 0)

		finally:
			# Remove the ticket.
			tickets.remove(self.rodsuser_session, ticket_string)


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

	def test_create_with_bad_type(self):
		"""Test user create with bad type."""
		self.assertRaises(
			ValueError, users_groups.create_user, self.rodsadmin_session, "baduser", self.zone_name, user_type="bad"
		)

	def test_set_to_bad_type(self):
		"""Test setting user type to bad value."""
		self.assertRaises(
			ValueError, users_groups.set_user_type, self.rodsadmin_session, "baduser", self.zone_name, user_type="bad"
		)

	def test_create_stat_and_remove_rodsuser(self):
		"""Test creating, querying, and removing rodsuser users."""
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"

		# Create a new user.
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = users_groups.stat(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_empty_username(self):
		"""Test authenticate with an empty username."""
		self.assertRaises(ValueError, authenticate, self.url_base, "", "nope")

	def test_bad_password(self):
		"""Test authenticate with a bad password."""
		self.assertRaises(RuntimeError, authenticate, self.url_base, self.rodsadmin_username, "nope")

	def test_set_password(self):
		"""Test setting user passwords."""
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"

		# Create a new user.
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		new_password = "new_password"  # noqa: S105
		# Set a new password
		r = users_groups.set_password(self.rodsadmin_session, new_username, self.zone_name, new_password)
		self.assertEqual(r["status_code"], 200)

		# Try to get a token for the user
		session = authenticate(self.url_base, new_username, new_password)
		self.assertIsInstance(session.token, str)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_create_stat_and_remove_rodsadmin(self):
		"""Test creating, querying, and removing rodsadmin users."""
		new_username = "test_user_rodsadmin"
		user_type = "rodsadmin"

		# Create a new user.
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = users_groups.stat(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_create_stat_and_remove_groupadmin(self):
		"""Test creating, querying, and removing groupadmin users."""
		new_username = "test_user_groupadmin"
		user_type = "groupadmin"

		# Create a new user.
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Stat the user.
		r = users_groups.stat(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], user_type)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

	def test_add_remove_user_to_and_from_group(self):
		"""Test adding and removing users from groups."""
		# Create a new group.
		new_group = "test_group"
		r = users_groups.create_group(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Stat the group.
		r = users_groups.stat(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertIn("id", stat_info)
		self.assertEqual(stat_info["type"], "rodsgroup")

		# Create a new user.
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)

		# Add user to group.
		r = users_groups.add_to_group(self.rodsadmin_session, new_username, self.zone_name, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the user is a member of the group.
		r = users_groups.is_member_of_group(self.rodsadmin_session, new_group, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertEqual(result["is_member"], True)

		# Remove user from group.
		r = users_groups.remove_from_group(self.rodsadmin_session, new_username, self.zone_name, new_group)

		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		# Remove group.
		r = users_groups.remove_group(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that the group no longer exists.
		r = users_groups.stat(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], False)

	def test_only_a_rodsadmin_can_change_the_type_of_a_user(self):
		"""Test that only rodsadmin users can change user type."""
		# Create a new user.
		new_username = "test_user_rodsuser"
		user_type = "rodsuser"
		r = users_groups.create_user(self.rodsadmin_session, new_username, self.zone_name, user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that a rodsadmin can change the type of the new user.
		new_user_type = "groupadmin"
		r = users_groups.set_user_type(self.rodsadmin_session, new_username, self.zone_name, new_user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show that a non-admin cannot change the type of the new user.
		r = users_groups.set_user_type(self.rodsuser_session, new_user_type, self.zone_name, new_user_type)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], -13000)

		# Show that the user type matches the type set by the rodsadmin.
		r = users_groups.stat(self.rodsuser_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)

		stat_info = r["data"]
		self.assertEqual(stat_info["irods_response"]["status_code"], 0)
		self.assertEqual(stat_info["exists"], True)
		self.assertEqual(stat_info["local_unique_name"], f"{new_username}#{self.zone_name}")
		self.assertEqual(stat_info["type"], new_user_type)

		# Remove the user.
		r = users_groups.remove_user(self.rodsadmin_session, new_username, self.zone_name)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_listing_all_users_in_zone(self):
		"""Test listing all users in the zone."""
		r = users_groups.users(
			self.rodsadmin_session,
		)
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertIn({"name": self.rodsadmin_username, "zone": self.zone_name}, result["users"])
		self.assertIn({"name": self.rodsuser_username, "zone": self.zone_name}, result["users"])

	def test_listing_all_groups_in_zone(self):
		"""Test listing all groups in the zone."""
		# Create a new group.
		new_group = "test_group"
		r = users_groups.create_group(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)
		# Get all groups.
		r = users_groups.groups(
			self.rodsadmin_session,
		)
		self.assertEqual(r["status_code"], 200)
		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertIn("public", result["groups"])
		self.assertIn(new_group, result["groups"])
		# Remove the new group.
		r = users_groups.remove_group(self.rodsadmin_session, new_group)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

	def test_modifying_metadata_atomically(self):
		"""Test atomically modifying user metadata."""
		username = self.rodsuser_username

		# Add metadata to the user.
		ops = [{"operation": "add", "attribute": "a1", "value": "v1", "units": "u1"}]
		r = users_groups.modify_metadata(self.rodsadmin_session, username, ops)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the metadata exists on the user.
		r = queries.execute_genquery(
			self.rodsadmin_session,
			"select USER_NAME where META_USER_ATTR_NAME = 'a1' and "
			"META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'",
		)
		self.assertEqual(r["status_code"], 200)

		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)
		self.assertEqual(result["rows"][0][0], username)

		# Remove the metadata from the user.
		ops = [{"operation": "remove", "attribute": "a1", "value": "v1", "units": "u1"}]
		r = users_groups.modify_metadata(self.rodsadmin_session, username, ops)
		self.assertEqual(r["status_code"], 200)
		self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

		# Show the metadata no longer exists on the user.
		r = queries.execute_genquery(
			self.rodsadmin_session,
			"select USER_NAME where META_USER_ATTR_NAME = 'a1' and "
			"META_USER_ATTR_VALUE = 'v1' and META_USER_ATTR_UNITS = 'u1'",
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
		r = zones.report(
			self.rodsadmin_session,
		)
		self.assertEqual(r["status_code"], 200)

		result = r["data"]
		self.assertEqual(result["irods_response"]["status_code"], 0)

		zone_report = result["zone_report"]
		self.assertIn("zones", zone_report)
		self.assertGreaterEqual(len(zone_report["zones"]), 1)
		self.assertIn("schema_version", zone_report["zones"][0]["servers"][0]["server_config"])

	def test_adding_removing_and_modifying_zones(self):
		"""Test adding, removing, and modifying zones."""
		try:
			zone_name = "other_zone"

			# Add a zone, only to remove it immediately
			r = zones.add(self.rodsadmin_session, zone_name, connection_info="localhost:1250", comment="brief")
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Remove it
			r = zones.remove(self.rodsadmin_session, zone_name)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Add a remote zone to the local zone.
			# The new zone will not have any connection information or anything else.
			r = zones.add(self.rodsadmin_session, zone_name)
			self.assertEqual(r["status_code"], 200)
			self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

			# Show the new zone exists by executing the stat operation on it.
			r = zones.stat(self.rodsadmin_session, zone_name)
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
					r = zones.modify(self.rodsadmin_session, zone_name, p, v)
					self.assertEqual(r["status_code"], 200)
					self.assertEqual(r["data"]["irods_response"]["status_code"], 0)

					# Capture the new name of the zone following its renaming.
					if p == "name":
						zone_name = v

					# Show the new zone was modified successfully.
					r = zones.stat(self.rodsadmin_session, zone_name)
					self.assertEqual(r["status_code"], 200)

					result = r["data"]
					self.assertEqual(result["irods_response"]["status_code"], 0)
					self.assertEqual(result["exists"], True)
					self.assertEqual(result["info"][p], v)

		finally:
			# Remove the remote zone.
			zones.remove(self.rodsadmin_session, zone_name)


if __name__ == "__main__":
	unittest.main()
