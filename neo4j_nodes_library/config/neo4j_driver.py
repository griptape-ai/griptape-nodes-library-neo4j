"""Defines the Neo4jDriver node for configuring Neo4j database connections.

This module provides the `Neo4jDriver` class, which allows users
to configure and utilize Neo4j graph database connections within the Griptape
Nodes framework. It manages connection parameters, authentication, and
provides a configured Neo4j driver instance.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Self

if TYPE_CHECKING:
    from collections.abc import Callable

import neo4j
from neo4j import GraphDatabase

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import DataNode
from griptape_nodes.traits.options import Options

# NOTE: There is a known bug in the Neo4j Python driver where __del__ methods fail
# with "AttributeError: 'NoneType' object has no attribute 'close'" during garbage
# collection. This is harmless but produces error messages. See:
# https://github.com/neo4j/neo4j-python-driver/issues/255
# https://github.com/neo4j/neo4j-python-driver/issues/172


class DriverKey(NamedTuple):
    """Key for identifying unique driver configurations."""
    uri: str
    auth: str
    config: str


class Neo4jDriverRegistry:
    """Global singleton registry to manage Neo4j drivers and prevent __del__ issues."""

    _instance: ClassVar[Neo4jDriverRegistry | None] = None
    _drivers: ClassVar[dict[DriverKey, neo4j.Driver]] = {}

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Register cleanup function
            atexit.register(cls._instance._cleanup_all_drivers)
        return cls._instance

    def get_or_create_driver(self, uri: str, auth: neo4j.Auth | None, **config: Any) -> neo4j.Driver:
        """Get existing driver or create new one with given parameters."""
        # Create a key from connection parameters
        key = DriverKey(uri=uri, auth=str(auth), config=str(sorted(config.items())))

        if key not in self._drivers:
            driver = GraphDatabase.driver(uri, auth=auth, **config)
            self._drivers[key] = driver

        return self._drivers[key]

    def create_default_driver(self, config_getter: Callable[[str, str], str | None]) -> Neo4jDriverWrapper:
        """Create a driver using default settings from library configuration.

        Args:
            config_getter: Function that takes (service, key) and returns config value.

        Returns:
            Neo4jDriverWrapper instance configured with default settings.
        """
        # Default values if no config is available
        # Note: Default password is for development/testing only - production should use config
        default_uri = "bolt://localhost:7687"
        default_database = "neo4j"
        default_username = "neo4j"
        default_password = "password"  # noqa: S105

        # Use the config system to get values
        uri = config_getter("Neo4j", "NEO4J_URI") or default_uri
        database = config_getter("Neo4j", "NEO4J_DATABASE") or default_database
        username = config_getter("Neo4j", "NEO4J_USERNAME") or default_username
        password = config_getter("Neo4j", "NEO4J_PASSWORD") or default_password

        # Create basic auth
        auth = neo4j.basic_auth(username, password)

        # Create driver config with sensible defaults
        driver_config = {
            "connection_timeout": 30.0,
            "max_connection_lifetime": 3600.0,
        }

        # Create wrapper using the registry
        wrapper = Neo4jDriverWrapper(uri, auth, **driver_config)
        wrapper._database = database  # Store database name for use by connection nodes

        return wrapper

    def _cleanup_all_drivers(self) -> None:
        """Clean up all drivers at application exit."""
        for driver in self._drivers.values():
            with contextlib.suppress(Exception):
                driver.close()
        self._drivers.clear()


class Neo4jDriverWrapper:
    """Wrapper to properly manage Neo4j driver lifecycle and prevent __del__ issues."""

    def __init__(self, uri: str, auth: neo4j.Auth | None, **config: Any) -> None:
        self.uri = uri
        self.auth = auth
        self.config = config
        self._registry = Neo4jDriverRegistry()
        self._driver = None

    def _ensure_driver(self) -> neo4j.Driver:
        """Ensure the driver is created and return it."""
        if self._driver is None:
            self._driver = self._registry.get_or_create_driver(self.uri, self.auth, **self.config)
        return self._driver

    def session(self, **kwargs: Any) -> neo4j.Session:
        """Create a session using the managed driver."""
        driver = self._ensure_driver()
        return driver.session(**kwargs)

    def verify_connectivity(self, **kwargs: Any) -> None:
        """Verify driver connectivity."""
        driver = self._ensure_driver()
        return driver.verify_connectivity(**kwargs)

    def close(self) -> None:
        """Note: Actual driver cleanup is handled by the registry at app exit."""
        # We don't actually close the driver here since it might be shared
        # The registry will handle cleanup at application exit


logger = logging.getLogger("griptape_nodes")

# --- Constants ---

SERVICE = "Neo4j"
DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_DATABASE = "neo4j"
AUTH_TYPES = ["basic", "bearer_token", "custom", "none"]
DEFAULT_AUTH_TYPE = "basic"


class Neo4jDriver(DataNode):
    """Node for configuring and providing a Neo4j database driver.

    This node manages Neo4j database connection parameters including URI,
    authentication credentials, database name, and connection settings.
    It creates and provides a configured Neo4j driver instance that can
    be used by other Neo4j nodes for database operations.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the Neo4jDriver node.

        Sets up parameters for Neo4j connection configuration including
        URI, authentication, database selection, and connection options.
        """
        super().__init__(**kwargs)

        # Keep a reference to the driver to prevent premature cleanup
        self._neo4j_driver = None

        # Connection URI
        self.add_parameter(
            Parameter(
                name="uri",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value=DEFAULT_URI,
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Neo4j database URI (e.g., bolt://localhost:7687, neo4j://localhost:7687)",
                ui_options={"display_name": "Database URI"},
            )
        )

        # Database name
        self.add_parameter(
            Parameter(
                name="database",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value=DEFAULT_DATABASE,
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Neo4j database name",
                ui_options={"display_name": "Database"},
            )
        )

        # Authentication type
        auth_type_param = Parameter(
            name="auth_type",
            input_types=["str"],
            type="str",
            output_type="str",
            default_value=DEFAULT_AUTH_TYPE,
            allowed_modes={ParameterMode.PROPERTY},
            tooltip="Authentication type for Neo4j connection",
            ui_options={"display_name": "Auth Type"},
        )
        auth_type_param.add_trait(Options(choices=AUTH_TYPES))
        self.add_parameter(auth_type_param)

        # Username (for basic auth)
        self.add_parameter(
            Parameter(
                name="username",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="neo4j",
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Username for basic authentication",
                ui_options={"display_name": "Username"},
            )
        )

        # Password (for basic auth)
        self.add_parameter(
            Parameter(
                name="password",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="password",
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Password for basic authentication",
                ui_options={"display_name": "Password", "password": True},
            )
        )

        # Bearer token (for bearer auth)
        self.add_parameter(
            Parameter(
                name="bearer_token",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="",
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Bearer token for token-based authentication",
                ui_options={"display_name": "Bearer Token", "password": True},
            )
        )

        # Custom auth realm (optional)
        self.add_parameter(
            Parameter(
                name="realm",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="",
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Authentication realm (optional)",
                ui_options={"display_name": "Realm"},
            )
        )

        # Connection timeout
        self.add_parameter(
            Parameter(
                name="connection_timeout",
                input_types=["float"],
                type="float",
                output_type="float",
                default_value=30.0,
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Connection timeout in seconds",
                ui_options={"display_name": "Connection Timeout"},
            )
        )

        # Max connection lifetime
        self.add_parameter(
            Parameter(
                name="max_connection_lifetime",
                input_types=["float"],
                type="float",
                output_type="float",
                default_value=3600.0,
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Maximum connection lifetime in seconds",
                ui_options={"display_name": "Max Connection Lifetime"},
            )
        )

        # Output parameter for the driver
        self.add_parameter(
            Parameter(
                name="driver",
                input_types=["Any"],
                type="Any",
                output_type="Any",
                default_value=None,
                tooltip="Configured Neo4j driver instance",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Neo4j Driver"},
            )
        )

        # Message parameter for validation feedback
        self.add_parameter(
            Parameter(
                name="message",
                output_type="str",
                default_value="",
                tooltip="Status or error messages",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Message", "hide": True},
            )
        )

    def _test_connection_or_raise(self, session: neo4j.Session) -> None:
        """Test connection or raise ConnectionError."""
        result = session.run("RETURN 1 as test")
        test_record = result.single()
        if not test_record or test_record["test"] != 1:
            msg = "Connection test failed - could not execute test query"
            raise ConnectionError(msg)

    def _create_auth(self, auth_type: str, username: str, password: str, bearer_token: str, realm: str) -> Any:
        """Creates appropriate authentication object based on auth type.

        Args:
            auth_type: Type of authentication to use
            username: Username for basic auth
            password: Password for basic auth
            bearer_token: Bearer token for token auth
            realm: Authentication realm

        Returns:
            Neo4j authentication object or None
        """
        if auth_type == "basic":
            if realm:
                return neo4j.basic_auth(username, password, realm)
            return neo4j.basic_auth(username, password)
        if auth_type == "bearer_token":
            return neo4j.bearer_auth(bearer_token)
        if auth_type == "none":
            return None
        if auth_type == "custom":
            # For custom auth, users would need to extend this
            logger.warning("Custom authentication not implemented. Using basic auth.")
            return neo4j.basic_auth(username, password)
        msg = f"Unsupported authentication type: {auth_type}"
        raise ValueError(msg)

    def process(self) -> None:
        """Processes the node configuration to create a Neo4j driver.

        Creates and tests a Neo4j driver with the specified connection parameters.
        Fails fast if the driver cannot be created or the connection test fails.
        """
        # Get parameter values
        uri = self.get_parameter_value("uri")
        auth_type = self.get_parameter_value("auth_type")
        username = self.get_parameter_value("username")
        password = self.get_parameter_value("password")
        bearer_token = self.get_parameter_value("bearer_token")
        realm = self.get_parameter_value("realm")
        connection_timeout = self.get_parameter_value("connection_timeout")
        max_connection_lifetime = self.get_parameter_value("max_connection_lifetime")

        # Create authentication
        auth = self._create_auth(auth_type, username, password, bearer_token, realm)

        # Create driver configuration
        driver_config = {
            "connection_timeout": connection_timeout,
            "max_connection_lifetime": max_connection_lifetime,
        }

        try:
            # Create the wrapped Neo4j driver to avoid __del__ issues
            driver_wrapper = Neo4jDriverWrapper(uri, auth, **driver_config)

            # Test the connection - this is mandatory
            with driver_wrapper.session() as session:
                self._test_connection_or_raise(session)

            # Store the wrapper reference to keep it alive
            self._neo4j_driver = driver_wrapper

            # Set output values
            self.parameter_output_values["driver"] = driver_wrapper
            self.parameter_output_values["message"] = "✓ Neo4j driver created and tested successfully"

        except Exception as e:
            # No need to clean up driver wrapper since registry handles lifecycle

            # Import Neo4j exception types for proper matching
            import socket

            from neo4j.exceptions import AuthError, ConfigurationError, ServiceUnavailable

            # Provide specific, actionable error messages based on exception type
            if isinstance(e, ServiceUnavailable):
                error_msg = (
                    f"❌ Neo4j Service Unavailable: Cannot connect to Neo4j at {uri}\n"
                    f"Solutions:\n"
                    f"• Check that Neo4j is running\n"
                    f"• Verify the URI is correct (currently: {uri})\n"
                    f"• Check firewall settings\n"
                    f"• Try running: docker run --publish=7474:7474 --publish=7687:7687 neo4j"
                )

            elif isinstance(e, AuthError):
                error_msg = (
                    f"❌ Neo4j Authentication Failed: Invalid credentials\n"
                    f"Solutions:\n"
                    f"• Check username/password (currently using: {username})\n"
                    f"• Verify auth type is correct (currently: {auth_type})\n"
                    f"• For default Neo4j: username='neo4j', password=<your-password>\n"
                    f"• Reset password: neo4j-admin set-initial-password <new-password>"
                )

            elif isinstance(e, ConfigurationError):
                error_msg = (
                    f"❌ Neo4j Configuration Error: Invalid connection configuration\n"
                    f"Solutions:\n"
                    f"• Check URI format: {uri}\n"
                    f"• Verify connection timeout: {connection_timeout}s\n"
                    f"• Check max connection lifetime: {max_connection_lifetime}s"
                )

            elif isinstance(e, (socket.gaierror, socket.error)):
                error_msg = (
                    f"❌ Neo4j Network Error: Cannot resolve or connect to host\n"
                    f"Solutions:\n"
                    f"• Check the hostname in URI: {uri}\n"
                    f"• Try using IP address instead: bolt://127.0.0.1:7687\n"
                    f"• Check DNS settings\n"
                    f"• Verify Neo4j is listening on the correct port"
                )

            elif isinstance(e, ConnectionRefusedError):
                error_msg = (
                    f"❌ Neo4j Connection Refused: Neo4j is not accepting connections\n"
                    f"Solutions:\n"
                    f"• Ensure Neo4j is running on {uri}\n"
                    f"• Check if port 7687 is available\n"
                    f"• Verify firewall/security settings\n"
                    f"• Check Neo4j configuration (neo4j.conf)"
                )

            elif isinstance(e, TimeoutError):
                error_msg = (
                    f"❌ Neo4j Connection Timeout: Connection took too long\n"
                    f"Solutions:\n"
                    f"• Increase connection timeout (currently: {connection_timeout}s)\n"
                    f"• Check network connectivity to {uri}\n"
                    f"• Verify Neo4j is responding (not overloaded)"
                )

            else:
                error_msg = (
                    f"❌ Neo4j Connection Error: {type(e).__name__}: {e}\n"
                    f"Troubleshooting:\n"
                    f"• URI: {uri}\n"
                    f"• Username: {username}\n"
                    f"• Auth Type: {auth_type}\n"
                    f"• Connection Timeout: {connection_timeout}s\n"
                    f"• Check Neo4j server logs for more details"
                )

            logger.error(error_msg)
            self.parameter_output_values["message"] = error_msg

            # Re-raise the exception to abort the flow
            raise RuntimeError(error_msg) from e

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the Neo4j connection parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Validate URI
        uri = self.get_parameter_value("uri")
        if not uri:
            exceptions.append(ValueError("Database URI is required"))

        # Validate authentication parameters based on auth type
        auth_type = self.get_parameter_value("auth_type")

        if auth_type == "basic":
            username = self.get_parameter_value("username")
            password = self.get_parameter_value("password")
            if not username:
                exceptions.append(ValueError("Username is required for basic authentication"))
            if not password:
                exceptions.append(ValueError("Password is required for basic authentication"))

        elif auth_type == "bearer_token":
            bearer_token = self.get_parameter_value("bearer_token")
            if not bearer_token:
                exceptions.append(ValueError("Bearer token is required for token authentication"))

        return exceptions if exceptions else None
