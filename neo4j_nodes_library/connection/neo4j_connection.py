"""Defines the Neo4jConnection node for managing Neo4j database sessions.

This module provides the `Neo4jConnection` class, which takes a Neo4j driver
and creates a database session that can be used by other Neo4j nodes for
executing queries and transactions.
"""

import logging
from typing import Any

import neo4j

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from griptape_nodes.traits.options import Options
from neo4j_nodes_library.config.neo4j_driver import Neo4jDriverRegistry

logger = logging.getLogger("griptape_nodes")

# --- Constants ---

SESSION_MODES = ["read", "write"]
DEFAULT_SESSION_MODE = "write"


class Neo4jSessionWrapper:
    """Wrapper to manage Neo4j session lifecycle and prevent __del__ issues."""

    def __init__(self, session: neo4j.Session) -> None:
        self._session = session
        self._closed = False

    def run(self, *args: Any, **kwargs: Any) -> neo4j.Result:
        """Execute a query using the wrapped session."""
        if self._closed:
            msg = "Session has been closed"
            raise RuntimeError(msg)
        return self._session.run(*args, **kwargs)

    def close(self) -> None:
        """Close the session safely."""
        if not self._closed and self._session:
            try:
                self._session.close()
            except Exception as e:
                # Suppress exceptions during cleanup to prevent issues during garbage collection
                # or application shutdown when Neo4j resources may already be cleaned up
                logger.debug("Exception during session cleanup (expected during shutdown): %s", e)
            finally:
                self._closed = True

    def __getattr__(self, name: str) -> Any:
        """Delegate other attributes to the wrapped session."""
        if self._closed:
            msg = "Session has been closed"
            raise RuntimeError(msg)
        return getattr(self._session, name)


class Neo4jConnection(ControlNode):
    """Node for creating and managing Neo4j database sessions.

    This node can take an optional Neo4j driver instance or will create a default
    driver using library configuration settings. It creates a database session
    with specified configuration and manages session lifecycle, providing
    the session for use by other Neo4j operation nodes.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the Neo4jConnection node.

        Sets up parameters for Neo4j session configuration including
        driver input, database selection, and session options.
        """
        super().__init__(**kwargs)
        self._session_wrapper = None

        # Input driver (optional - will use default if not provided)
        self.add_parameter(
            Parameter(
                name="driver",
                input_types=["Any"],
                type="Any",
                output_type="Any",
                default_value=None,
                tooltip="Neo4j driver instance from Neo4j Driver node (optional - uses default configuration if not provided)",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Neo4j Driver"},
            )
        )

        # Database name (can override driver's default)
        self.add_parameter(
            Parameter(
                name="database",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="",
                tooltip="Database name (leave empty to use driver's default)",
                allowed_modes={ParameterMode.INPUT, ParameterMode.PROPERTY},
                ui_options={"display_name": "Database"},
            )
        )

        # Session access mode
        session_mode_param = Parameter(
            name="session_mode",
            input_types=["str"],
            type="str",
            output_type="str",
            default_value=DEFAULT_SESSION_MODE,
            allowed_modes={ParameterMode.PROPERTY},
            tooltip="Session access mode: read for read-only operations, write for read-write operations",
            ui_options={"display_name": "Session Mode"},
        )
        session_mode_param.add_trait(Options(choices=SESSION_MODES))
        self.add_parameter(session_mode_param)

        # Fetch size (for streaming results)
        self.add_parameter(
            Parameter(
                name="fetch_size",
                input_types=["int"],
                type="int",
                output_type="int",
                default_value=1000,
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Number of records to fetch at a time for streaming results",
                ui_options={"display_name": "Fetch Size"},
            )
        )

        # Default access mode for transactions
        self.add_parameter(
            Parameter(
                name="default_access_mode",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="",
                allowed_modes={ParameterMode.PROPERTY},
                tooltip="Default access mode for transactions (leave empty to use session mode)",
                ui_options={"display_name": "Default Access Mode"},
            )
        )

        # Output parameter for the session
        self.add_parameter(
            Parameter(
                name="session",
                output_type="Any",
                default_value=None,
                tooltip="Neo4j session instance",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Neo4j Session"},
            )
        )

        # Output parameter for connection info
        self.add_parameter(
            Parameter(
                name="connection_info",
                output_type="dict",
                default_value={},
                tooltip="Connection information and metadata",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Connection Info"},
            )
        )

        # Message parameter for status feedback
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

    def _get_access_mode(self, session_mode: str) -> Any:
        """Converts session mode string to Neo4j access mode constant.

        Args:
            session_mode: Session mode string ("read" or "write")

        Returns:
            Neo4j access mode constant
        """
        import neo4j

        if session_mode == "read":
            return neo4j.READ_ACCESS
        if session_mode == "write":
            return neo4j.WRITE_ACCESS
        msg = f"Invalid session mode: {session_mode}"
        raise ValueError(msg)

    def process(self) -> None:
        """Processes the node to create a Neo4j session.

        Takes the input driver and creates a configured session with the
        specified parameters. Provides the session and connection information
        as outputs.
        """
        # Get parameter values
        driver = self.get_parameter_value("driver")

        # If no driver is provided, create a default one using the registry
        if driver is None:
            registry = Neo4jDriverRegistry()
            driver = registry.create_default_driver(self.get_config_value)

        database = self.get_parameter_value("database")
        session_mode = self.get_parameter_value("session_mode")
        fetch_size = self.get_parameter_value("fetch_size")
        self.get_parameter_value("default_access_mode")

        # Prepare session configuration
        session_config = {}

        # Set database if specified, otherwise use driver's database if available
        if database:
            session_config["database"] = database
        elif hasattr(driver, "_database") and driver._database:
            session_config["database"] = driver._database

        # Set fetch size
        if fetch_size > 0:
            session_config["fetch_size"] = fetch_size

        # Set default access mode
        access_mode = self._get_access_mode(session_mode)
        session_config["default_access_mode"] = access_mode

        try:
            # Create session and wrap it
            raw_session = driver.session(**session_config)
            session = Neo4jSessionWrapper(raw_session)
            self._session_wrapper = session  # Keep reference to manage lifecycle

            # Get connection information
            connection_info = {
                "database": database or "default",
                "session_mode": session_mode,
                "fetch_size": fetch_size,
                "server_info": {
                    "address": str(driver._pool.address) if hasattr(driver, "_pool") and driver._pool else "unknown",
                    "protocol_version": "unknown",
                },
            }

            # Test the session with a simple query
            result = session.run("RETURN 1 as test")
            test_record = result.single()
            if test_record and test_record["test"] == 1:
                session_status = "✓ Session created successfully"
            else:
                session_status = "⚠️ Session created but test query failed"

            # Set output values
            self.parameter_output_values["session"] = session
            self.parameter_output_values["connection_info"] = connection_info
            self.parameter_output_values["message"] = session_status

            # Hide message parameter since connection is successful
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to create Neo4j session: {e!s}"
            logger.error(error_msg)

            # Set error message and make it visible
            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            # Set outputs to None/empty
            self.parameter_output_values["session"] = None
            self.parameter_output_values["connection_info"] = {}

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the Neo4j connection parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Validate fetch size
        fetch_size = self.get_parameter_value("fetch_size")
        if fetch_size <= 0:
            exceptions.append(ValueError("Fetch size must be greater than 0"))

        # Note: We don't validate the driver parameter here because it comes from
        # an input connection that might not be resolved at validation time

        return exceptions if exceptions else None

    def __del__(self) -> None:
        """Clean up session wrapper on node destruction."""
        if hasattr(self, "_session_wrapper") and self._session_wrapper:
            try:
                self._session_wrapper.close()
            except Exception as e:
                # Suppress exceptions during __del__ cleanup to prevent issues when the
                # Python interpreter is shutting down and objects may be partially cleaned up
                logger.debug("Exception during __del__ cleanup (expected during shutdown): %s", e)
