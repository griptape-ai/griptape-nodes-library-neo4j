"""Defines the UpdateNode node for updating nodes in Neo4j.

This module provides the `UpdateNode` class, which allows users to update
properties of existing nodes in a Neo4j database.
"""

import json
import logging

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from neo4j_nodes_library.connection.neo4j_connection import Neo4jSessionWrapper

logger = logging.getLogger("griptape_nodes")


class UpdateNode(ControlNode):
    """Node for updating properties of nodes in a Neo4j database.

    This node takes a Neo4j session and updates properties of nodes that
    match specified criteria.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the UpdateNode node."""
        super().__init__(**kwargs)

        # Input session
        self.add_parameter(
            Parameter(
                name="session",
                output_type="Any",
                default_value=None,
                tooltip="Neo4j session instance from Neo4j Connection node",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Neo4j Session"},
            )
        )

        # Match criteria
        self.add_parameter(
            Parameter(
                name="match_labels",
                output_type="str",
                default_value="Person",
                tooltip="Labels to match nodes for update",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Match Labels"},
            )
        )

        self.add_parameter(
            Parameter(
                name="match_properties",
                output_type="str",
                default_value='{"name": "John Doe"}',
                tooltip="Properties to match nodes for update as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Match Properties", "multiline": True, "rows": 3},
            )
        )

        # Update properties
        self.add_parameter(
            Parameter(
                name="update_properties",
                output_type="str",
                default_value='{"age": 31, "city": "New York"}',
                tooltip="Properties to update as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Update Properties", "multiline": True, "rows": 3},
            )
        )

        # Update mode
        self.add_parameter(
            Parameter(
                name="update_mode",
                output_type="str",
                default_value="SET",
                tooltip="Update mode: SET (add/update), REMOVE (remove properties)",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Update Mode"},
            )
        )

        # Output parameters
        self.add_parameter(
            Parameter(
                name="updated_count",
                output_type="int",
                default_value=0,
                tooltip="Number of nodes updated",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Updated Count"},
            )
        )

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

    def _validate_session_or_raise(self, session: Neo4jSessionWrapper | None) -> None:
        """Validate session is not None or raise ValueError."""
        if session is None:
            msg = "Neo4j session is required"
            raise ValueError(msg)

    def process(self) -> None:
        """Processes the node to update nodes in the Neo4j database."""
        try:
            session = self.get_parameter_value("session")
            match_labels_str = self.get_parameter_value("match_labels")
            match_props_str = self.get_parameter_value("match_properties")
            update_props_str = self.get_parameter_value("update_properties")
            update_mode = self.get_parameter_value("update_mode")

            self._validate_session_or_raise(session)

            # Parse parameters
            match_labels = [label.strip() for label in match_labels_str.split(",") if label.strip()]
            match_props = json.loads(match_props_str) if match_props_str.strip() else {}
            update_props = json.loads(update_props_str) if update_props_str.strip() else {}

            # Build query
            label_part = ":" + ":".join(match_labels) if match_labels else ""
            query = f"MATCH (n{label_part})"

            # Add WHERE conditions for match properties
            params = {}
            if match_props:
                conditions = []
                for prop, value in match_props.items():
                    param_name = f"match_{prop.replace('.', '_')}"
                    conditions.append(f"n.{prop} = ${param_name}")
                    params[param_name] = value
                query += " WHERE " + " AND ".join(conditions)

            # Add SET clause for updates
            if update_props:
                if update_mode.upper() == "SET":
                    set_clauses = []
                    for prop, value in update_props.items():
                        param_name = f"update_{prop.replace('.', '_')}"
                        set_clauses.append(f"n.{prop} = ${param_name}")
                        params[param_name] = value
                    query += " SET " + ", ".join(set_clauses)
                elif update_mode.upper() == "REMOVE":
                    remove_clauses = [f"n.{prop}" for prop in update_props]
                    query += " REMOVE " + ", ".join(remove_clauses)

            # Execute query
            result = session.run(query, params)
            summary = result.consume()

            updated_count = summary.counters.properties_set

            self.parameter_output_values["updated_count"] = updated_count
            self.parameter_output_values["message"] = f"✓ Updated {updated_count} properties"

            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to update nodes: {e!s}"
            logger.error(error_msg)

            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            self.parameter_output_values["updated_count"] = 0

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the update parameters."""
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        # Validate JSON parameters
        for param_name in ["match_properties", "update_properties"]:
            try:
                param_value = self.get_parameter_value(param_name)
                if param_value.strip():
                    json.loads(param_value)
            except json.JSONDecodeError as e:
                exceptions.append(ValueError(f"Invalid JSON in {param_name}: {e!s}"))

        return exceptions if exceptions else None
