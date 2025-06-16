"""Defines the DeleteNode node for deleting nodes from Neo4j.

This module provides the `DeleteNode` class, which allows users to delete
nodes from a Neo4j database based on specified criteria.
"""

import json
import logging

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from connection.neo4j_connection import Neo4jSessionWrapper

logger = logging.getLogger("griptape_nodes")


class DeleteNode(ControlNode):
    """Node for deleting nodes from a Neo4j database.

    This node takes a Neo4j session and deletes nodes that match
    specified criteria. Optionally deletes relationships as well.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the DeleteNode node."""
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
                tooltip="Labels to match nodes for deletion",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Match Labels"},
            )
        )

        self.add_parameter(
            Parameter(
                name="match_properties",
                output_type="str",
                default_value='{"name": "John Doe"}',
                tooltip="Properties to match nodes for deletion as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Match Properties", "multiline": True, "rows": 3},
            )
        )

        # Delete relationships too
        self.add_parameter(
            Parameter(
                name="detach_delete",
                output_type="bool",
                default_value=True,
                tooltip="Whether to delete relationships connected to the node (DETACH DELETE)",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Detach Delete"},
            )
        )

        # Confirmation parameter
        self.add_parameter(
            Parameter(
                name="confirm_delete",
                output_type="bool",
                default_value=False,
                tooltip="Confirmation required to proceed with deletion",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Confirm Delete"},
            )
        )

        # Output parameters
        self.add_parameter(
            Parameter(
                name="deleted_count",
                output_type="int",
                default_value=0,
                tooltip="Number of nodes deleted",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Deleted Count"},
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

    def _validate_confirm_delete_or_raise(self, *, confirm_delete: bool) -> None:
        """Validate delete confirmation is True or raise ValueError."""
        if not confirm_delete:
            msg = "Delete confirmation is required"
            raise ValueError(msg)

    def process(self) -> None:
        """Processes the node to delete nodes from the Neo4j database."""
        try:
            session = self.get_parameter_value("session")
            match_labels_str = self.get_parameter_value("match_labels")
            match_props_str = self.get_parameter_value("match_properties")
            detach_delete = self.get_parameter_value("detach_delete")
            confirm_delete = self.get_parameter_value("confirm_delete")

            self._validate_session_or_raise(session)
            self._validate_confirm_delete_or_raise(confirm_delete=confirm_delete)

            # Parse parameters
            match_labels = [label.strip() for label in match_labels_str.split(",") if label.strip()]
            match_props = json.loads(match_props_str) if match_props_str.strip() else {}

            # Build query
            label_part = ":" + ":".join(match_labels) if match_labels else ""
            query = f"MATCH (n{label_part})"

            # Add WHERE conditions
            params = {}
            if match_props:
                conditions = []
                for prop, value in match_props.items():
                    param_name = f"match_{prop.replace('.', '_')}"
                    conditions.append(f"n.{prop} = ${param_name}")
                    params[param_name] = value
                query += " WHERE " + " AND ".join(conditions)

            # Add DELETE clause
            if detach_delete:
                query += " DETACH DELETE n"
            else:
                query += " DELETE n"

            # Execute query
            result = session.run(query, params)
            summary = result.consume()

            deleted_count = summary.counters.nodes_deleted

            self.parameter_output_values["deleted_count"] = deleted_count
            self.parameter_output_values["message"] = f"✓ Deleted {deleted_count} node(s)"

            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to delete nodes: {e!s}"
            logger.error(error_msg)

            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            self.parameter_output_values["deleted_count"] = 0

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the delete parameters."""
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        confirm_delete = self.get_parameter_value("confirm_delete")
        if not confirm_delete:
            exceptions.append(ValueError("Delete confirmation is required"))

        # Validate JSON parameters
        try:
            match_props_str = self.get_parameter_value("match_properties")
            if match_props_str.strip():
                json.loads(match_props_str)
        except json.JSONDecodeError as e:
            exceptions.append(ValueError(f"Invalid JSON in match_properties: {e!s}"))

        return exceptions if exceptions else None
