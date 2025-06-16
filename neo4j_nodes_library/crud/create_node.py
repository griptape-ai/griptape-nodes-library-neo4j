"""Defines the CreateNode node for creating nodes in Neo4j.

This module provides the `CreateNode` class, which allows users to create
new nodes in a Neo4j database with specified labels and properties.
"""

import json
import logging
from typing import Any

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from connection.neo4j_connection import Neo4jSessionWrapper

logger = logging.getLogger("griptape_nodes")


class CreateNode(ControlNode):
    """Node for creating nodes in a Neo4j database.

    This node takes a Neo4j session and creates a new node with specified
    labels and properties. It returns the created node information.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the CreateNode node.

        Sets up parameters for node creation including session input,
        labels, properties, and result handling.
        """
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

        # Node labels
        self.add_parameter(
            Parameter(
                name="labels",
                output_type="str",
                default_value="Person",
                tooltip="Node labels separated by commas (e.g., 'Person,Employee')",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Labels"},
            )
        )

        # Node properties (JSON string)
        self.add_parameter(
            Parameter(
                name="properties",
                output_type="str",
                default_value='{"name": "John Doe", "age": 30}',
                tooltip="Node properties as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Properties", "multiline": True, "rows": 4},
            )
        )

        # Whether to return the created node
        self.add_parameter(
            Parameter(
                name="return_node",
                output_type="bool",
                default_value=True,
                tooltip="Whether to return the created node data",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Return Node"},
            )
        )

        # Output parameter for created node
        self.add_parameter(
            Parameter(
                name="created_node",
                output_type="dict",
                default_value={},
                tooltip="Information about the created node",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Created Node"},
            )
        )

        # Output parameter for node ID
        self.add_parameter(
            Parameter(
                name="node_id",
                output_type="int",
                default_value=0,
                tooltip="ID of the created node",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Node ID"},
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

    def _parse_labels(self, labels_str: str) -> list[str]:
        """Parses label string into a list of labels.

        Args:
            labels_str: Comma-separated string of labels

        Returns:
            List of label strings
        """
        if not labels_str.strip():
            return []

        # Split by comma and clean up whitespace
        labels = [label.strip() for label in labels_str.split(",")]
        return [label for label in labels if label]  # Remove empty labels

    def _parse_properties(self, properties_str: str) -> dict:
        """Parses JSON property string into a dictionary.

        Args:
            properties_str: JSON string containing node properties

        Returns:
            Dictionary of parsed properties

        Raises:
            ValueError: If JSON is invalid
        """
        if not properties_str.strip():
            return {}

        try:
            return json.loads(properties_str)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in properties: {e!s}"
            raise ValueError(msg) from e

    def _validate_session_or_raise(self, session: Neo4jSessionWrapper | None) -> None:
        """Validate session is not None or raise ValueError."""
        if session is None:
            msg = "Neo4j session is required"
            raise ValueError(msg)

    def _build_create_query(self, labels: list[str], *, return_node: bool) -> str:
        """Builds the Cypher CREATE query.

        Args:
            labels: List of node labels
            return_node: Whether to return the created node

        Returns:
            Cypher query string
        """
        if labels:
            label_part = ":" + ":".join(labels)
        else:
            label_part = ""

        query = f"CREATE (n{label_part} $properties)"

        if return_node:
            query += " RETURN n"

        return query

    def _convert_node_to_dict(self, node: Any) -> dict:
        """Converts a Neo4j node to a dictionary.

        Args:
            node: Neo4j node object

        Returns:
            Dictionary representation of the node
        """
        return {"id": node.id, "labels": list(node.labels), "properties": dict(node.items())}

    def process(self) -> None:
        """Processes the node to create a new Neo4j node.

        Creates a new node in the Neo4j database with the specified labels
        and properties.
        """
        try:
            # Get parameter values
            session = self.get_parameter_value("session")
            labels_str = self.get_parameter_value("labels")
            properties_str = self.get_parameter_value("properties")
            return_node = self.get_parameter_value("return_node")

            # Validate inputs
            self._validate_session_or_raise(session)

            # Parse labels and properties
            labels = self._parse_labels(labels_str)
            properties = self._parse_properties(properties_str)

            # Build and execute query
            query = self._build_create_query(labels, return_node=return_node)
            result = session.run(query, properties=properties)

            # Process results
            created_node = {}
            node_id = 0

            if return_node:
                record = result.single()
                if record:
                    node = record["n"]
                    created_node = self._convert_node_to_dict(node)
                    node_id = node.id

            # Consume the result to get summary
            summary = result.consume()
            nodes_created = summary.counters.nodes_created

            # Set output values
            self.parameter_output_values["created_node"] = created_node
            self.parameter_output_values["node_id"] = node_id

            success_msg = f"✓ Successfully created {nodes_created} node(s)"
            if node_id > 0:
                success_msg += f" with ID {node_id}"

            self.parameter_output_values["message"] = success_msg

            # Hide message parameter since creation is successful
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to create node: {e!s}"
            logger.error(error_msg)

            # Set error message and make it visible
            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            # Set outputs to empty/default values
            self.parameter_output_values["created_node"] = {}
            self.parameter_output_values["node_id"] = 0

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the node creation parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        # Validate properties JSON
        try:
            properties_str = self.get_parameter_value("properties")
            if properties_str.strip():
                json.loads(properties_str)
        except json.JSONDecodeError as e:
            exceptions.append(ValueError(f"Invalid JSON in properties: {e!s}"))

        return exceptions if exceptions else None
