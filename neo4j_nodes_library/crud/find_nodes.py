"""Defines the FindNodes node for finding and retrieving nodes from Neo4j.

This module provides the `FindNodes` class, which allows users to search
for nodes in a Neo4j database using various criteria like labels, properties,
and custom where clauses.
"""

import json
import logging
from typing import Any

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from connection.neo4j_connection import Neo4jSessionWrapper

logger = logging.getLogger("griptape_nodes")


class FindNodes(ControlNode):
    """Node for finding and retrieving nodes from a Neo4j database.

    This node takes a Neo4j session and searches for nodes based on
    specified criteria including labels, properties, and custom filters.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the FindNodes node.

        Sets up parameters for node searching including session input,
        search criteria, and result handling options.
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

        # Node labels to match
        self.add_parameter(
            Parameter(
                name="labels",
                output_type="str",
                default_value="",
                tooltip="Node labels to match, separated by commas (e.g., 'Person,Employee'). Leave empty to match any label.",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Labels"},
            )
        )

        # Property filters (JSON string)
        self.add_parameter(
            Parameter(
                name="property_filters",
                output_type="str",
                default_value="{}",
                tooltip='Property filters as JSON object (e.g., {"name": "John", "age": 30})',
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Property Filters", "multiline": True, "rows": 3},
            )
        )

        # Custom WHERE clause
        self.add_parameter(
            Parameter(
                name="where_clause",
                output_type="str",
                default_value="",
                tooltip="Custom WHERE clause (e.g., 'n.age > 25 AND n.name CONTAINS \"John\"'). Leave empty for no additional filters.",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "WHERE Clause", "multiline": True, "rows": 2},
            )
        )

        # Custom parameters for WHERE clause
        self.add_parameter(
            Parameter(
                name="where_parameters",
                output_type="str",
                default_value="{}",
                tooltip="Parameters for WHERE clause as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "WHERE Parameters", "multiline": True, "rows": 2},
            )
        )

        # Maximum number of results
        self.add_parameter(
            Parameter(
                name="limit",
                output_type="int",
                default_value=100,
                tooltip="Maximum number of nodes to return (0 for no limit)",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Limit"},
            )
        )

        # Skip/offset
        self.add_parameter(
            Parameter(
                name="skip",
                output_type="int",
                default_value=0,
                tooltip="Number of nodes to skip for pagination",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Skip"},
            )
        )

        # Order by
        self.add_parameter(
            Parameter(
                name="order_by",
                output_type="str",
                default_value="",
                tooltip="Order by clause (e.g., 'n.name ASC, n.age DESC'). Leave empty for no ordering.",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Order By"},
            )
        )

        # Output parameter for found nodes
        self.add_parameter(
            Parameter(
                name="found_nodes",
                output_type="list",
                default_value=[],
                tooltip="List of found nodes",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Found Nodes"},
            )
        )

        # Output parameter for node count
        self.add_parameter(
            Parameter(
                name="node_count",
                output_type="int",
                default_value=0,
                tooltip="Number of nodes found",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Node Count"},
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

        labels = [label.strip() for label in labels_str.split(",")]
        return [label for label in labels if label]

    def _parse_json_parameter(self, json_str: str, param_name: str) -> dict:
        """Parses JSON parameter string into a dictionary.

        Args:
            json_str: JSON string
            param_name: Name of parameter for error messages

        Returns:
            Dictionary of parsed data

        Raises:
            ValueError: If JSON is invalid
        """
        if not json_str.strip():
            return {}

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in {param_name}: {e!s}"
            raise ValueError(msg) from e

    def _build_match_query(self, labels: list[str]) -> str:
        """Builds the MATCH part of the query.

        Args:
            labels: List of node labels

        Returns:
            MATCH clause string
        """
        if labels:
            label_part = ":" + ":".join(labels)
        else:
            label_part = ""

        return f"MATCH (n{label_part})"

    def _build_where_conditions(self, property_filters: dict, where_clause: str) -> tuple[str, dict]:
        """Builds WHERE conditions and parameters.

        Args:
            property_filters: Dictionary of property filters
            where_clause: Custom WHERE clause

        Returns:
            Tuple of (where_string, parameters_dict)
        """
        conditions = []
        parameters = {}

        # Add property filter conditions
        for prop, value in property_filters.items():
            param_name = f"prop_{prop.replace('.', '_')}"
            conditions.append(f"n.{prop} = ${param_name}")
            parameters[param_name] = value

        # Add custom WHERE clause
        if where_clause.strip():
            conditions.append(f"({where_clause.strip()})")

        if conditions:
            where_string = "WHERE " + " AND ".join(conditions)
        else:
            where_string = ""

        return where_string, parameters

    def _convert_node_to_dict(self, node: Any) -> dict:
        """Converts a Neo4j node to a dictionary.

        Args:
            node: Neo4j node object

        Returns:
            Dictionary representation of the node
        """
        return {"id": node.id, "labels": list(node.labels), "properties": dict(node.items())}

    def _validate_session_or_raise(self, session: Neo4jSessionWrapper | None) -> None:
        """Validate session is not None or raise ValueError."""
        if session is None:
            msg = "Neo4j session is required"
            raise ValueError(msg)

    def process(self) -> None:
        """Processes the node to find nodes in the Neo4j database.

        Searches for nodes based on the specified criteria and returns
        the matching nodes.
        """
        try:
            # Get parameter values
            session = self.get_parameter_value("session")
            labels_str = self.get_parameter_value("labels")
            property_filters_str = self.get_parameter_value("property_filters")
            where_clause = self.get_parameter_value("where_clause")
            where_parameters_str = self.get_parameter_value("where_parameters")
            limit = self.get_parameter_value("limit")
            skip = self.get_parameter_value("skip")
            order_by = self.get_parameter_value("order_by")

            # Validate inputs
            self._validate_session_or_raise(session)

            # Parse parameters
            labels = self._parse_labels(labels_str)
            property_filters = self._parse_json_parameter(property_filters_str, "property_filters")
            where_parameters = self._parse_json_parameter(where_parameters_str, "where_parameters")

            # Build query
            match_clause = self._build_match_query(labels)
            where_clause_str, filter_parameters = self._build_where_conditions(property_filters, where_clause)

            # Combine parameters
            all_parameters = {**filter_parameters, **where_parameters}

            # Build complete query
            query_parts = [match_clause]

            if where_clause_str:
                query_parts.append(where_clause_str)

            query_parts.append("RETURN n")

            if order_by.strip():
                query_parts.append(f"ORDER BY {order_by.strip()}")

            if skip > 0:
                query_parts.append(f"SKIP {skip}")

            if limit > 0:
                query_parts.append(f"LIMIT {limit}")

            query = " ".join(query_parts)

            # Execute query
            result = session.run(query, all_parameters)

            # Process results
            found_nodes = []
            for record in result:
                node = record["n"]
                found_nodes.append(self._convert_node_to_dict(node))

            node_count = len(found_nodes)

            # Set output values
            self.parameter_output_values["found_nodes"] = found_nodes
            self.parameter_output_values["node_count"] = node_count

            success_msg = f"✓ Found {node_count} node(s)"
            self.parameter_output_values["message"] = success_msg

            # Hide message parameter since search is successful
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to find nodes: {e!s}"
            logger.error(error_msg)

            # Set error message and make it visible
            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            # Set outputs to empty/default values
            self.parameter_output_values["found_nodes"] = []
            self.parameter_output_values["node_count"] = 0

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the node search parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        # Validate JSON parameters
        try:
            property_filters_str = self.get_parameter_value("property_filters")
            if property_filters_str.strip():
                json.loads(property_filters_str)
        except json.JSONDecodeError as e:
            exceptions.append(ValueError(f"Invalid JSON in property_filters: {e!s}"))

        try:
            where_parameters_str = self.get_parameter_value("where_parameters")
            if where_parameters_str.strip():
                json.loads(where_parameters_str)
        except json.JSONDecodeError as e:
            exceptions.append(ValueError(f"Invalid JSON in where_parameters: {e!s}"))

        # Validate numeric parameters
        limit = self.get_parameter_value("limit")
        if limit < 0:
            exceptions.append(ValueError("Limit must be 0 or greater"))

        skip = self.get_parameter_value("skip")
        if skip < 0:
            exceptions.append(ValueError("Skip must be 0 or greater"))

        return exceptions if exceptions else None
