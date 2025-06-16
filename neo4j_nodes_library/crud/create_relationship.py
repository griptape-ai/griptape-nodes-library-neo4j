"""Defines the CreateRelationship node for creating relationships in Neo4j.

This module provides the `CreateRelationship` class, which allows users to create
relationships between existing nodes in a Neo4j database with specified type and properties.
"""

import json
import logging
from typing import Any

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode
from connection.neo4j_connection import Neo4jSessionWrapper

logger = logging.getLogger("griptape_nodes")


class CreateRelationship(ControlNode):
    """Node for creating relationships between nodes in a Neo4j database.

    This node takes a Neo4j session and creates a relationship between two
    existing nodes with specified type and properties.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the CreateRelationship node.

        Sets up parameters for relationship creation including session input,
        node selection criteria, relationship type, and properties.
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

        # Start node criteria
        self.add_parameter(
            Parameter(
                name="start_node_labels",
                output_type="str",
                default_value="Person",
                tooltip="Labels for start node, separated by commas",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Start Node Labels"},
            )
        )

        self.add_parameter(
            Parameter(
                name="start_node_properties",
                output_type="str",
                default_value='{"name": "Alice"}',
                tooltip="Properties to match start node as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Start Node Properties", "multiline": True, "rows": 3},
            )
        )

        # End node criteria
        self.add_parameter(
            Parameter(
                name="end_node_labels",
                output_type="str",
                default_value="Person",
                tooltip="Labels for end node, separated by commas",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "End Node Labels"},
            )
        )

        self.add_parameter(
            Parameter(
                name="end_node_properties",
                output_type="str",
                default_value='{"name": "Bob"}',
                tooltip="Properties to match end node as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "End Node Properties", "multiline": True, "rows": 3},
            )
        )

        # Relationship type
        self.add_parameter(
            Parameter(
                name="relationship_type",
                output_type="str",
                default_value="KNOWS",
                tooltip="Type/label of the relationship",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Relationship Type"},
            )
        )

        # Relationship properties
        self.add_parameter(
            Parameter(
                name="relationship_properties",
                output_type="str",
                default_value='{"since": "2020"}',
                tooltip="Properties for the relationship as JSON object",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Relationship Properties", "multiline": True, "rows": 3},
            )
        )

        # Create if nodes don't exist
        self.add_parameter(
            Parameter(
                name="create_missing_nodes",
                output_type="bool",
                default_value=False,
                tooltip="Create nodes if they don't exist (uses MERGE instead of MATCH)",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Create Missing Nodes"},
            )
        )

        # Whether to return the created relationship
        self.add_parameter(
            Parameter(
                name="return_relationship",
                output_type="bool",
                default_value=True,
                tooltip="Whether to return the created relationship data",
                allowed_modes={ParameterMode.INPUT},
                ui_options={"display_name": "Return Relationship"},
            )
        )

        # Output parameter for created relationship
        self.add_parameter(
            Parameter(
                name="created_relationship",
                output_type="dict",
                default_value={},
                tooltip="Information about the created relationship",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Created Relationship"},
            )
        )

        # Output parameter for relationship ID
        self.add_parameter(
            Parameter(
                name="relationship_id",
                output_type="int",
                default_value=0,
                tooltip="ID of the created relationship",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Relationship ID"},
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

    def _parse_properties(self, properties_str: str, param_name: str) -> dict:
        """Parses JSON property string into a dictionary.

        Args:
            properties_str: JSON string containing properties
            param_name: Name of parameter for error messages

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
            msg = f"Invalid JSON in {param_name}: {e!s}"
            raise ValueError(msg) from e

    def _build_node_pattern(self, variable: str, labels: list[str], properties: dict) -> tuple[str, dict]:
        """Builds a node pattern for the query.

        Args:
            variable: Variable name for the node
            labels: List of node labels
            properties: Dictionary of node properties

        Returns:
            Tuple of (pattern_string, parameters_dict)
        """
        if labels:
            label_part = ":" + ":".join(labels)
        else:
            label_part = ""

        if properties:
            # Create parameter names for properties
            params = {}
            conditions = []
            for prop, value in properties.items():
                param_name = f"{variable}_{prop.replace('.', '_')}"
                conditions.append(f"{variable}.{prop} = ${param_name}")
                params[param_name] = value

            # For MATCH, we'll add WHERE conditions
            pattern = f"({variable}{label_part})"
            where_conditions = " AND ".join(conditions)
            return pattern, params, where_conditions
        pattern = f"({variable}{label_part})"
        return pattern, {}, ""

    def _build_create_query(  # noqa: PLR0913
        self,
        start_labels: list[str],
        start_props: dict,
        end_labels: list[str],
        end_props: dict,
        rel_type: str,
        rel_props: dict,
        *,
        create_missing: bool,
        return_rel: bool,
    ) -> tuple[str, dict]:
        # Complex Neo4j relationship queries require many parameters for start/end nodes and relationship
        """Builds the complete CREATE relationship query.

        Args:
            start_labels: Start node labels
            start_props: Start node properties
            end_labels: End node labels
            end_props: End node properties
            rel_type: Relationship type
            rel_props: Relationship properties
            create_missing: Whether to create missing nodes
            return_rel: Whether to return the relationship

        Returns:
            Tuple of (query_string, parameters_dict)
        """
        all_params = {}

        # Choose MATCH or MERGE for nodes

        # Build start node pattern
        start_pattern, start_params, start_where = self._build_node_pattern("start", start_labels, start_props)
        all_params.update(start_params)

        # Build end node pattern
        end_pattern, end_params, end_where = self._build_node_pattern("end", end_labels, end_props)
        all_params.update(end_params)

        # Build relationship properties
        rel_param_name = "rel_props"
        all_params[rel_param_name] = rel_props

        # Build query
        query_parts = []

        if create_missing:
            # Use MERGE for both nodes and relationship
            query_parts.append(f"MERGE {start_pattern}")
            if start_where:
                # For MERGE, we need to set properties after
                query_parts.append(f"MERGE {end_pattern}")
            else:
                query_parts.append(f"MERGE {end_pattern}")

            query_parts.append(f"MERGE (start)-[r:{rel_type} ${rel_param_name}]->(end)")
        else:
            # Use MATCH for nodes, CREATE for relationship
            query_parts.append(f"MATCH {start_pattern}")
            if start_where:
                query_parts.append(f"WHERE {start_where}")

            query_parts.append(f"MATCH {end_pattern}")
            if end_where:
                if start_where:
                    query_parts[-1] = f"MATCH {end_pattern} WHERE {end_where}"
                else:
                    query_parts.append(f"WHERE {end_where}")

            query_parts.append(f"CREATE (start)-[r:{rel_type} ${rel_param_name}]->(end)")

        if return_rel:
            query_parts.append("RETURN r")

        query = " ".join(query_parts)
        return query, all_params

    def _convert_relationship_to_dict(self, relationship: Any) -> dict:
        """Converts a Neo4j relationship to a dictionary.

        Args:
            relationship: Neo4j relationship object

        Returns:
            Dictionary representation of the relationship
        """
        return {
            "id": relationship.id,
            "type": relationship.type,
            "start_node_id": relationship.start_node.id,
            "end_node_id": relationship.end_node.id,
            "properties": dict(relationship.items()),
        }

    def _validate_session_or_raise(self, session: Neo4jSessionWrapper | None) -> None:
        """Validate session is not None or raise ValueError."""
        if session is None:
            msg = "Neo4j session is required"
            raise ValueError(msg)

    def _validate_relationship_type_or_raise(self, rel_type: str) -> None:
        """Validate relationship type is not empty or raise ValueError."""
        if not rel_type.strip():
            msg = "Relationship type is required"
            raise ValueError(msg)

    def process(self) -> None:  # noqa: PLR0915
        # Complex Neo4j relationship creation requires comprehensive parameter validation and query building
        """Processes the node to create a relationship in the Neo4j database.

        Creates a relationship between two nodes based on the specified criteria.
        """
        try:
            # Get parameter values
            session = self.get_parameter_value("session")
            start_labels_str = self.get_parameter_value("start_node_labels")
            start_props_str = self.get_parameter_value("start_node_properties")
            end_labels_str = self.get_parameter_value("end_node_labels")
            end_props_str = self.get_parameter_value("end_node_properties")
            rel_type = self.get_parameter_value("relationship_type")
            rel_props_str = self.get_parameter_value("relationship_properties")
            create_missing = self.get_parameter_value("create_missing_nodes")
            return_rel = self.get_parameter_value("return_relationship")

            # Validate inputs
            self._validate_session_or_raise(session)
            self._validate_relationship_type_or_raise(rel_type)

            # Parse parameters
            start_labels = self._parse_labels(start_labels_str)
            start_props = self._parse_properties(start_props_str, "start_node_properties")
            end_labels = self._parse_labels(end_labels_str)
            end_props = self._parse_properties(end_props_str, "end_node_properties")
            rel_props = self._parse_properties(rel_props_str, "relationship_properties")

            # Build and execute query
            query, parameters = self._build_create_query(
                start_labels, start_props, end_labels, end_props, rel_type, rel_props,
                create_missing=create_missing, return_rel=return_rel
            )

            result = session.run(query, parameters)

            # Process results
            created_relationship = {}
            relationship_id = 0

            if return_rel:
                record = result.single()
                if record:
                    rel = record["r"]
                    created_relationship = self._convert_relationship_to_dict(rel)
                    relationship_id = rel.id

            # Consume the result to get summary
            summary = result.consume()
            relationships_created = summary.counters.relationships_created
            nodes_created = summary.counters.nodes_created

            # Set output values
            self.parameter_output_values["created_relationship"] = created_relationship
            self.parameter_output_values["relationship_id"] = relationship_id

            success_msg = f"✓ Successfully created {relationships_created} relationship(s)"
            if nodes_created > 0:
                success_msg += f" and {nodes_created} node(s)"
            if relationship_id > 0:
                success_msg += f" with ID {relationship_id}"

            self.parameter_output_values["message"] = success_msg

            # Hide message parameter since creation is successful
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = True

        except Exception as e:
            error_msg = f"❌ Failed to create relationship: {e!s}"
            logger.error(error_msg)

            # Set error message and make it visible
            self.parameter_output_values["message"] = error_msg
            message_param = self.get_parameter_by_name("message")
            if message_param:
                message_param._ui_options["hide"] = False

            # Set outputs to empty/default values
            self.parameter_output_values["created_relationship"] = {}
            self.parameter_output_values["relationship_id"] = 0

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the relationship creation parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        # Validate relationship type
        rel_type = self.get_parameter_value("relationship_type")
        if not rel_type.strip():
            exceptions.append(ValueError("Relationship type is required"))

        # Validate JSON parameters
        json_params = [
            ("start_node_properties", self.get_parameter_value("start_node_properties")),
            ("end_node_properties", self.get_parameter_value("end_node_properties")),
            ("relationship_properties", self.get_parameter_value("relationship_properties")),
        ]

        for param_name, param_value in json_params:
            try:
                if param_value.strip():
                    json.loads(param_value)
            except json.JSONDecodeError as e:
                exceptions.append(ValueError(f"Invalid JSON in {param_name}: {e!s}"))

        return exceptions if exceptions else None
