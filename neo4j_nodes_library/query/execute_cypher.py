"""Defines the ExecuteCypher node for executing Cypher queries against Neo4j.

This module provides the `ExecuteCypher` class, which allows users to execute
arbitrary Cypher queries against a Neo4j database using a provided session.
It supports parameterized queries and returns structured results.
"""

import json
import logging
from typing import Any

import neo4j

from griptape_nodes.exe_types.core_types import Parameter, ParameterMode
from griptape_nodes.exe_types.node_types import ControlNode

logger = logging.getLogger("griptape_nodes")


class ExecuteCypher(ControlNode):
    """Node for executing Cypher queries against a Neo4j database.

    This node takes a Neo4j session and executes Cypher queries with optional
    parameters. It returns the query results in a structured format that can
    be consumed by other nodes in the workflow.
    """

    def __init__(self, **kwargs) -> None:
        """Initializes the ExecuteCypher node.

        Sets up parameters for Cypher query execution including session input,
        query text, parameters, and result handling options.
        """
        super().__init__(**kwargs)

        # Input session
        self.add_parameter(
            Parameter(
                name="session",
                output_type="Any",
                default_value=None,
                tooltip="Neo4j session instance from Neo4j Connection node",
                ui_options={"display_name": "Neo4j Session"},
            )
        )

        # Cypher query
        self.add_parameter(
            Parameter(
                name="cypher_query",
                output_type="str",
                default_value="MATCH (n) RETURN n LIMIT 10",
                tooltip="Cypher query to execute",
                ui_options={"display_name": "Cypher Query", "multiline": True, "rows": 5},
            )
        )

        # Query parameters (JSON string)
        self.add_parameter(
            Parameter(
                name="parameters",
                output_type="str",
                default_value="{}",
                tooltip='Query parameters as JSON object (e.g., {"name": "Alice", "age": 30})',
                ui_options={"display_name": "Parameters", "multiline": True, "rows": 3},
            )
        )

        # Maximum number of records to return
        self.add_parameter(
            Parameter(
                name="limit",
                output_type="int",
                default_value=1000,
                tooltip="Maximum number of records to return (0 for no limit)",
                ui_options={"display_name": "Limit"},
            )
        )

        # Whether to consume all results immediately
        self.add_parameter(
            Parameter(
                name="consume_all",
                output_type="bool",
                default_value=True,
                tooltip="Whether to consume all results immediately (faster for small result sets)",
                ui_options={"display_name": "Consume All"},
            )
        )

        # Output parameter for query results
        self.add_parameter(
            Parameter(
                name="results",
                output_type="list",
                default_value=[],
                tooltip="Query results as list of dictionaries",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Results"},
            )
        )

        # Output parameter for result summary
        self.add_parameter(
            Parameter(
                name="summary",
                output_type="dict",
                default_value={},
                tooltip="Query execution summary and statistics",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Summary"},
            )
        )

        # Output parameter for record count
        self.add_parameter(
            Parameter(
                name="record_count",
                output_type="int",
                default_value=0,
                tooltip="Number of records returned",
                allowed_modes={ParameterMode.OUTPUT},
                ui_options={"display_name": "Record Count"},
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

    def _convert_neo4j_value(self, value: Any) -> Any:  # noqa: PLR0911 - Multiple returns needed for efficient type dispatching
        """Converts Neo4j values to JSON-serializable Python types.

        Args:
            value: Neo4j value to convert

        Returns:
            JSON-serializable Python value
        """
        if value is None:
            return None

        # Check for specific Neo4j types first
        if self._is_neo4j_node(value):
            return self._convert_neo4j_node(value)

        if self._is_neo4j_relationship(value):
            return self._convert_neo4j_relationship(value)

        if self._is_neo4j_path(value):
            return self._convert_neo4j_path(value)

        # Handle standard Python collections recursively
        if isinstance(value, list):
            return [self._convert_neo4j_value(item) for item in value]

        if isinstance(value, dict):
            return {k: self._convert_neo4j_value(v) for k, v in value.items()}

        # Handle primitive types and fallback
        return self._convert_primitive_or_fallback(value)

    def _is_neo4j_node(self, value: Any) -> bool:
        """Check if value is a Neo4j node object."""
        return hasattr(value, "labels") and hasattr(value, "items")

    def _is_neo4j_relationship(self, value: Any) -> bool:
        """Check if value is a Neo4j relationship object."""
        return hasattr(value, "type") and hasattr(value, "start_node")

    def _is_neo4j_path(self, value: Any) -> bool:
        """Check if value is a Neo4j path object."""
        return hasattr(value, "nodes") and hasattr(value, "relationships")

    def _safe_get_labels(self, node: Any) -> list[str]:
        """Safely extract labels from a Neo4j node."""
        try:
            raw_labels = getattr(node, "labels", set())
            if isinstance(raw_labels, (frozenset, set)) or (
                hasattr(raw_labels, "__iter__") and not isinstance(raw_labels, str)
            ):
                return list(raw_labels)
            return [str(raw_labels)]
        except Exception:
            return []

    def _safe_get_properties(self, obj: Any) -> dict[str, Any]:
        """Safely extract properties from a Neo4j object."""
        try:
            if hasattr(obj, "items") and callable(obj.items):
                return dict(obj.items())
        except Exception:
            return {}
        else:
            return {}

    def _convert_neo4j_node(self, node: Any) -> dict[str, Any]:
        """Convert a Neo4j node to a dictionary."""
        try:
            node_id = getattr(node, "id", "unknown")
            element_id = getattr(node, "element_id", str(node_id))
            labels = self._safe_get_labels(node)
            properties = self._safe_get_properties(node)
        except Exception:
            return {
                "id": "error",
                "element_id": "error",
                "labels": [],
                "properties": {},
                "error": "Node conversion failed",
            }
        else:
            return {
                "id": node_id,
                "element_id": element_id,
                "labels": labels,
                "properties": properties,
            }

    def _convert_neo4j_relationship(self, rel: Any) -> dict[str, Any]:
        """Convert a Neo4j relationship to a dictionary."""
        try:
            return {
                "id": rel.id,
                "type": rel.type,
                "start_node_id": rel.start_node.id,
                "end_node_id": rel.end_node.id,
                "properties": self._safe_get_properties(rel),
            }
        except Exception:
            return {
                "id": getattr(rel, "id", "unknown"),
                "type": "unknown",
                "error": "Failed to convert relationship",
            }

    def _convert_neo4j_path(self, path: Any) -> dict[str, Any]:
        """Convert a Neo4j path to a dictionary."""
        return {
            "nodes": [self._convert_neo4j_value(node) for node in path.nodes],
            "relationships": [self._convert_neo4j_value(rel) for rel in path.relationships],
            "length": len(path.relationships),
        }

    def _convert_primitive_or_fallback(self, value: Any) -> Any:
        """Convert primitive types or fallback to string representation."""
        try:
            # Test if value is JSON serializable
            json.dumps(value)
        except (TypeError, ValueError):
            # If not serializable, convert to string
            return str(value)
        else:
            return value

    def _parse_parameters(self, parameters_str: str) -> dict:
        """Parses JSON parameter string into a dictionary.

        Args:
            parameters_str: JSON string containing query parameters

        Returns:
            Dictionary of parsed parameters

        Raises:
            ValueError: If JSON is invalid
        """
        if not parameters_str.strip():
            return {}

        try:
            return json.loads(parameters_str)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON in parameters: {e!s}"
            raise ValueError(msg) from e

    def process(self) -> None:
        """Processes the node to execute the Cypher query.

        Executes the provided Cypher query against the Neo4j session with
        the specified parameters and returns structured results.
        """
        try:
            # Get parameter values
            session = self.get_parameter_value("session")
            cypher_query = self.get_parameter_value("cypher_query")
            parameters_str = self.get_parameter_value("parameters")
            limit = self.get_parameter_value("limit")
            consume_all = self.get_parameter_value("consume_all")

            # Validate inputs
            self._validate_inputs(session, cypher_query)

            # Parse parameters
            parameters = self._parse_parameters(parameters_str)

            # Execute query
            result = session.run(cypher_query, parameters)

            # Process results based on consume_all setting
            records = []
            record_count = 0
            if consume_all:
                record_count = self._process_results_consume_all(result, limit, records, record_count)
            else:
                record_count = self._process_results_streaming(result, limit, records, record_count)

            # Get result summary and build summary info
            summary = result.consume()
            summary_info = self._build_summary_info(summary)

            # Set success outputs
            self._set_success_outputs(records, record_count, summary_info)

        except Exception as e:
            # Handle potential Neo4j objects in error messages
            try:
                # Check if this is a conversion error by looking at the exception type and message
                if "Node element_id=" in str(e):
                    error_str = f"Query executed successfully but failed to convert results to JSON. This may be due to complex Neo4j objects in the query results. Original error: {type(e).__name__}"
                else:
                    error_str = str(e)
            except Exception:
                error_str = f"{type(e).__name__}: [Error converting exception to string]"

            self._set_error_outputs(error_str)

    def validate_before_workflow_run(self) -> list[Exception] | None:
        """Validates the query execution parameters.

        Returns:
            List of validation exceptions or None if validation passes
        """
        exceptions = []

        # Skip session validation here - it will be validated when the node runs
        # after dependency resolution has provided the session

        # Validate query
        cypher_query = self.get_parameter_value("cypher_query")
        if not cypher_query.strip():
            exceptions.append(ValueError("Cypher query cannot be empty"))

        # Validate parameters JSON
        try:
            parameters_str = self.get_parameter_value("parameters")
            if parameters_str.strip():
                json.loads(parameters_str)
        except json.JSONDecodeError as e:
            exceptions.append(ValueError(f"Invalid JSON in parameters: {e!s}"))

        # Validate limit
        limit = self.get_parameter_value("limit")
        if limit < 0:
            exceptions.append(ValueError("Limit must be 0 or greater"))

        return exceptions if exceptions else None

    def _validate_inputs(self, session: neo4j.Session | None, cypher_query: str) -> None:
        """Validate input parameters before execution."""
        if session is None:
            msg = "Neo4j session is required"
            raise ValueError(msg)

        if not cypher_query.strip():
            msg = "Cypher query cannot be empty"
            raise ValueError(msg)

    def _convert_single_record(self, record: Any, record_count: int) -> dict[str, Any]:
        """Convert a single Neo4j record to a dictionary."""
        try:
            record_dict = {}
            # Must use record.keys() instead of direct iteration over record
            # because Neo4j Record objects don't support direct iteration in the same way as dicts
            for key in record.keys():  # noqa: SIM118
                value = record.get(key)
                record_dict[key] = self._convert_neo4j_value(value)
        except Exception as conversion_error:
            logger.warning("Failed to convert record %d: %s", record_count, type(conversion_error).__name__)
            return {"error": f"Failed to convert record: {type(conversion_error).__name__}"}
        else:
            return record_dict

    def _process_results_consume_all(self, result: Any, limit: int, records: list[dict], record_count: int) -> int:
        """Process results by consuming all at once."""
        for record in result:
            if limit > 0 and record_count >= limit:
                break

            converted_record = self._convert_single_record(record, record_count)
            records.append(converted_record)
            record_count += 1

        return record_count

    def _process_results_streaming(self, result: Any, limit: int, records: list[dict], record_count: int) -> int:
        """Process results by streaming one by one."""
        try:
            for record in result:
                if limit > 0 and record_count >= limit:
                    break

                converted_record = self._convert_single_record(record, record_count)
                records.append(converted_record)
                record_count += 1
        except Exception as e:
            logger.warning("Error during result streaming: %s", e)

        return record_count

    def _build_summary_info(self, summary: neo4j.ResultSummary) -> dict[str, Any]:
        """Build summary information from Neo4j result summary."""
        return {
            "query_type": summary.query_type if hasattr(summary, "query_type") else "unknown",
            "counters": {
                "nodes_created": summary.counters.nodes_created,
                "nodes_deleted": summary.counters.nodes_deleted,
                "relationships_created": summary.counters.relationships_created,
                "relationships_deleted": summary.counters.relationships_deleted,
                "properties_set": summary.counters.properties_set,
                "labels_added": summary.counters.labels_added,
                "labels_removed": summary.counters.labels_removed,
                "indexes_added": summary.counters.indexes_added,
                "indexes_removed": summary.counters.indexes_removed,
                "constraints_added": summary.counters.constraints_added,
                "constraints_removed": summary.counters.constraints_removed,
            }
            if hasattr(summary, "counters")
            else {},
            "result_available_after": summary.result_available_after
            if hasattr(summary, "result_available_after")
            else 0,
            "result_consumed_after": summary.result_consumed_after if hasattr(summary, "result_consumed_after") else 0,
        }

    def _set_success_outputs(
        self, records: list[dict[str, Any]], record_count: int, summary_info: dict[str, Any]
    ) -> None:
        """Set output parameters for successful execution."""
        self.parameter_output_values["results"] = records
        self.parameter_output_values["summary"] = summary_info
        self.parameter_output_values["record_count"] = record_count

        success_msg = f"✓ Query executed successfully. Returned {record_count} records."
        self.parameter_output_values["message"] = success_msg

        # Hide message parameter since execution is successful
        message_param = self.get_parameter_by_name("message")
        if message_param:
            message_param._ui_options["hide"] = True

    def _set_error_outputs(self, error_str: str) -> None:
        """Set output parameters for failed execution."""
        error_msg = f"❌ Failed to execute Cypher query: {error_str}"
        logger.error(error_msg)

        # Set error message and make it visible
        self.parameter_output_values["message"] = error_msg
        message_param = self.get_parameter_by_name("message")
        if message_param:
            message_param._ui_options["hide"] = False

        # Set outputs to empty/default values
        self.parameter_output_values["results"] = []
        self.parameter_output_values["summary"] = {}
        self.parameter_output_values["record_count"] = 0
