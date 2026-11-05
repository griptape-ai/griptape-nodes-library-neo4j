"""Maps Neo4j driver failures to `NodeError`s. The nodes load as standalone files, so they import this by bare name."""

from __future__ import annotations

import json
from typing import Any

import neo4j.exceptions as neo4j_exceptions
from griptape_nodes.exe_types.core_types import NodeError, NodeErrorLink

CYPHER_MANUAL_URL = "https://neo4j.com/docs/cypher-manual/current/"
# The default driver reads these library settings, which resolve from Settings -> API Keys & Secrets.
CREDENTIAL_SETTINGS = ("NEO4J_USERNAME", "NEO4J_PASSWORD")


def missing_session_error() -> NodeError:
    return NodeError(
        "No Neo4j session is connected. Connect the output of a Neo4j Connection node to 'Neo4j Session', "
        "then run the node again."
    )


def parse_json_object(text: str, label: str) -> dict[str, Any]:
    """Parse a parameter that holds a JSON object. Empty text is an empty object."""
    if not text.strip():
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError as e:
        msg = (
            f"'{label}' is not valid JSON: {e.msg} (line {e.lineno}, column {e.colno}). Fix it and run the node again."
        )
        raise NodeError(msg, fields={"parameter": label}) from e
    if not isinstance(value, dict):
        msg = f'\'{label}\' must be a JSON object such as {{"name": "Alice"}}. Fix it and run the node again.'
        raise NodeError(msg, fields={"parameter": label})
    return value


def node_error_from_exception(
    error: Exception, *, action: str, uri: str | None = None, default_driver: bool = False
) -> NodeError:
    """The `NodeError` for an exception from the Neo4j driver, with the server's code and message attached.

    `action` is a verb phrase such as "create the node". `default_driver` is True when the connection came
    from the library settings, so a credential failure links to them.
    """
    if isinstance(error, NodeError):
        return error
    if isinstance(error, neo4j_exceptions.Neo4jError):
        fields = {"error_code": error.code}
        response = {"code": error.code, "message": error.message}
        reason = error.message.strip().splitlines()[0] if error.message.strip() else error.code
        if isinstance(error, neo4j_exceptions.AuthError):
            where = (
                "NEO4J_USERNAME and NEO4J_PASSWORD in Settings"
                if default_driver
                else "'Auth Type', 'Username' and 'Password' on the Neo4j Driver node"
            )
            links = [NodeErrorLink.from_secret(name, f"Check {name}") for name in CREDENTIAL_SETTINGS]
            message = f"Neo4j rejected the username and password. Check {where}, then run the node again."
            return NodeError(message, fields=fields, response=response, links=links if default_driver else None)
        if isinstance(error, neo4j_exceptions.CypherSyntaxError):
            message = f"Neo4j could not parse the Cypher query: {reason}. Fix the query and run the node again."
            links = [NodeErrorLink(label="Cypher manual", url=CYPHER_MANUAL_URL)]
            return NodeError(message, fields=fields, response=response, links=links)
        return NodeError(f"Neo4j could not {action}: {reason}", fields=fields, response=response)
    if isinstance(error, neo4j_exceptions.ConfigurationError):
        message = "The Neo4j connection settings are not valid. Check 'Database URI', then run the node again."
        return NodeError(message, response={"error": type(error).__name__, "message": str(error)})
    if isinstance(error, (neo4j_exceptions.DriverError, OSError)):
        target = f" at {uri}" if uri else ""
        message = (
            f"Cannot connect to the Neo4j server{target}. "
            "Check that the server is running and that the database URI is correct, then run the node again."
        )
        links = [NodeErrorLink.from_secret("NEO4J_URI", "Check NEO4J_URI")] if default_driver else None
        return NodeError(message, response={"error": type(error).__name__, "message": str(error)}, links=links)
    return NodeError(f"Could not {action}: {error}", response={"error": type(error).__name__, "message": str(error)})
