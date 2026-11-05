import importlib.util
import sys
from pathlib import Path

import neo4j
import pytest
from griptape_nodes.exe_types.core_types import NodeError
from neo4j_errors import missing_session_error, node_error_from_exception, parse_json_object

from tests.unit.nodes.fake_bolt import FakeBolt

LIBRARY = Path(__file__).parents[3] / "neo4j_nodes_library"


@pytest.fixture(scope="module")
def server():
    return FakeBolt(good_password="good")


def run(server, query, *, password="good", **error_kwargs):
    """Run a query through the real driver and return the NodeError for what it raises."""
    driver = neo4j.GraphDatabase.driver(f"bolt://127.0.0.1:{server.port}", auth=neo4j.basic_auth("neo4j", password))
    try:
        with driver.session() as session:
            list(session.run(query))
    except Exception as e:  # noqa: BLE001
        return node_error_from_exception(e, action="run the query", **error_kwargs)
    finally:
        driver.close()
    pytest.fail("the query did not fail")


def test_syntax_error_carries_the_server_code_and_message(server):
    error = run(server, "BAD query")
    assert str(error).startswith("Neo4j could not parse the Cypher query: Invalid input 'BAD'")
    assert "\n" not in str(error)
    assert error.fields == {"error_code": "Neo.ClientError.Statement.SyntaxError"}
    assert error.response is not None
    assert error.response["message"].endswith("^")
    assert [link.label for link in error.links] == ["Cypher manual"]


def test_other_server_errors_use_the_servers_reason(server):
    error = run(server, "READONLY")
    assert str(error) == (
        "Neo4j could not run the query: Writing in read access mode not allowed. Attempted write to neo4j"
    )
    assert error.fields == {"error_code": "Neo.ClientError.Statement.AccessMode"}


def test_refused_credentials_point_at_the_driver_node(server):
    error = run(server, "RETURN 1", password="wrong")
    assert "Neo4j Driver node" in str(error)
    assert error.fields == {"error_code": "Neo.ClientError.Security.Unauthorized"}
    assert error.links == []


def test_refused_default_credentials_link_to_the_settings(server):
    error = run(server, "RETURN 1", password="wrong", default_driver=True)
    assert [link.url for link in error.links] == [
        "#settings-secrets?filter=NEO4J_USERNAME",
        "#settings-secrets?filter=NEO4J_PASSWORD",
    ]


def test_unreachable_server_names_the_uri():
    uri = "bolt://127.0.0.1:1"
    driver = neo4j.GraphDatabase.driver(uri, auth=neo4j.basic_auth("neo4j", "x"), connection_timeout=2.0)
    with pytest.raises(neo4j.exceptions.ServiceUnavailable) as raised, driver.session() as session:
        session.run("RETURN 1").consume()
    error = node_error_from_exception(raised.value, action="connect", uri=uri)
    assert str(error).startswith("Cannot connect to the Neo4j server at bolt://127.0.0.1:1. Check that")
    assert error.response is not None
    assert error.response["error"] == "ServiceUnavailable"
    assert error.links == []


def test_unsupported_scheme_is_a_settings_error():
    with pytest.raises(neo4j.exceptions.ConfigurationError) as raised:
        neo4j.GraphDatabase.driver("foo://127.0.0.1:1")
    error = node_error_from_exception(raised.value, action="connect")
    assert str(error) == "The Neo4j connection settings are not valid. Check 'Database URI', then run the node again."
    assert error.response is not None
    assert error.response["message"].startswith("URI scheme 'foo' is not supported")


def test_a_node_error_passes_through():
    original = missing_session_error()
    assert node_error_from_exception(original, action="run the query") is original


def test_parse_json_object():
    assert parse_json_object("  ", "Properties") == {}
    assert parse_json_object('{"name": "Alice"}', "Properties") == {"name": "Alice"}
    with pytest.raises(NodeError, match=r"'Properties' is not valid JSON: .* \(line 1, column 2\)") as bad:
        parse_json_object("{bad", "Properties")
    assert bad.value.fields == {"parameter": "Properties"}
    with pytest.raises(NodeError, match="'Properties' must be a JSON object"):
        parse_json_object("[1]", "Properties")


def load_node(relative_path: str, class_name: str):
    """Load a node file the way the engine does: on its own, with the library folder on sys.path."""
    spec = importlib.util.spec_from_file_location(f"standalone_{class_name}", LIBRARY / relative_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(LIBRARY))
    sys.modules["neo4j_nodes_library"] = None
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(LIBRARY))
        del sys.modules["neo4j_nodes_library"]
    return getattr(module, class_name)


NODE_FILES = [
    ("config/neo4j_driver.py", "Neo4jDriver"),
    ("connection/neo4j_connection.py", "Neo4jConnection"),
    ("query/execute_cypher.py", "ExecuteCypher"),
    ("crud/create_node.py", "CreateNode"),
    ("crud/create_relationship.py", "CreateRelationship"),
    ("crud/delete_node.py", "DeleteNode"),
    ("crud/find_nodes.py", "FindNodes"),
    ("crud/update_node.py", "UpdateNode"),
]


@pytest.mark.parametrize(("path", "class_name"), NODE_FILES)
def test_node_files_load_standalone(path, class_name):
    assert load_node(path, class_name).__name__ == class_name


def stub_node(cls, values):
    node = object.__new__(cls)
    node.get_parameter_value = values.__getitem__
    return node


def test_validation_reports_every_bad_input_when_the_node_runs():
    node = stub_node(
        load_node("crud/find_nodes.py", "FindNodes"),
        {"property_filters": "{bad", "where_parameters": "[1]", "limit": -1, "skip": -2},
    )
    errors = node.validate_before_node_run()
    assert all(isinstance(e, NodeError) for e in errors)
    parameters = [e.fields["parameter"] for e in errors]
    assert parameters == ["Property Filters", "WHERE Parameters", "Limit", "Skip"]


def test_delete_requires_confirmation():
    node = stub_node(load_node("crud/delete_node.py", "DeleteNode"), {"confirm_delete": False, "match_properties": ""})
    (error,) = node.validate_before_node_run()
    assert str(error) == "Deleting is not confirmed. Turn on 'Confirm Delete' and run the node again."


def test_crud_node_without_a_session_raises_node_error():
    node = stub_node(load_node("crud/create_node.py", "CreateNode"), {"session": None})
    with pytest.raises(NodeError, match="No Neo4j session is connected"):
        node._validate_session_or_raise(None)
