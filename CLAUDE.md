# Griptape Nodes Neo4j Library

A library providing Neo4j graph database integration nodes for Griptape Nodes.

## Project Structure

- `neo4j_nodes_library/` - Main library directory
  - `config/` - Neo4j driver configuration
  - `connection/` - Neo4j connection management
  - `crud/` - Create, Read, Update, Delete operations
  - `query/` - Query execution nodes
  - `griptape_nodes_library.json` - Node library metadata

## Development

- Built with Python 3.12
- Uses `uv` for dependency management
- Dependencies: `griptape-nodes` (from git), `neo4j>=5.25.0`

## Testing

No specific test commands configured yet.

## Notes

- All file paths in `griptape_nodes_library.json` are relative to the JSON file location
- Import statements use absolute imports from the library root directory
- Supports Neo4j database operations including node/relationship CRUD and Cypher queries