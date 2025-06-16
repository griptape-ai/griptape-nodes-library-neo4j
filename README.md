# Griptape Nodes: Neo4j Library

A comprehensive Neo4j graph database integration library for [Griptape Nodes](https://www.griptapenodes.com/), providing complete CRUD operations, query execution, and connection management for Neo4j databases.

## Overview

This library contains 8 specialized nodes that enable seamless interaction with Neo4j graph databases:

### Connection & Configuration
- **Neo4j Driver** - Configure database connections with authentication
- **Neo4j Connection** - Manage database sessions

### Query Execution  
- **Execute Cypher** - Run arbitrary Cypher queries

### CRUD Operations
- **Create Node** - Add nodes with labels and properties
- **Create Relationship** - Connect nodes with typed relationships
- **Find Nodes** - Search and retrieve nodes with flexible filtering
- **Update Node** - Modify node properties
- **Delete Node** - Remove nodes and relationships

## Quick Start

### Prerequisites
- Neo4j database (local or remote)
- Neo4j credentials (URI, username, password)
- Griptape Nodes Engine installed

### Installation

1. Clone this repository to your Griptape Nodes workspace:
   ```bash
   cd $(gtn config | grep workspace_directory | cut -d'"' -f4)
   git clone https://github.com/your-repo/griptape-nodes-library-neo4j.git
   ```

2. Add the library to your Griptape Nodes engine:
   - Copy the path to `neo4j_nodes_library/griptape_nodes_library.json`
   - In Griptape Nodes, go to Settings → App Events → Libraries to Register
   - Add the JSON file path

### Environment Setup

Configure your Neo4j connection by setting these environment variables:
```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_DATABASE="neo4j"  
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="your-password"
```

## Node Reference

### Connection & Configuration Nodes

#### Neo4j Driver
Configures database connections with comprehensive authentication support.

**Key Features:**
- Multiple authentication methods (basic, bearer token, custom, none)
- Connection pooling and timeout configuration
- Built-in connection testing
- Driver registry to prevent memory leaks

**Parameters:**
- `uri`: Database URI (default: bolt://localhost:7687)
- `database`: Database name (default: neo4j)  
- `auth_type`: Authentication method
- `username`/`password`: Credentials for basic auth
- `connection_timeout`: Connection timeout in seconds

#### Neo4j Connection  
Creates and manages database sessions from a configured driver.

**Key Features:**
- Automatic driver creation if none provided
- Read/write session modes
- Configurable fetch sizes for streaming
- Session wrapper for lifecycle management

**Parameters:**
- `driver`: Optional Neo4j driver instance
- `session_mode`: read or write (default: write)
- `fetch_size`: Records per batch (default: 1000)

### Query Execution

#### Execute Cypher
Runs arbitrary Cypher queries with full result processing.

**Key Features:**
- Parameterized query support
- Result limiting and streaming
- Neo4j object to JSON conversion
- Comprehensive execution statistics

**Parameters:**
- `session`: Neo4j session instance
- `cypher_query`: Cypher query to execute
- `parameters`: Query parameters as JSON
- `limit`: Maximum records to return
- `consume_all`: Process all results immediately

### CRUD Operations

#### Create Node
Creates nodes with labels and properties.

**Parameters:**
- `labels`: Comma-separated node labels
- `properties`: Node properties as JSON
- `return_node`: Return created node data

#### Create Relationship
Connects nodes with typed relationships.

**Parameters:**
- `start_node_labels`/`start_node_properties`: Start node criteria
- `end_node_labels`/`end_node_properties`: End node criteria  
- `relationship_type`: Relationship type name
- `relationship_properties`: Relationship properties as JSON
- `create_missing_nodes`: Create nodes if they don't exist

#### Find Nodes
Searches nodes with flexible filtering options.

**Parameters:**
- `labels`: Labels to match (optional)
- `property_filters`: Property filters as JSON
- `where_clause`: Custom WHERE clause
- `limit`/`skip`: Pagination controls
- `order_by`: Result ordering

#### Update Node
Modifies node properties with SET/REMOVE operations.

**Parameters:**
- `match_labels`/`match_properties`: Node matching criteria
- `update_properties`: Properties to update as JSON
- `update_mode`: SET (add/update) or REMOVE

#### Delete Node
Removes nodes with safety confirmation.

**Parameters:**
- `match_labels`/`match_properties`: Node matching criteria
- `detach_delete`: Remove connected relationships
- `confirm_delete`: Safety confirmation required

## Example Workflows

### Basic Graph Creation
1. **Neo4j Driver** → Configure connection
2. **Neo4j Connection** → Create session  
3. **Create Node** → Add person nodes
4. **Create Relationship** → Connect people

### Data Analysis Pipeline
1. **Neo4j Connection** → Establish session
2. **Find Nodes** → Query specific nodes
3. **Execute Cypher** → Run analytics queries
4. **Update Node** → Store computed results

### Graph Maintenance
1. **Neo4j Connection** → Connect to database
2. **Find Nodes** → Locate outdated data
3. **Update Node** → Refresh properties
4. **Delete Node** → Remove obsolete nodes

## Common Patterns

### Error Handling
All nodes provide comprehensive error messages with troubleshooting guidance. Check the `message` output for detailed error information.

### Authentication
Set environment variables for automatic authentication:
```bash
export NEO4J_URI="bolt://your-server:7687"
export NEO4J_USERNAME="your-username"  
export NEO4J_PASSWORD="your-password"
```

### JSON Data Format
Properties should be provided as valid JSON strings:
```json
{"name": "Alice", "age": 30, "city": "New York"}
```

### Session Management
Always connect a session from Neo4j Connection or Neo4j Driver to other nodes. Sessions handle connection lifecycle automatically.

## Troubleshooting

### Connection Issues
- Verify Neo4j server is running
- Check URI format (bolt://, neo4j://, etc.)
- Confirm authentication credentials
- Test network connectivity

### Query Problems  
- Validate Cypher syntax
- Check parameter formatting (valid JSON)
- Verify node/relationship existence
- Review query execution summary

### Performance Tips
- Use appropriate fetch sizes for large result sets
- Set reasonable limits on Find Nodes operations
- Consider pagination for large datasets
- Use indexed properties for filtering

## Dependencies

- **Neo4j Python Driver**: >= 5.25.0
- **Griptape Nodes**: Latest from GitHub

## Support

For issues and questions:
- Check node `message` outputs for specific error guidance
- Review Neo4j server logs
- Verify JSON parameter formatting
- Test connections with simple queries first
