# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the official Terraform provider for ClickHouse database operations. It manages databases, users, roles, and permissions in ClickHouse clusters using the Terraform Plugin Framework.

## Essential Development Commands

### Build and Development
- `make build` - Build the provider binary to `tmp/terraform-provider-clickhousedbops`
- `make install` - Build and install to local Terraform plugins directory
- `air` - Run with hot reload for development (requires `go install github.com/air-verse/air@latest`)

### Testing and Quality
- `make test` - Run unit tests with 30s timeout and parallel execution
- `make fmt` - Format code using go fmt and golangci-lint
- `make docs` - Generate documentation using tfplugindocs
- `make enable_git_hooks` - Enable pre-commit hooks (formatting, docs, build)

### Release (ClickHouse employees only)
- `make release` - Build binaries for multiple platforms

## Architecture Overview

### Provider Structure
The provider follows a clean layered architecture:

1. **Entry Point**: `main.go` → `pkg/provider/provider.go`
2. **Resources**: Each resource in `pkg/resource/{resource_name}/` implements CRUD operations
3. **Internal Packages**:
   - `internal/clickhouseclient/`: Protocol implementations (native/HTTP)
   - `internal/dbops/`: High-level database operations
   - `internal/querybuilder/`: Type-safe SQL query construction

### Key Architectural Patterns

1. **No Update Operations**: All resources use `RequiresReplace` - changes require recreation
2. **UUID Tracking**: Resources use ClickHouse system-assigned UUIDs for state management
3. **Cluster Support**: All operations support optional cluster specification via `cluster_name`
4. **Protocol Flexibility**: Supports native (port 9000) and HTTP/HTTPS (port 8123) protocols
5. **Authentication**: Password auth for native, basic auth for HTTP

### Resource Interaction Flow
```
Terraform Resource → DBOps Client → ClickHouse Client → ClickHouse Server
                                    (Native or HTTP)
```

### System Tables Used
- `system.databases`, `system.users`, `system.roles`
- `system.grants`, `system.role_grants`
- `system.clusters`, `system.user_directories`

## Development Setup

1. Create `~/.terraformrc` with dev_overrides:
```hcl
provider_installation {
  dev_overrides {
    "ClickHouse/clickhousedbops" = "/path/to/terraform-provider-clickhousedbops/tmp"
  }
  direct {}
}
```

2. Skip `terraform init` when using dev_overrides

## Important Implementation Notes

1. **Error Handling**: Uses `github.com/pingcap/errors` for error wrapping. Convert to Terraform diagnostics with `resp.Diagnostics.AddError()`

2. **Query Building**: Always use `querybuilder` package for SQL construction to ensure proper escaping:
   - Identifiers: Backticks (`)
   - String values: Single quotes (')

3. **Replicated Storage**: The provider detects and warns about replicated user directories which are incompatible with certain operations

4. **Import Patterns**: Resources support flexible import:
   - By name: `terraform import resource.name "resource_name"`
   - By UUID: `terraform import resource.name "uuid"`
   - With cluster: `terraform import resource.name "cluster:resource_name"`

5. **Testing**: Use Docker Compose setup in `tests/` for integration testing with real ClickHouse instances

## Resource Implementation Checklist

When adding new resources:
1. Create directory in `pkg/resource/{resource_name}/`
2. Implement resource with Create, Read, Delete, ImportState methods
3. Add embedded documentation (`.md` file)
4. Add corresponding operations in `internal/dbops/`
5. Add query builders in `internal/querybuilder/`
6. Add examples in `examples/tests/{resource_name}/`
7. Run `make docs` to generate documentation

## Common Patterns

### Reading System Tables
```go
rows, err := dbops.SelectQuery(ctx, client, query)
if err != nil {
    return false, errors.Wrap(err, "failed to query")
}
if rows.Next() {
    // Process row
}
```

### Cluster-Aware Operations
```go
if clusterName != "" {
    query = query.OnCluster(clusterName)
}
```

### Resource State Management
- Always use UUID field for state tracking
- Name fields are for user convenience but UUID is authoritative
- Handle missing resources by checking if Read returns nil