package dbops

import (
	"context"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/querybuilder"
)

type Client interface {
	CreateDatabase(ctx context.Context, database Database, clusterName *string) (*Database, error)
	GetDatabase(ctx context.Context, uuid string, clusterName *string) (*Database, error)
	DeleteDatabase(ctx context.Context, uuid string, clusterName *string) error
	FindDatabaseByName(ctx context.Context, name string, clusterName *string) (*Database, error)

	CreateRole(ctx context.Context, role Role, clusterName *string) (*Role, error)
	GetRole(ctx context.Context, id string, clusterName *string) (*Role, error)
	DeleteRole(ctx context.Context, id string, clusterName *string) error
	FindRoleByName(ctx context.Context, name string, clusterName *string) (*Role, error)

	CreateUser(ctx context.Context, user User, clusterName *string) (*User, error)
	GetUser(ctx context.Context, id string, clusterName *string) (*User, error)
	DeleteUser(ctx context.Context, id string, clusterName *string) error
	FindUserByName(ctx context.Context, name string, clusterName *string) (*User, error)

	GrantRole(ctx context.Context, grantRole GrantRole, clusterName *string) (*GrantRole, error)
	GetGrantRole(ctx context.Context, grantedRoleName string, granteeUserName *string, granteeRoleName *string, clusterName *string) (*GrantRole, error)
	RevokeGrantRole(ctx context.Context, grantedRoleName string, granteeUserName *string, granteeRoleName *string, clusterName *string) error

	GrantPrivilege(ctx context.Context, grantPrivilege GrantPrivilege, clusterName *string) (*GrantPrivilege, error)
	GetGrantPrivilege(ctx context.Context, accessType string, database *string, table *string, column *string, granteeUserName *string, granteeRoleName *string, clusterName *string) (*GrantPrivilege, error)
	RevokeGrantPrivilege(ctx context.Context, accessType string, database *string, table *string, column *string, granteeUserName *string, granteeRoleName *string, clusterName *string) error
	GetAllGrantsForGrantee(ctx context.Context, granteeUsername *string, granteeRoleName *string, clusterName *string) ([]GrantPrivilege, error)

	IsReplicatedStorage(ctx context.Context) (bool, error)

	CreateTable(ctx context.Context, table Table, clusterName *string) (*Table, error)
	GetTable(ctx context.Context, uuid string, clusterName *string) (*Table, error)
	DeleteTable(ctx context.Context, uuid string, clusterName *string) error
	FindTableByName(ctx context.Context, databaseName, tableName string, clusterName *string) (*Table, error)
	AddTableColumns(ctx context.Context, databaseName, tableName string, columns []querybuilder.TableColumn, clusterName *string) error
	DropTableColumns(ctx context.Context, databaseName, tableName string, columnNames []string, clusterName *string) error
}
