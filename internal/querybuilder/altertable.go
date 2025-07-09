package querybuilder

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

// AlterTableAddColumnQueryBuilder builds ALTER TABLE ADD COLUMN queries
type AlterTableAddColumnQueryBuilder struct {
	databaseName string
	tableName    string
	columns      []TableColumn
	clusterName  *string
}

// NewAlterTableAddColumn creates a new ALTER TABLE ADD COLUMN query builder
func NewAlterTableAddColumn(databaseName, tableName string, columns []TableColumn) *AlterTableAddColumnQueryBuilder {
	return &AlterTableAddColumnQueryBuilder{
		databaseName: databaseName,
		tableName:    tableName,
		columns:      columns,
	}
}

// WithCluster adds ON CLUSTER clause
func (b *AlterTableAddColumnQueryBuilder) WithCluster(clusterName *string) *AlterTableAddColumnQueryBuilder {
	b.clusterName = clusterName
	return b
}

// Build generates the ALTER TABLE ADD COLUMN SQL query
func (b *AlterTableAddColumnQueryBuilder) Build() (string, error) {
	if b.databaseName == "" {
		return "", errors.New("database name is required")
	}
	if b.tableName == "" {
		return "", errors.New("table name is required")
	}
	if len(b.columns) == 0 {
		return "", errors.New("at least one column is required")
	}

	var sb strings.Builder
	
	// ALTER TABLE database.table
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(fmt.Sprintf("`%s`.`%s`", b.databaseName, b.tableName))
	
	// ON CLUSTER 'cluster'
	if b.clusterName != nil && *b.clusterName != "" {
		sb.WriteString(fmt.Sprintf(" ON CLUSTER %s", quote(*b.clusterName)))
	}
	
	// ADD COLUMN for each column
	for i, col := range b.columns {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(" ADD COLUMN ")
		
		// Column name and type
		sb.WriteString(fmt.Sprintf("`%s` %s", col.Name, col.Type))
		
		// DEFAULT expression
		if col.Default != nil && *col.Default != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.Default))
		}
		
		// COMMENT
		if col.Comment != nil && *col.Comment != "" {
			sb.WriteString(fmt.Sprintf(" COMMENT %s", quote(*col.Comment)))
		}
	}
	
	return sb.String(), nil
}

// AlterTableDropColumnQueryBuilder builds ALTER TABLE DROP COLUMN queries
type AlterTableDropColumnQueryBuilder struct {
	databaseName string
	tableName    string
	columnNames  []string
	clusterName  *string
}

// NewAlterTableDropColumn creates a new ALTER TABLE DROP COLUMN query builder
func NewAlterTableDropColumn(databaseName, tableName string, columnNames []string) *AlterTableDropColumnQueryBuilder {
	return &AlterTableDropColumnQueryBuilder{
		databaseName: databaseName,
		tableName:    tableName,
		columnNames:  columnNames,
	}
}

// WithCluster adds ON CLUSTER clause
func (b *AlterTableDropColumnQueryBuilder) WithCluster(clusterName *string) *AlterTableDropColumnQueryBuilder {
	b.clusterName = clusterName
	return b
}

// Build generates the ALTER TABLE DROP COLUMN SQL query
func (b *AlterTableDropColumnQueryBuilder) Build() (string, error) {
	if b.databaseName == "" {
		return "", errors.New("database name is required")
	}
	if b.tableName == "" {
		return "", errors.New("table name is required")
	}
	if len(b.columnNames) == 0 {
		return "", errors.New("at least one column name is required")
	}

	var sb strings.Builder
	
	// ALTER TABLE database.table
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(fmt.Sprintf("`%s`.`%s`", b.databaseName, b.tableName))
	
	// ON CLUSTER 'cluster'
	if b.clusterName != nil && *b.clusterName != "" {
		sb.WriteString(fmt.Sprintf(" ON CLUSTER %s", quote(*b.clusterName)))
	}
	
	// DROP COLUMN for each column
	for i, colName := range b.columnNames {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(" DROP COLUMN ")
		sb.WriteString(fmt.Sprintf("`%s`", colName))
	}
	
	return sb.String(), nil
}