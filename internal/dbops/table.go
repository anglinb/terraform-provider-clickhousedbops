package dbops

import (
	"context"
	"strings"

	"github.com/pingcap/errors"

	"github.com/anglinb/terraform-provider-clickhousedbops/internal/clickhouseclient"
	"github.com/anglinb/terraform-provider-clickhousedbops/internal/querybuilder"
)

type Table struct {
	UUID         string                     `json:"uuid"`
	DatabaseName string                     `json:"database_name"`
	Name         string                     `json:"name"`
	Engine       string                     `json:"engine"`
	Columns      []querybuilder.TableColumn `json:"columns"`
	OrderBy      []string                   `json:"order_by"`
	PartitionBy  *string                    `json:"partition_by,omitempty"`
	PrimaryKey   []string                   `json:"primary_key,omitempty"`
	SampleBy     *string                    `json:"sample_by,omitempty"`
	TTL          *string                    `json:"ttl,omitempty"`
	Settings     map[string]string          `json:"settings,omitempty"`
	Comment      string                     `json:"comment"`
}

func (i *impl) CreateTable(ctx context.Context, table Table, clusterName *string) (*Table, error) {
	builder := querybuilder.NewCreateTable(table.DatabaseName, table.Name, table.Columns).
		WithCluster(clusterName).
		WithEngine(table.Engine).
		WithOrderBy(table.OrderBy).
		WithComment(table.Comment)

	if table.PartitionBy != nil {
		builder = builder.WithPartitionBy(*table.PartitionBy)
	}
	if len(table.PrimaryKey) > 0 {
		builder = builder.WithPrimaryKey(table.PrimaryKey)
	}
	if table.SampleBy != nil {
		builder = builder.WithSampleBy(*table.SampleBy)
	}
	if table.TTL != nil {
		builder = builder.WithTTL(*table.TTL)
	}
	if len(table.Settings) > 0 {
		builder = builder.WithSettings(table.Settings)
	}

	sql, err := builder.Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	return i.FindTableByName(ctx, table.DatabaseName, table.Name, clusterName)
}

func (i *impl) GetTable(ctx context.Context, uuid string, clusterName *string) (*Table, error) {
	// First get basic table info
	sql, err := querybuilder.NewSelect(
		[]querybuilder.Field{
			querybuilder.NewField("database"),
			querybuilder.NewField("name"),
			querybuilder.NewField("engine"),
			querybuilder.NewField("partition_key"),
			querybuilder.NewField("sorting_key"),
			querybuilder.NewField("primary_key"),
			querybuilder.NewField("sampling_key"),
			querybuilder.NewField("engine_full"),
			querybuilder.NewField("comment"),
		},
		"system.tables",
	).WithCluster(clusterName).Where(querybuilder.WhereEquals("uuid", uuid)).Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var table *Table

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		dbName, err := data.GetString("database")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'database' field")
		}
		name, err := data.GetString("name")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'name' field")
		}
		engine, err := data.GetString("engine")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'engine' field")
		}
		partitionKey, err := data.GetString("partition_key")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'partition_key' field")
		}
		sortingKey, err := data.GetString("sorting_key")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'sorting_key' field")
		}
		primaryKey, err := data.GetString("primary_key")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'primary_key' field")
		}
		samplingKey, err := data.GetString("sampling_key")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'sampling_key' field")
		}
		engineFull, err := data.GetString("engine_full")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'engine_full' field")
		}
		comment, err := data.GetString("comment")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'comment' field")
		}

		table = &Table{
			UUID:         uuid,
			DatabaseName: dbName,
			Name:         name,
			Engine:       engine,
			Comment:      comment,
		}

		// Parse order by from sorting_key
		if sortingKey != "" {
			table.OrderBy = parseKeyColumns(sortingKey)
		}

		// Parse partition by
		if partitionKey != "" {
			table.PartitionBy = &partitionKey
		}

		// Parse primary key
		if primaryKey != "" {
			table.PrimaryKey = parseKeyColumns(primaryKey)
		}

		// Parse sample by
		if samplingKey != "" {
			table.SampleBy = &samplingKey
		}

		// Parse TTL and settings from engine_full
		ttl, settings := parseEngineFullForTTLAndSettings(engineFull)
		if ttl != "" {
			table.TTL = &ttl
		}
		if len(settings) > 0 {
			table.Settings = settings
		}

		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if table == nil {
		// Table not found
		return nil, nil
	}

	// Get column information
	columnsSql, err := querybuilder.NewSelect(
		[]querybuilder.Field{
			querybuilder.NewField("name"),
			querybuilder.NewField("type"),
			querybuilder.NewField("default_expression"),
			querybuilder.NewField("comment"),
		},
		"system.columns",
	).WithCluster(clusterName).
		Where(
			querybuilder.WhereEquals("database", table.DatabaseName),
			querybuilder.WhereEquals("table", table.Name),
		).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building columns query")
	}

	var columns []querybuilder.TableColumn
	err = i.clickhouseClient.Select(ctx, columnsSql, func(data clickhouseclient.Row) error {
		name, err := data.GetString("name")
		if err != nil {
			return errors.WithMessage(err, "error scanning column result, missing 'name' field")
		}
		colType, err := data.GetString("type")
		if err != nil {
			return errors.WithMessage(err, "error scanning column result, missing 'type' field")
		}
		defaultExpr, err := data.GetString("default_expression")
		if err != nil {
			return errors.WithMessage(err, "error scanning column result, missing 'default_expression' field")
		}
		comment, err := data.GetString("comment")
		if err != nil {
			return errors.WithMessage(err, "error scanning column result, missing 'comment' field")
		}

		col := querybuilder.TableColumn{
			Name: name,
			Type: colType,
		}
		if defaultExpr != "" {
			col.Default = &defaultExpr
		}
		if comment != "" {
			col.Comment = &comment
		}
		columns = append(columns, col)
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error querying columns")
	}

	table.Columns = columns

	return table, nil
}

func (i *impl) DeleteTable(ctx context.Context, uuid string, clusterName *string) error {
	table, err := i.GetTable(ctx, uuid, clusterName)
	if err != nil {
		return errors.WithMessage(err, "error getting table")
	}

	if table == nil {
		// This is desired state.
		return nil
	}

	sql, err := querybuilder.NewDropTable(table.DatabaseName, table.Name).WithCluster(clusterName).Build()
	if err != nil {
		return errors.WithMessage(err, "error building query")
	}

	err = i.clickhouseClient.Exec(ctx, sql)
	if err != nil {
		return errors.WithMessage(err, "error running query")
	}

	return nil
}

func (i *impl) FindTableByName(ctx context.Context, databaseName, tableName string, clusterName *string) (*Table, error) {
	sql, err := querybuilder.NewSelect(
		[]querybuilder.Field{querybuilder.NewField("uuid")},
		"system.tables",
	).WithCluster(clusterName).
		Where(
			querybuilder.WhereEquals("database", databaseName),
			querybuilder.WhereEquals("name", tableName),
		).
		Build()
	if err != nil {
		return nil, errors.WithMessage(err, "error building query")
	}

	var uuid string

	err = i.clickhouseClient.Select(ctx, sql, func(data clickhouseclient.Row) error {
		uuid, err = data.GetString("uuid")
		if err != nil {
			return errors.WithMessage(err, "error scanning query result, missing 'uuid' field")
		}

		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "error running query")
	}

	if uuid == "" {
		return nil, errors.New("table with such name not found")
	}

	return i.GetTable(ctx, uuid, clusterName)
}

// parseKeyColumns parses a comma-separated list of columns (possibly with spaces)
func parseKeyColumns(key string) []string {
	if key == "" {
		return nil
	}
	parts := strings.Split(key, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// parseEngineFullForTTLAndSettings attempts to extract TTL and SETTINGS from engine_full string
// This is a simplified parser and may need to be enhanced for complex cases
func parseEngineFullForTTLAndSettings(engineFull string) (string, map[string]string) {
	ttl := ""
	settings := make(map[string]string)

	// Look for TTL
	if idx := strings.Index(engineFull, "TTL "); idx != -1 {
		ttlStart := idx + 4
		// Find the end of TTL expression (before SETTINGS or end of string)
		ttlEnd := strings.Index(engineFull[ttlStart:], " SETTINGS")
		if ttlEnd == -1 {
			ttl = strings.TrimSpace(engineFull[ttlStart:])
		} else {
			ttl = strings.TrimSpace(engineFull[ttlStart : ttlStart+ttlEnd])
		}
	}

	// Look for SETTINGS
	if idx := strings.Index(engineFull, "SETTINGS "); idx != -1 {
		settingsStr := engineFull[idx+9:]
		// Parse settings (simplified - assumes key = value format)
		pairs := strings.Split(settingsStr, ",")
		for _, pair := range pairs {
			parts := strings.Split(strings.TrimSpace(pair), "=")
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				settings[key] = value
			}
		}
	}

	return ttl, settings
}

func (i *impl) AddTableColumns(ctx context.Context, databaseName, tableName string, columns []querybuilder.TableColumn, clusterName *string) error {
	query, err := querybuilder.NewAlterTableAddColumn(databaseName, tableName, columns).
		WithCluster(clusterName).
		Build()
	if err != nil {
		return errors.WithMessage(err, "error building ALTER TABLE ADD COLUMN query")
	}

	err = i.clickhouseClient.Exec(ctx, query)
	if err != nil {
		return errors.WithMessage(err, "error adding columns to table")
	}

	return nil
}

func (i *impl) DropTableColumns(ctx context.Context, databaseName, tableName string, columnNames []string, clusterName *string) error {
	query, err := querybuilder.NewAlterTableDropColumn(databaseName, tableName, columnNames).
		WithCluster(clusterName).
		Build()
	if err != nil {
		return errors.WithMessage(err, "error building ALTER TABLE DROP COLUMN query")
	}

	err = i.clickhouseClient.Exec(ctx, query)
	if err != nil {
		return errors.WithMessage(err, "error dropping columns from table")
	}

	return nil
}
