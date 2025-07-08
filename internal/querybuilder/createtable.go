package querybuilder

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

// CreateTableQueryBuilder is an interface to build CREATE TABLE SQL queries (already interpolated).
type CreateTableQueryBuilder interface {
	QueryBuilder
	WithCluster(clusterName *string) CreateTableQueryBuilder
	WithEngine(engine string) CreateTableQueryBuilder
	WithOrderBy(orderBy []string) CreateTableQueryBuilder
	WithPartitionBy(partitionBy string) CreateTableQueryBuilder
	WithPrimaryKey(primaryKey []string) CreateTableQueryBuilder
	WithSampleBy(sampleBy string) CreateTableQueryBuilder
	WithTTL(ttl string) CreateTableQueryBuilder
	WithSettings(settings map[string]string) CreateTableQueryBuilder
	WithComment(comment string) CreateTableQueryBuilder
}

type createTableQueryBuilder struct {
	databaseName string
	tableName    string
	columns      []TableColumn
	clusterName  *string
	engine       string
	orderBy      []string
	partitionBy  *string
	primaryKey   []string
	sampleBy     *string
	ttl          *string
	settings     map[string]string
	comment      *string
}

type TableColumn struct {
	Name    string
	Type    string
	Default *string
	Comment *string
}

func NewCreateTable(databaseName, tableName string, columns []TableColumn) CreateTableQueryBuilder {
	return &createTableQueryBuilder{
		databaseName: databaseName,
		tableName:    tableName,
		columns:      columns,
		settings:     make(map[string]string),
	}
}

func (q *createTableQueryBuilder) WithCluster(clusterName *string) CreateTableQueryBuilder {
	q.clusterName = clusterName
	return q
}

func (q *createTableQueryBuilder) WithEngine(engine string) CreateTableQueryBuilder {
	q.engine = engine
	return q
}

func (q *createTableQueryBuilder) WithOrderBy(orderBy []string) CreateTableQueryBuilder {
	q.orderBy = orderBy
	return q
}

func (q *createTableQueryBuilder) WithPartitionBy(partitionBy string) CreateTableQueryBuilder {
	q.partitionBy = &partitionBy
	return q
}

func (q *createTableQueryBuilder) WithPrimaryKey(primaryKey []string) CreateTableQueryBuilder {
	q.primaryKey = primaryKey
	return q
}

func (q *createTableQueryBuilder) WithSampleBy(sampleBy string) CreateTableQueryBuilder {
	q.sampleBy = &sampleBy
	return q
}

func (q *createTableQueryBuilder) WithTTL(ttl string) CreateTableQueryBuilder {
	q.ttl = &ttl
	return q
}

func (q *createTableQueryBuilder) WithSettings(settings map[string]string) CreateTableQueryBuilder {
	q.settings = settings
	return q
}

func (q *createTableQueryBuilder) WithComment(comment string) CreateTableQueryBuilder {
	q.comment = &comment
	return q
}

func (q *createTableQueryBuilder) Build() (string, error) {
	if q.databaseName == "" {
		return "", errors.New("databaseName cannot be empty for CREATE TABLE queries")
	}
	if q.tableName == "" {
		return "", errors.New("tableName cannot be empty for CREATE TABLE queries")
	}
	if len(q.columns) == 0 {
		return "", errors.New("columns cannot be empty for CREATE TABLE queries")
	}
	if q.engine == "" {
		return "", errors.New("engine cannot be empty for CREATE TABLE queries")
	}

	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(backtick(q.databaseName))
	sb.WriteString(".")
	sb.WriteString(backtick(q.tableName))

	if q.clusterName != nil {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(quote(*q.clusterName))
	}

	// Build column definitions
	sb.WriteString(" (")
	for i, col := range q.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(backtick(col.Name))
		sb.WriteString(" ")
		sb.WriteString(col.Type)
		if col.Default != nil {
			sb.WriteString(" DEFAULT ")
			sb.WriteString(*col.Default)
		}
		if col.Comment != nil {
			sb.WriteString(" COMMENT ")
			sb.WriteString(quote(*col.Comment))
		}
	}
	sb.WriteString(")")

	// Engine
	sb.WriteString(" ENGINE = ")
	sb.WriteString(q.engine)

	// ORDER BY
	if len(q.orderBy) > 0 {
		sb.WriteString(" ORDER BY (")
		for i, orderCol := range q.orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(backtick(orderCol))
		}
		sb.WriteString(")")
	}

	// PARTITION BY
	if q.partitionBy != nil {
		sb.WriteString(" PARTITION BY ")
		sb.WriteString(*q.partitionBy)
	}

	// PRIMARY KEY
	if len(q.primaryKey) > 0 {
		sb.WriteString(" PRIMARY KEY (")
		for i, pkCol := range q.primaryKey {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(backtick(pkCol))
		}
		sb.WriteString(")")
	}

	// SAMPLE BY
	if q.sampleBy != nil {
		sb.WriteString(" SAMPLE BY ")
		sb.WriteString(*q.sampleBy)
	}

	// TTL
	if q.ttl != nil {
		sb.WriteString(" TTL ")
		sb.WriteString(*q.ttl)
	}

	// SETTINGS
	if len(q.settings) > 0 {
		sb.WriteString(" SETTINGS ")
		i := 0
		for key, value := range q.settings {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s = %s", key, value))
			i++
		}
	}

	// COMMENT
	if q.comment != nil {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quote(*q.comment))
	}

	sb.WriteString(";")

	return sb.String(), nil
}
