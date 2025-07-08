package querybuilder

import (
	"strings"

	"github.com/pingcap/errors"
)

// DropTableQueryBuilder is an interface to build DROP TABLE SQL queries (already interpolated).
type DropTableQueryBuilder interface {
	QueryBuilder
	WithCluster(clusterName *string) DropTableQueryBuilder
}

type dropTableQueryBuilder struct {
	databaseName string
	tableName    string
	clusterName  *string
}

func NewDropTable(databaseName, tableName string) DropTableQueryBuilder {
	return &dropTableQueryBuilder{
		databaseName: databaseName,
		tableName:    tableName,
	}
}

func (q *dropTableQueryBuilder) WithCluster(clusterName *string) DropTableQueryBuilder {
	q.clusterName = clusterName
	return q
}

func (q *dropTableQueryBuilder) Build() (string, error) {
	if q.databaseName == "" {
		return "", errors.New("databaseName cannot be empty for DROP TABLE queries")
	}
	if q.tableName == "" {
		return "", errors.New("tableName cannot be empty for DROP TABLE queries")
	}

	tokens := []string{
		"DROP",
		"TABLE",
		backtick(q.databaseName) + "." + backtick(q.tableName),
	}

	if q.clusterName != nil {
		tokens = append(tokens, "ON", "CLUSTER", quote(*q.clusterName))
	}

	return strings.Join(tokens, " ") + ";", nil
}
