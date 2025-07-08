package querybuilder

import (
	"testing"
)

func TestCreateTableQueryBuilder_Build(t *testing.T) {
	tests := []struct {
		name    string
		builder CreateTableQueryBuilder
		want    string
		wantErr bool
	}{
		{
			name: "simple MergeTree table",
			builder: NewCreateTable("mydb", "mytable", []TableColumn{
				{Name: "id", Type: "UInt64"},
				{Name: "name", Type: "String"},
			}).WithEngine("MergeTree()").WithOrderBy([]string{"id"}),
			want:    "CREATE TABLE `mydb`.`mytable` (`id` UInt64, `name` String) ENGINE = MergeTree() ORDER BY (`id`);",
			wantErr: false,
		},
		{
			name: "table with column defaults and comments",
			builder: NewCreateTable("mydb", "users", []TableColumn{
				{Name: "id", Type: "UInt64"},
				{Name: "created_at", Type: "DateTime", Default: stringPtr("now()"), Comment: stringPtr("Creation timestamp")},
				{Name: "is_active", Type: "UInt8", Default: stringPtr("1")},
			}).WithEngine("MergeTree()").WithOrderBy([]string{"id"}),
			want:    "CREATE TABLE `mydb`.`users` (`id` UInt64, `created_at` DateTime DEFAULT now() COMMENT 'Creation timestamp', `is_active` UInt8 DEFAULT 1) ENGINE = MergeTree() ORDER BY (`id`);",
			wantErr: false,
		},
		{
			name: "table with cluster",
			builder: NewCreateTable("mydb", "distributed_table", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}).WithEngine("MergeTree()").WithOrderBy([]string{"id"}).WithCluster(stringPtr("my_cluster")),
			want:    "CREATE TABLE `mydb`.`distributed_table` ON CLUSTER 'my_cluster' (`id` UInt64) ENGINE = MergeTree() ORDER BY (`id`);",
			wantErr: false,
		},
		{
			name: "table with partitioning and TTL",
			builder: NewCreateTable("mydb", "logs", []TableColumn{
				{Name: "timestamp", Type: "DateTime"},
				{Name: "message", Type: "String"},
			}).WithEngine("MergeTree()").
				WithOrderBy([]string{"timestamp"}).
				WithPartitionBy("toYYYYMM(timestamp)").
				WithTTL("timestamp + INTERVAL 30 DAY"),
			want:    "CREATE TABLE `mydb`.`logs` (`timestamp` DateTime, `message` String) ENGINE = MergeTree() ORDER BY (`timestamp`) PARTITION BY toYYYYMM(timestamp) TTL timestamp + INTERVAL 30 DAY;",
			wantErr: false,
		},
		{
			name: "table with primary key and sample by",
			builder: NewCreateTable("mydb", "metrics", []TableColumn{
				{Name: "server_id", Type: "UInt32"},
				{Name: "timestamp", Type: "DateTime"},
				{Name: "value", Type: "Float64"},
			}).WithEngine("MergeTree()").
				WithOrderBy([]string{"server_id", "timestamp"}).
				WithPrimaryKey([]string{"server_id"}).
				WithSampleBy("intHash32(server_id)"),
			want:    "CREATE TABLE `mydb`.`metrics` (`server_id` UInt32, `timestamp` DateTime, `value` Float64) ENGINE = MergeTree() ORDER BY (`server_id`, `timestamp`) PRIMARY KEY (`server_id`) SAMPLE BY intHash32(server_id);",
			wantErr: false,
		},
		{
			name: "table with settings",
			builder: NewCreateTable("mydb", "optimized", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}).WithEngine("MergeTree()").
				WithOrderBy([]string{"id"}).
				WithSettings(map[string]string{
					"index_granularity":      "16384",
					"merge_with_ttl_timeout": "86400",
				}),
			want:    "CREATE TABLE `mydb`.`optimized` (`id` UInt64) ENGINE = MergeTree() ORDER BY (`id`) SETTINGS index_granularity = 16384, merge_with_ttl_timeout = 86400;",
			wantErr: false,
		},
		{
			name: "table with comment",
			builder: NewCreateTable("mydb", "documented", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}).WithEngine("MergeTree()").
				WithOrderBy([]string{"id"}).
				WithComment("This is a well-documented table"),
			want:    "CREATE TABLE `mydb`.`documented` (`id` UInt64) ENGINE = MergeTree() ORDER BY (`id`) COMMENT 'This is a well-documented table';",
			wantErr: false,
		},
		{
			name: "ReplacingMergeTree with version column",
			builder: NewCreateTable("mydb", "versioned", []TableColumn{
				{Name: "id", Type: "UInt64"},
				{Name: "data", Type: "String"},
				{Name: "version", Type: "UInt64"},
			}).WithEngine("ReplacingMergeTree(version)").
				WithOrderBy([]string{"id"}),
			want:    "CREATE TABLE `mydb`.`versioned` (`id` UInt64, `data` String, `version` UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY (`id`);",
			wantErr: false,
		},
		{
			name: "error: empty database name",
			builder: NewCreateTable("", "mytable", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}).WithEngine("MergeTree()"),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: empty table name",
			builder: NewCreateTable("mydb", "", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}).WithEngine("MergeTree()"),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: no columns",
			builder: NewCreateTable("mydb", "mytable", []TableColumn{}).
				WithEngine("MergeTree()"),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: no engine",
			builder: NewCreateTable("mydb", "mytable", []TableColumn{
				{Name: "id", Type: "UInt64"},
			}),
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.builder.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTableQueryBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateTableQueryBuilder.Build() = %v, want %v", got, tt.want)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
