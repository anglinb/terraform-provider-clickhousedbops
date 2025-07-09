package querybuilder

import (
	"testing"
)

func TestAlterTableAddColumnQueryBuilder_Build(t *testing.T) {
	tests := []struct {
		name    string
		builder *AlterTableAddColumnQueryBuilder
		want    string
		wantErr bool
	}{
		{
			name: "single column without extras",
			builder: NewAlterTableAddColumn("mydb", "mytable", []TableColumn{
				{Name: "new_col", Type: "String"},
			}),
			want:    "ALTER TABLE `mydb`.`mytable` ADD COLUMN `new_col` String",
			wantErr: false,
		},
		{
			name: "single column with default and comment",
			builder: NewAlterTableAddColumn("mydb", "mytable", []TableColumn{
				{Name: "created_at", Type: "DateTime", Default: stringPtr("now()"), Comment: stringPtr("Creation time")},
			}),
			want:    "ALTER TABLE `mydb`.`mytable` ADD COLUMN `created_at` DateTime DEFAULT now() COMMENT 'Creation time'",
			wantErr: false,
		},
		{
			name: "multiple columns",
			builder: NewAlterTableAddColumn("mydb", "mytable", []TableColumn{
				{Name: "col1", Type: "UInt64"},
				{Name: "col2", Type: "String", Default: stringPtr("''")},
				{Name: "col3", Type: "Float64", Comment: stringPtr("Score value")},
			}),
			want:    "ALTER TABLE `mydb`.`mytable` ADD COLUMN `col1` UInt64, ADD COLUMN `col2` String DEFAULT '', ADD COLUMN `col3` Float64 COMMENT 'Score value'",
			wantErr: false,
		},
		{
			name: "with cluster",
			builder: NewAlterTableAddColumn("mydb", "mytable", []TableColumn{
				{Name: "new_col", Type: "String"},
			}).WithCluster(stringPtr("my_cluster")),
			want:    "ALTER TABLE `mydb`.`mytable` ON CLUSTER 'my_cluster' ADD COLUMN `new_col` String",
			wantErr: false,
		},
		{
			name: "error: empty database name",
			builder: NewAlterTableAddColumn("", "mytable", []TableColumn{
				{Name: "col", Type: "String"},
			}),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: empty table name",
			builder: NewAlterTableAddColumn("mydb", "", []TableColumn{
				{Name: "col", Type: "String"},
			}),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: no columns",
			builder: NewAlterTableAddColumn("mydb", "mytable", []TableColumn{}),
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.builder.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("AlterTableAddColumnQueryBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AlterTableAddColumnQueryBuilder.Build() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAlterTableDropColumnQueryBuilder_Build(t *testing.T) {
	tests := []struct {
		name    string
		builder *AlterTableDropColumnQueryBuilder
		want    string
		wantErr bool
	}{
		{
			name: "single column",
			builder: NewAlterTableDropColumn("mydb", "mytable", []string{"old_col"}),
			want:    "ALTER TABLE `mydb`.`mytable` DROP COLUMN `old_col`",
			wantErr: false,
		},
		{
			name: "multiple columns",
			builder: NewAlterTableDropColumn("mydb", "mytable", []string{"col1", "col2", "col3"}),
			want:    "ALTER TABLE `mydb`.`mytable` DROP COLUMN `col1`, DROP COLUMN `col2`, DROP COLUMN `col3`",
			wantErr: false,
		},
		{
			name: "with cluster",
			builder: NewAlterTableDropColumn("mydb", "mytable", []string{"old_col"}).WithCluster(stringPtr("my_cluster")),
			want:    "ALTER TABLE `mydb`.`mytable` ON CLUSTER 'my_cluster' DROP COLUMN `old_col`",
			wantErr: false,
		},
		{
			name: "error: empty database name",
			builder: NewAlterTableDropColumn("", "mytable", []string{"col"}),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: empty table name",
			builder: NewAlterTableDropColumn("mydb", "", []string{"col"}),
			want:    "",
			wantErr: true,
		},
		{
			name: "error: no columns",
			builder: NewAlterTableDropColumn("mydb", "mytable", []string{}),
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.builder.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("AlterTableDropColumnQueryBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AlterTableDropColumnQueryBuilder.Build() = %v, want %v", got, tt.want)
			}
		})
	}
}