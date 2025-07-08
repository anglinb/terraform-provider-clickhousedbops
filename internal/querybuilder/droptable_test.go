package querybuilder

import (
	"testing"
)

func TestDropTableQueryBuilder_Build(t *testing.T) {
	tests := []struct {
		name    string
		builder DropTableQueryBuilder
		want    string
		wantErr bool
	}{
		{
			name:    "simple drop table",
			builder: NewDropTable("mydb", "mytable"),
			want:    "DROP TABLE `mydb`.`mytable`;",
			wantErr: false,
		},
		{
			name:    "drop table with cluster",
			builder: NewDropTable("mydb", "distributed_table").WithCluster(stringPtr("my_cluster")),
			want:    "DROP TABLE `mydb`.`distributed_table` ON CLUSTER 'my_cluster';",
			wantErr: false,
		},
		{
			name:    "drop table with special characters in names",
			builder: NewDropTable("my-db", "my.table"),
			want:    "DROP TABLE `my-db`.`my.table`;",
			wantErr: false,
		},
		{
			name:    "error: empty database name",
			builder: NewDropTable("", "mytable"),
			want:    "",
			wantErr: true,
		},
		{
			name:    "error: empty table name",
			builder: NewDropTable("mydb", ""),
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.builder.Build()
			if (err != nil) != tt.wantErr {
				t.Errorf("DropTableQueryBuilder.Build() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DropTableQueryBuilder.Build() = %v, want %v", got, tt.want)
			}
		})
	}
}
