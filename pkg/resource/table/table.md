# clickhousedbops_table

The `clickhousedbops_table` resource allows you to manage tables in a ClickHouse database.

## Example Usage

```hcl
# Create a simple MergeTree table
resource "clickhousedbops_table" "events" {
  database_name = clickhousedbops_database.my_db.name
  name          = "events"
  
  columns = [
    {
      name = "timestamp"
      type = "DateTime"
    },
    {
      name = "user_id"
      type = "UInt64"
    },
    {
      name = "event_type"
      type = "String"
    },
    {
      name = "data"
      type = "String"
      default = "''"
    }
  ]
  
  engine = "MergeTree()"
  order_by = ["timestamp", "user_id"]
  partition_by = "toYYYYMM(timestamp)"
}

# Create a replicated table with TTL
resource "clickhousedbops_table" "logs" {
  cluster_name  = "my_cluster"
  database_name = "logs_db"
  name          = "access_logs"
  
  columns = [
    {
      name = "timestamp"
      type = "DateTime"
    },
    {
      name = "level"
      type = "Enum8('DEBUG' = 1, 'INFO' = 2, 'WARN' = 3, 'ERROR' = 4)"
    },
    {
      name = "message"
      type = "String"
    },
    {
      name = "attributes"
      type = "Map(String, String)"
      default = "map()"
    }
  ]
  
  engine = "ReplicatedMergeTree('/clickhouse/tables/{shard}/logs_db/access_logs', '{replica}')"
  order_by = ["timestamp"]
  partition_by = "toStartOfHour(timestamp)"
  ttl = "timestamp + INTERVAL 30 DAY"
  
  settings = {
    index_granularity = "8192"
    merge_with_ttl_timeout = "86400"
  }
  
  comment = "Application access logs with 30-day retention"
}

# Create a table with primary key and sampling
resource "clickhousedbops_table" "metrics" {
  database_name = "metrics_db"
  name          = "server_metrics"
  
  columns = [
    {
      name = "timestamp"
      type = "DateTime"
    },
    {
      name = "server_id"
      type = "UInt32"
    },
    {
      name = "metric_name"
      type = "LowCardinality(String)"
    },
    {
      name = "value"
      type = "Float64"
    }
  ]
  
  engine = "MergeTree()"
  order_by = ["server_id", "metric_name", "timestamp"]
  primary_key = ["server_id", "metric_name"]
  sample_by = "server_id"
  partition_by = "toDate(timestamp)"
}
```

## Import

Tables can be imported using one of these formats:

```bash
# Import by database and table name
terraform import clickhousedbops_table.my_table "database_name:table_name"

# Import by database name and table UUID
terraform import clickhousedbops_table.my_table "database_name:00000000-0000-0000-0000-000000000000"

# Import with cluster name
terraform import clickhousedbops_table.my_table "cluster_name:database_name:table_name"
```