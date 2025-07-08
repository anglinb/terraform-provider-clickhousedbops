# Create a database for our tables
resource "clickhousedbops_database" "test_db" {
  name    = "terraform_test_tables"
  comment = "Database for table resource testing"
}

# Example 1: Basic MergeTree table
resource "clickhousedbops_table" "basic_table" {
  database_name = clickhousedbops_database.test_db.name
  name          = "basic_events"
  
  columns = [
    {
      name = "id"
      type = "UInt64"
    },
    {
      name = "timestamp"
      type = "DateTime"
    },
    {
      name = "message"
      type = "String"
    }
  ]
  
  engine   = "MergeTree()"
  order_by = ["timestamp", "id"]
  
  comment = "Basic events table for testing"
}

# Example 2: Table with defaults and comments
resource "clickhousedbops_table" "users_table" {
  database_name = clickhousedbops_database.test_db.name
  name          = "users"
  
  columns = [
    {
      name = "user_id"
      type = "UInt64"
      comment = "Unique user identifier"
    },
    {
      name = "username"
      type = "String"
      comment = "User login name"
    },
    {
      name = "email"
      type = "String"
      comment = "User email address"
    },
    {
      name = "created_at"
      type = "DateTime"
      default = "now()"
      comment = "Account creation timestamp"
    },
    {
      name = "is_active"
      type = "UInt8"
      default = "1"
      comment = "1 if user is active, 0 otherwise"
    }
  ]
  
  engine      = "MergeTree()"
  order_by    = ["user_id"]
  primary_key = ["user_id"]
}

# Example 3: Table with partitioning and TTL
resource "clickhousedbops_table" "logs_table" {
  database_name = clickhousedbops_database.test_db.name
  name          = "application_logs"
  
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
      name = "logger"
      type = "LowCardinality(String)"
    },
    {
      name = "message"
      type = "String"
    },
    {
      name = "context"
      type = "Map(String, String)"
      default = "map()"
    }
  ]
  
  engine       = "MergeTree()"
  order_by     = ["timestamp", "level"]
  partition_by = "toYYYYMM(timestamp)"
  ttl          = "timestamp + INTERVAL 90 DAY"
  
  settings = {
    index_granularity = "8192"
    ttl_only_drop_parts = "1"
  }
  
  comment = "Application logs with 90-day retention"
}

# Example 4: ReplacingMergeTree table
resource "clickhousedbops_table" "inventory_table" {
  database_name = clickhousedbops_database.test_db.name
  name          = "inventory"
  
  columns = [
    {
      name = "product_id"
      type = "UInt64"
    },
    {
      name = "warehouse_id"
      type = "UInt32"
    },
    {
      name = "quantity"
      type = "Int64"
    },
    {
      name = "last_updated"
      type = "DateTime"
    },
    {
      name = "version"
      type = "UInt64"
    }
  ]
  
  engine   = "ReplacingMergeTree(version)"
  order_by = ["product_id", "warehouse_id"]
  
  comment = "Product inventory with version-based deduplication"
}

# Example 5: Table with sampling
resource "clickhousedbops_table" "metrics_table" {
  database_name = clickhousedbops_database.test_db.name
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
    },
    {
      name = "tags"
      type = "Array(String)"
      default = "[]"
    }
  ]
  
  engine       = "MergeTree()"
  order_by     = ["server_id", "timestamp", "metric_name"]
  primary_key  = ["server_id", "timestamp"]
  sample_by    = "intHash32(server_id)"
  partition_by = "toStartOfDay(timestamp)"
}

# Output the created tables
output "tables" {
  value = {
    basic_table     = clickhousedbops_table.basic_table.name
    users_table     = clickhousedbops_table.users_table.name
    logs_table      = clickhousedbops_table.logs_table.name
    inventory_table = clickhousedbops_table.inventory_table.name
    metrics_table   = clickhousedbops_table.metrics_table.name
  }
}