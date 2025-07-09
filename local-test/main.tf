terraform {
  required_version = ">= 1.0"

  required_providers {
    clickhousedbops = {
      source  = "ClickHouse/clickhousedbops"
      version = "~> 1.1.0"
    }
  }
}

provider "clickhousedbops" {
  protocol = "native"
  host     = "localhost"
  port     = 9000
  
  auth_config = {
    strategy = "password"
    username = "default"
  }
}

# Create a test database
resource "clickhousedbops_database" "test_db" {
  name    = "terraform_test_db"
  comment = "Test database for table resource"
}

# Create a simple table
resource "clickhousedbops_table" "events" {
  database_name = clickhousedbops_database.test_db.name
  name          = "events"
  
  columns = [
    {
      name = "id"
      type = "UInt64"
    },
    {
      name = "timestamp"
      type = "DateTime"
      default = "now()"
    },
    {
      name = "event_type"
      type = "String"
    },
    {
      name = "user_id"
      type = "UInt32"
    },
    {
      name = "data"
      type = "String"
    }
  ]
  
  engine   = "MergeTree()"
  order_by = ["timestamp", "id"]
  
  comment = "Events tracking table"
}

# Create a table with more advanced features
resource "clickhousedbops_table" "user_activity" {
  database_name = clickhousedbops_database.test_db.name
  name          = "user_activity"
  
  columns = [
    {
      name = "user_id"
      type = "UInt32"
    },
    {
      name = "activity_date"
      type = "Date"
    },
    {
      name = "activity_type"
      type = "Enum8('login' = 1, 'purchase' = 2, 'view' = 3, 'click' = 4)"
    },
    {
      name = "timestamp"
      type = "DateTime"
    },
    {
      name = "properties"
      type = "Map(String, String)"
      default = "map()"
    }
  ]
  
  engine       = "MergeTree()"
  order_by     = ["user_id", "activity_date", "timestamp"]
  partition_by = "toYYYYMM(activity_date)"
  primary_key  = ["user_id", "activity_date"]
  ttl          = "activity_date + INTERVAL 365 DAY"
  
  settings = {
    index_granularity = "8192"
  }
  
  comment = "User activity tracking with partitioning and TTL"
}

output "database_name" {
  value = clickhousedbops_database.test_db.name
}

output "tables_created" {
  value = {
    events        = clickhousedbops_table.events.name
    user_activity = clickhousedbops_table.user_activity.name
  }
}
