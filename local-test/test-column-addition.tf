# Test configuration for column addition feature

resource "clickhousedbops_table" "mutable_table" {
  database_name = clickhousedbops_database.test_db.name
  name          = "mutable_test_table"
  
  columns = [
    {
      name = "id"
      type = "UInt64"
    },
    {
      name = "created_at"
      type = "DateTime"
      default = "now()"
    },
    # New columns added via update
    # Removed new_column1 to test recreation
    {
      name = "new_column2"
      type = "Float64"
      comment = "Another new column"
    }
  ]
  
  engine   = "MergeTree()"
  order_by = ["id"]
  
  comment = "Table to test column mutations"
}

output "mutable_table_info" {
  value = {
    name    = clickhousedbops_table.mutable_table.name
    columns = [for col in clickhousedbops_table.mutable_table.columns : col.name]
  }
}