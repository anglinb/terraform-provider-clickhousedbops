# Local Testing Instructions

## Prerequisites
- ClickHouse running locally (you already have this)
- Terraform installed
- Provider built to `tmp/terraform-provider-clickhousedbops`

## Testing Steps

1. **Navigate to the test directory:**
   ```bash
   cd /Users/brian/superwall/terraform-provider-clickhousedbops/local-test
   ```

2. **Run terraform plan (no init needed with dev overrides):**
   ```bash
   terraform plan
   ```

3. **Apply the configuration:**
   ```bash
   terraform apply
   ```

4. **Verify the tables were created:**
   ```bash
   # Using clickhouse-client
   clickhouse-client --password test -q "SHOW TABLES FROM terraform_test_db"
   
   # Or using HTTP interface
   curl -u default:test 'http://localhost:8123/?query=SHOW%20TABLES%20FROM%20terraform_test_db'
   ```

5. **Check table structure:**
   ```bash
   clickhouse-client --password test -q "DESCRIBE terraform_test_db.events"
   clickhouse-client --password test -q "DESCRIBE terraform_test_db.user_activity"
   ```

6. **Test import functionality:**
   ```bash
   # Import by name
   terraform import clickhousedbops_table.new_table "terraform_test_db:events"
   
   # Import by UUID (get UUID first)
   clickhouse-client --password test -q "SELECT uuid FROM system.tables WHERE database='terraform_test_db' AND name='events'"
   ```

7. **Clean up:**
   ```bash
   terraform destroy
   ```

## What This Tests

The configuration creates:
1. A database named `terraform_test_db`
2. A simple `events` table with basic columns
3. A more complex `user_activity` table with:
   - Enum type column
   - Map type column
   - Partitioning by month
   - TTL (365 days)
   - Custom settings

## Troubleshooting

If you get authentication errors:
- Make sure your ClickHouse instance has password `test` for the `default` user
- Or update the password in `main.tf` to match your setup

If the provider isn't found:
- Make sure `~/.terraformrc` exists with the dev_overrides
- Ensure the provider binary exists at `/Users/brian/superwall/terraform-provider-clickhousedbops/tmp/terraform-provider-clickhousedbops`