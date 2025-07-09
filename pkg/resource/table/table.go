package table

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/pingcap/errors"

	"github.com/anglinb/terraform-provider-clickhousedbops/internal/dbops"
	"github.com/anglinb/terraform-provider-clickhousedbops/internal/querybuilder"
)

//go:embed table.md
var tableResourceDescription string

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &Resource{}
	_ resource.ResourceWithConfigure   = &Resource{}
	_ resource.ResourceWithImportState = &Resource{}
	_ resource.ResourceWithModifyPlan  = &Resource{}
)

// NewResource is a helper function to simplify the provider implementation.
func NewResource() resource.Resource {
	return &Resource{}
}

// Resource is the resource implementation.
type Resource struct {
	client dbops.Client
}

// Metadata returns the resource type name.
func (r *Resource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

// Schema defines the schema for the resource.
func (r *Resource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"cluster_name": schema.StringAttribute{
				Optional:    true,
				Description: "Name of the cluster to create the table into. If omitted, the table will be created on the replica hit by the query.\nThis field must be left null when using a ClickHouse Cloud cluster.\nShould be set when hitting a cluster with more than one replica.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"uuid": schema.StringAttribute{
				Computed:    true,
				Description: "The system-assigned UUID for the table",
			},
			"database_name": schema.StringAttribute{
				Required:    true,
				Description: "Name of the database containing the table",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				Required:    true,
				Description: "Name of the table",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"engine": schema.StringAttribute{
				Required:    true,
				Description: "Table engine (e.g., MergeTree(), ReplacingMergeTree(), Log, Memory)",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"columns": schema.ListNestedAttribute{
				Required:    true,
				Description: "List of columns in the table. New columns can be added without recreating the table. Removing columns or modifying existing columns requires table recreation.",
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Required:    true,
							Description: "Column name",
						},
						"type": schema.StringAttribute{
							Required:    true,
							Description: "Column data type (e.g., UInt64, String, DateTime)",
						},
						"default": schema.StringAttribute{
							Optional:    true,
							Description: "Default value or expression for the column",
						},
						"comment": schema.StringAttribute{
							Optional:    true,
							Description: "Column comment",
							Validators: []validator.String{
								stringvalidator.LengthAtMost(255),
							},
						},
					},
				},
				// Removed RequiresReplace - we'll handle updates in the Update method
			},
			"order_by": schema.ListAttribute{
				Optional:    true,
				Computed:    true,
				ElementType: types.StringType,
				Description: "ORDER BY clause columns",
				Default:     listdefault.StaticValue(types.ListValueMust(types.StringType, []attr.Value{})),
				Validators: []validator.List{
					listvalidator.SizeAtLeast(1),
				},
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"partition_by": schema.StringAttribute{
				Optional:    true,
				Description: "PARTITION BY expression",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"primary_key": schema.ListAttribute{
				Optional:    true,
				Computed:    true,
				ElementType: types.StringType,
				Description: "PRIMARY KEY columns",
				Default:     listdefault.StaticValue(types.ListValueMust(types.StringType, []attr.Value{})),
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"sample_by": schema.StringAttribute{
				Optional:    true,
				Description: "SAMPLE BY expression",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"ttl": schema.StringAttribute{
				Optional:    true,
				Description: "TTL expression",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"settings": schema.MapAttribute{
				Optional:    true,
				Computed:    true,
				ElementType: types.StringType,
				Description: "Table-level settings",
				Default:     mapdefault.StaticValue(types.MapValueMust(types.StringType, map[string]attr.Value{})),
				PlanModifiers: []planmodifier.Map{
					mapplanmodifier.RequiresReplace(),
				},
			},
			"comment": schema.StringAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Comment associated with the table",
				Default:     stringdefault.StaticString(""),
				Validators: []validator.String{
					stringvalidator.LengthAtMost(255),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"allow_drops": schema.BoolAttribute{
				Optional:    true,
				Computed:    true,
				Description: "Allow column and table drops. When set to false (default), attempts to remove columns or delete the table will fail as a safety measure. Set to true to allow destructive operations.",
				Default:     booldefault.StaticBool(false),
			},
		},
		MarkdownDescription: tableResourceDescription,
	}
}

func (r *Resource) Configure(_ context.Context, req resource.ConfigureRequest, _ *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	r.client = req.ProviderData.(dbops.Client)
}

func (r *Resource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan Table
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Convert columns from Terraform to dbops format
	columns := make([]querybuilder.TableColumn, len(plan.Columns))
	for i, col := range plan.Columns {
		columns[i] = querybuilder.TableColumn{
			Name:    col.Name.ValueString(),
			Type:    col.Type.ValueString(),
			Default: col.Default.ValueStringPointer(),
			Comment: col.Comment.ValueStringPointer(),
		}
	}

	// Convert order by list
	orderBy := []string{}
	if !plan.OrderBy.IsNull() {
		diags = plan.OrderBy.ElementsAs(ctx, &orderBy, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Convert primary key list
	primaryKey := []string{}
	if !plan.PrimaryKey.IsNull() {
		diags = plan.PrimaryKey.ElementsAs(ctx, &primaryKey, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	// Convert settings map
	settings := make(map[string]string)
	if !plan.Settings.IsNull() {
		diags = plan.Settings.ElementsAs(ctx, &settings, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	dbopsTable := dbops.Table{
		DatabaseName: plan.DatabaseName.ValueString(),
		Name:         plan.Name.ValueString(),
		Engine:       plan.Engine.ValueString(),
		Columns:      columns,
		OrderBy:      orderBy,
		PartitionBy:  plan.PartitionBy.ValueStringPointer(),
		PrimaryKey:   primaryKey,
		SampleBy:     plan.SampleBy.ValueStringPointer(),
		TTL:          plan.TTL.ValueStringPointer(),
		Settings:     settings,
		Comment:      plan.Comment.ValueString(),
	}

	table, err := r.client.CreateTable(ctx, dbopsTable, plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error creating table",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	state, err := r.syncTableState(ctx, table.UUID, plan.ClusterName.ValueStringPointer(), &plan)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error syncing table",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	if state == nil {
		resp.Diagnostics.AddError(
			"Error syncing table",
			"failed retrieving table after creation",
		)
		return
	}

	diags = resp.State.Set(ctx, state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
}

func (r *Resource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var plan Table
	diags := req.State.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	state, err := r.syncTableState(ctx, plan.UUID.ValueString(), plan.ClusterName.ValueStringPointer(), &plan)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error syncing table",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	if state == nil {
		resp.State.RemoveResource(ctx)
	} else {
		diags = resp.State.Set(ctx, state)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}
}

func (r *Resource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state Table
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Compare columns to find additions and removals
	stateColumns := make(map[string]Column)
	for _, col := range state.Columns {
		stateColumns[col.Name.ValueString()] = col
	}

	planColumns := make(map[string]Column)
	for _, col := range plan.Columns {
		planColumns[col.Name.ValueString()] = col
	}

	// Find new columns to add
	var columnsToAdd []querybuilder.TableColumn
	for _, planCol := range plan.Columns {
		colName := planCol.Name.ValueString()
		if _, exists := stateColumns[colName]; !exists {
			// This is a new column
			columnsToAdd = append(columnsToAdd, querybuilder.TableColumn{
				Name:    planCol.Name.ValueString(),
				Type:    planCol.Type.ValueString(),
				Default: planCol.Default.ValueStringPointer(),
				Comment: planCol.Comment.ValueStringPointer(),
			})
		}
	}

	// Find columns to remove
	var columnsToRemove []string
	for _, stateCol := range state.Columns {
		colName := stateCol.Name.ValueString()
		if _, exists := planColumns[colName]; !exists {
			// This column should be removed
			columnsToRemove = append(columnsToRemove, colName)
		}
	}

	// Remove columns if any
	if len(columnsToRemove) > 0 {
		// Check if drops are allowed
		if !plan.AllowDrops.ValueBool() {
			resp.Diagnostics.AddError(
				"Column removal not allowed",
				fmt.Sprintf("Cannot remove columns %v because 'allow_drops' is set to false. To allow column removal, set 'allow_drops = true' in your table configuration.", columnsToRemove),
			)
			return
		}
		
		err := r.client.DropTableColumns(ctx, state.DatabaseName.ValueString(), state.Name.ValueString(), columnsToRemove, state.ClusterName.ValueStringPointer())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error removing columns from table",
				fmt.Sprintf("Failed to remove columns: %+v\n", err),
			)
			return
		}
	}

	// Add new columns if any
	if len(columnsToAdd) > 0 {
		err := r.client.AddTableColumns(ctx, state.DatabaseName.ValueString(), state.Name.ValueString(), columnsToAdd, state.ClusterName.ValueStringPointer())
		if err != nil {
			resp.Diagnostics.AddError(
				"Error adding columns to table",
				fmt.Sprintf("Failed to add columns: %+v\n", err),
			)
			return
		}
	}

	// Sync state with the updated table
	updatedState, err := r.syncTableState(ctx, state.UUID.ValueString(), state.ClusterName.ValueStringPointer(), &plan)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error syncing table state",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}

	diags = resp.State.Set(ctx, updatedState)
	resp.Diagnostics.Append(diags...)
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var plan Table
	diags := req.State.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Check if drops are allowed
	if !plan.AllowDrops.ValueBool() {
		resp.Diagnostics.AddError(
			"Table deletion not allowed",
			fmt.Sprintf("Cannot delete table '%s' because 'allow_drops' is set to false. To allow table deletion, set 'allow_drops = true' in your table configuration.", plan.Name.ValueString()),
		)
		return
	}

	err := r.client.DeleteTable(ctx, plan.UUID.ValueString(), plan.ClusterName.ValueStringPointer())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error deleting table",
			fmt.Sprintf("%+v\n", err),
		)
		return
	}
}

func (r *Resource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// req.ID can either be in the form <cluster name>:<database name>:<table ref> or just <database name>:<table ref>
	// table ref can either be the name or the UUID of the table.

	parts := strings.Split(req.ID, ":")
	if len(parts) < 2 || len(parts) > 3 {
		resp.Diagnostics.AddError(
			"Invalid import ID format",
			"Import ID must be in format 'database_name:table_name' or 'cluster_name:database_name:table_name' or 'database_name:table_uuid'",
		)
		return
	}

	var clusterName *string
	var databaseName string
	var tableRef string

	if len(parts) == 3 {
		// cluster:database:table format
		clusterName = &parts[0]
		databaseName = parts[1]
		tableRef = parts[2]
	} else {
		// database:table format
		databaseName = parts[0]
		tableRef = parts[1]
	}

	// Check if ref is a UUID
	_, err := uuid.Parse(tableRef)
	if err != nil {
		// Failed parsing UUID, try importing using the table name
		table, err := r.client.FindTableByName(ctx, databaseName, tableRef, clusterName)
		if err != nil {
			resp.Diagnostics.AddError(
				"Cannot find table",
				fmt.Sprintf("%+v\n", err),
			)
			return
		}

		// Set basic attributes
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("uuid"), table.UUID)...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("database_name"), databaseName)...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), table.Name)...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("engine"), types.StringValue(table.Engine))...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("comment"), types.StringValue(table.Comment))...)
	} else {
		// User passed a UUID
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("uuid"), tableRef)...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("database_name"), databaseName)...)
	}

	if clusterName != nil {
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("cluster_name"), clusterName)...)
	}
}

// syncTableState reads table settings from clickhouse and returns a Table
func (r *Resource) syncTableState(ctx context.Context, uuid string, clusterName *string, plan *Table) (*Table, error) {
	table, err := r.client.GetTable(ctx, uuid, clusterName)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get table")
	}

	if table == nil {
		// Table not found.
		return nil, nil
	}

	// Convert columns
	columns := make([]Column, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = Column{
			Name:    types.StringValue(col.Name),
			Type:    types.StringValue(col.Type),
			Default: types.StringPointerValue(col.Default),
			Comment: types.StringPointerValue(col.Comment),
		}
	}

	// Convert order by
	orderByValues := make([]attr.Value, len(table.OrderBy))
	for i, col := range table.OrderBy {
		orderByValues[i] = types.StringValue(col)
	}
	orderByList, diags := types.ListValue(types.StringType, orderByValues)
	if diags.HasError() {
		return nil, errors.New("failed to create order by list")
	}

	// Convert primary key - handle auto-inference by ClickHouse
	var primaryKeyList types.List
	if plan != nil {
		// Get the planned primary key
		var plannedPrimaryKey []string
		if !plan.PrimaryKey.IsNull() {
			diags = plan.PrimaryKey.ElementsAs(ctx, &plannedPrimaryKey, false)
			if diags.HasError() {
				return nil, errors.New("failed to parse planned primary key")
			}
		}
		
		// If plan had empty primary key but ClickHouse inferred one, keep plan's empty list
		if len(plannedPrimaryKey) == 0 && len(table.PrimaryKey) > 0 {
			primaryKeyList = plan.PrimaryKey
		} else {
			primaryKeyValues := make([]attr.Value, len(table.PrimaryKey))
			for i, col := range table.PrimaryKey {
				primaryKeyValues[i] = types.StringValue(col)
			}
			primaryKeyList, diags = types.ListValue(types.StringType, primaryKeyValues)
			if diags.HasError() {
				return nil, errors.New("failed to create primary key list")
			}
		}
	} else {
		primaryKeyValues := make([]attr.Value, len(table.PrimaryKey))
		for i, col := range table.PrimaryKey {
			primaryKeyValues[i] = types.StringValue(col)
		}
		primaryKeyList, diags = types.ListValue(types.StringType, primaryKeyValues)
		if diags.HasError() {
			return nil, errors.New("failed to create primary key list")
		}
	}

	// Convert settings - only include settings that were explicitly set in the plan
	settingsMap := make(map[string]attr.Value)
	if plan != nil && !plan.Settings.IsNull() {
		// Get planned settings
		var plannedSettings map[string]string
		diags = plan.Settings.ElementsAs(ctx, &plannedSettings, false)
		if diags.HasError() {
			return nil, errors.New("failed to parse planned settings")
		}
		// Only include settings that were in the plan
		for k := range plannedSettings {
			if v, ok := table.Settings[k]; ok {
				settingsMap[k] = types.StringValue(v)
			}
		}
	}
	settings, diags := types.MapValue(types.StringType, settingsMap)
	if diags.HasError() {
		return nil, errors.New("failed to create settings map")
	}

	// Handle engine normalization - especially for ClickHouse Cloud
	engine := types.StringValue(table.Engine)
	if plan != nil && !plan.Engine.IsNull() {
		// Check if this is a ClickHouse Cloud engine transformation
		plannedEngine := plan.Engine.ValueString()
		actualEngine := table.Engine
		
		// Normalize engine names for comparison (remove parentheses and parameters)
		normalizedPlanned := normalizeEngineName(plannedEngine)
		normalizedActual := normalizeEngineName(actualEngine)
		
		// Check if this is an expected Cloud transformation
		if isCloudEngineTransformation(normalizedPlanned, normalizedActual) {
			// Keep the planned engine to avoid drift
			engine = plan.Engine
		} else if normalizedPlanned == normalizedActual {
			// Same engine type, just different formatting - keep planned value
			engine = plan.Engine
		} else {
			// This is an actual engine change - use the actual value
			engine = types.StringValue(table.Engine)
		}
	}

	// For TTL, use the plan value if available to avoid normalization issues
	ttl := types.StringPointerValue(table.TTL)
	if plan != nil && !plan.TTL.IsNull() && table.TTL != nil {
		ttl = plan.TTL
	}

	// Preserve the allow_drops setting from the plan
	var allowDrops types.Bool
	if plan != nil {
		allowDrops = plan.AllowDrops
	} else {
		allowDrops = types.BoolValue(false)
	}

	state := &Table{
		ClusterName:  types.StringPointerValue(clusterName),
		UUID:         types.StringValue(table.UUID),
		DatabaseName: types.StringValue(table.DatabaseName),
		Name:         types.StringValue(table.Name),
		Columns:      columns,
		Engine:       engine,
		OrderBy:      orderByList,
		PartitionBy:  types.StringPointerValue(table.PartitionBy),
		PrimaryKey:   primaryKeyList,
		SampleBy:     types.StringPointerValue(table.SampleBy),
		TTL:          ttl,
		Settings:     settings,
		Comment:      types.StringValue(table.Comment),
		AllowDrops:   allowDrops,
	}

	return state, nil
}

// normalizeEngineName extracts the base engine name without parameters
func normalizeEngineName(engine string) string {
	// Remove everything after the first parenthesis
	if idx := strings.Index(engine, "("); idx != -1 {
		return strings.TrimSpace(engine[:idx])
	}
	return strings.TrimSpace(engine)
}

// isCloudEngineTransformation checks if the engine change is an expected ClickHouse Cloud transformation
func isCloudEngineTransformation(planned, actual string) bool {
	// Map of engines that get transformed in ClickHouse Cloud
	cloudTransformations := map[string]string{
		"MergeTree":          "SharedMergeTree",
		"ReplacingMergeTree": "SharedReplacingMergeTree",
		"SummingMergeTree":   "SharedSummingMergeTree",
		"AggregatingMergeTree": "SharedAggregatingMergeTree",
		"CollapsingMergeTree": "SharedCollapsingMergeTree",
		"VersionedCollapsingMergeTree": "SharedVersionedCollapsingMergeTree",
	}
	
	// Check if this is a known transformation
	if expectedEngine, ok := cloudTransformations[planned]; ok {
		return actual == expectedEngine
	}
	
	// Also check the reverse (in case someone explicitly uses SharedMergeTree)
	for original, shared := range cloudTransformations {
		if planned == shared && actual == original {
			return true
		}
	}
	
	return false
}

// ModifyPlan checks if column changes require table recreation
func (r *Resource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	// If the entire resource is being destroyed, skip this check
	if req.Plan.Raw.IsNull() {
		return
	}

	// If this is a create operation, skip this check
	if req.State.Raw.IsNull() {
		return
	}

	var plan, state Table
	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Build maps for comparison
	stateColumns := make(map[string]Column)
	for _, col := range state.Columns {
		stateColumns[col.Name.ValueString()] = col
	}

	planColumns := make(map[string]Column)
	for _, col := range plan.Columns {
		planColumns[col.Name.ValueString()] = col
	}

	// Get order by columns for checking
	var orderByColumns []string
	if !state.OrderBy.IsNull() {
		diags = state.OrderBy.ElementsAs(ctx, &orderByColumns, false)
		if diags.HasError() {
			resp.Diagnostics.Append(diags...)
			return
		}
	}

	// Create a set of order by columns for quick lookup
	orderBySet := make(map[string]bool)
	for _, col := range orderByColumns {
		orderBySet[col] = true
	}

	// Check for removed or modified columns
	requiresReplace := false
	for _, stateCol := range state.Columns {
		colName := stateCol.Name.ValueString()
		planCol, exists := planColumns[colName]
		
		if !exists {
			// Column was removed - check if drops are allowed
			if !plan.AllowDrops.ValueBool() {
				resp.Diagnostics.AddError(
					"Column removal not allowed",
					fmt.Sprintf("Column '%s' cannot be removed because 'allow_drops' is set to false. To allow column removal, set 'allow_drops = true' in your table configuration.", colName),
				)
				return
			}
			
			// Check if it's in ORDER BY
			if orderBySet[colName] {
				resp.Diagnostics.AddWarning(
					"Cannot remove column in ORDER BY",
					fmt.Sprintf("Column '%s' is part of the table's ORDER BY clause and cannot be removed. This requires recreating the table.", colName),
				)
				requiresReplace = true
			}
			// Otherwise, column can be dropped without recreation
		} else if !stateCol.Type.Equal(planCol.Type) {
			// Column type changed
			resp.Diagnostics.AddWarning(
				"Column type change requires table recreation",
				fmt.Sprintf("Column '%s' type change from '%s' to '%s' requires recreating the table.", 
					colName, stateCol.Type.ValueString(), planCol.Type.ValueString()),
			)
			requiresReplace = true
		}
	}

	// If recreation is required, mark the resource for replacement
	if requiresReplace {
		resp.RequiresReplace = append(resp.RequiresReplace, path.Root("columns"))
	}
}
