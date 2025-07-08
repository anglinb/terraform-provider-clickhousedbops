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

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/querybuilder"
)

//go:embed table.md
var tableResourceDescription string

// Ensure the implementation satisfies the expected interfaces.
var (
	_ resource.Resource                = &Resource{}
	_ resource.ResourceWithConfigure   = &Resource{}
	_ resource.ResourceWithImportState = &Resource{}
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
				Description: "List of columns in the table",
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
				PlanModifiers: []planmodifier.List{
					// Any change to columns requires table recreation
					listplanmodifier.RequiresReplace(),
				},
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

	state, err := r.syncTableState(ctx, table.UUID, plan.ClusterName.ValueStringPointer())
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

	state, err := r.syncTableState(ctx, plan.UUID.ValueString(), plan.ClusterName.ValueStringPointer())
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
	panic("unsupported")
}

func (r *Resource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var plan Table
	diags := req.State.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
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

		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("uuid"), table.UUID)...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("database_name"), databaseName)...)
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
func (r *Resource) syncTableState(ctx context.Context, uuid string, clusterName *string) (*Table, error) {
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

	// Convert primary key
	primaryKeyValues := make([]attr.Value, len(table.PrimaryKey))
	for i, col := range table.PrimaryKey {
		primaryKeyValues[i] = types.StringValue(col)
	}
	primaryKeyList, diags := types.ListValue(types.StringType, primaryKeyValues)
	if diags.HasError() {
		return nil, errors.New("failed to create primary key list")
	}

	// Convert settings
	settingsMap := make(map[string]attr.Value)
	for k, v := range table.Settings {
		settingsMap[k] = types.StringValue(v)
	}
	settings, diags := types.MapValue(types.StringType, settingsMap)
	if diags.HasError() {
		return nil, errors.New("failed to create settings map")
	}

	state := &Table{
		ClusterName:  types.StringPointerValue(clusterName),
		UUID:         types.StringValue(table.UUID),
		DatabaseName: types.StringValue(table.DatabaseName),
		Name:         types.StringValue(table.Name),
		Columns:      columns,
		Engine:       types.StringValue(table.Engine),
		OrderBy:      orderByList,
		PartitionBy:  types.StringPointerValue(table.PartitionBy),
		PrimaryKey:   primaryKeyList,
		SampleBy:     types.StringPointerValue(table.SampleBy),
		TTL:          types.StringPointerValue(table.TTL),
		Settings:     settings,
		Comment:      types.StringValue(table.Comment),
	}

	return state, nil
}
