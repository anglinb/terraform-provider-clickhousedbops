package table

import (
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type Table struct {
	ClusterName  types.String `tfsdk:"cluster_name"`
	UUID         types.String `tfsdk:"uuid"`
	DatabaseName types.String `tfsdk:"database_name"`
	Name         types.String `tfsdk:"name"`
	Columns      []Column     `tfsdk:"columns"`
	Engine       types.String `tfsdk:"engine"`
	OrderBy      types.List   `tfsdk:"order_by"`
	PartitionBy  types.String `tfsdk:"partition_by"`
	PrimaryKey   types.List   `tfsdk:"primary_key"`
	SampleBy     types.String `tfsdk:"sample_by"`
	TTL          types.String `tfsdk:"ttl"`
	Settings     types.Map    `tfsdk:"settings"`
	Comment      types.String `tfsdk:"comment"`
	AllowDrops   types.Bool   `tfsdk:"allow_drops"`
}

type Column struct {
	Name    types.String `tfsdk:"name"`
	Type    types.String `tfsdk:"type"`
	Default types.String `tfsdk:"default"`
	Comment types.String `tfsdk:"comment"`
}
