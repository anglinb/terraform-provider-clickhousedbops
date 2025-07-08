package role_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"

	testutils "github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/compose"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/dbopsclient"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/factories"
	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/providerconfig"
	internalstatecheck "github.com/ClickHouse/terraform-provider-clickhousedbops/internal/testutils/statecheck"
)

const resourceName = "foo"

func TestRole_acceptance(t *testing.T) {
	// Prepare docker compose to run local clickhouse cluster.
	dcm := testutils.NewDockerComposeManager("../../../tests")

	tests := []struct {
		name     string
		chEnv    map[string]string
		protocol string
		resource string
	}{
		{
			name:     "Create Role using Native protocol on a single replica",
			chEnv:    map[string]string{"CONFIGFILE": "config-single.xml"},
			protocol: "native",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).Build(),
		},
		{
			name:     "Create Role using HTTP protocol on a single replica",
			chEnv:    map[string]string{"CONFIGFILE": "config-single.xml"},
			protocol: "http",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).Build(),
		},
		{
			name:     "Create Role using Native protocol on a cluster using replicated storage",
			chEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			protocol: "native",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).Build(),
		},
		{
			name:     "Create Role using HTTP protocol on a cluster using replicated storage",
			chEnv:    map[string]string{"CONFIGFILE": "config-replicated.xml"},
			protocol: "http",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).Build(),
		},
		{
			name:     "Create Role using Native protocol on a cluster using localfile storage",
			chEnv:    map[string]string{"CONFIGFILE": "config-localfile.xml"},
			protocol: "native",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).WithClusterName("cluster1").Build(),
		},
		{
			name:     "Create Role using HTTP protocol on a cluster using localfile storage",
			chEnv:    map[string]string{"CONFIGFILE": "config-localfile.xml"},
			protocol: "http",
			resource: newRoleResourceBuilder(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)).WithClusterName("cluster1").Build(),
		},
	}

	for _, tc := range tests {
		// Start CH cluster using docker compose.
		if err := dcm.Up(tc.chEnv); err != nil {
			t.Fatal(err)
		}

		dbopsClient, connSettings, err := dbopsclient.NewDbopsClient(tc.protocol)
		if err != nil {
			t.Fatal(err)
		}

		providerCfg, err := providerconfig.ProviderConfig(tc.protocol, connSettings.Host, connSettings.Port, connSettings.Username, connSettings.Password)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(tc.name, func(t *testing.T) {
			resourceAddress := fmt.Sprintf("clickhousedbops_role.%s", resourceName)

			resource.Test(t, resource.TestCase{
				ProtoV6ProviderFactories: factories.ProviderFactories(),
				CheckDestroy: internalstatecheck.CheckNotExist(t.Context(), resourceAddress, func(ctx context.Context, attrs map[string]string) (bool, error) {
					id := attrs["id"]
					if id == "" {
						return false, fmt.Errorf("id attribute was not set")
					}
					role, err := dbopsClient.GetRole(ctx, id, nil)
					return role != nil, err
				}),
				Steps: []resource.TestStep{
					{
						// Combine the provider definition and the resourcePtr definition.
						Config: fmt.Sprintf("%s\n%s", providerCfg, tc.resource),
						ConfigStateChecks: []statecheck.StateCheck{
							// Compare the state with the actual resource.
							internalstatecheck.NewGetAttributes(resourceAddress, func(attrs map[string]interface{}) error {
								id := attrs["id"]
								if id == nil {
									return fmt.Errorf("id was nil")
								}

								role, err := dbopsClient.GetRole(t.Context(), id.(string), nil)
								if err != nil {
									return err
								}

								if role == nil {
									return fmt.Errorf("role with id %q was not found", id)
								}

								// Check state fields are aligned with the role we retrieved from CH.
								if attrs["name"].(string) != role.Name {
									return fmt.Errorf("expected name to be %q, was %q", role.Name, attrs["name"].(string))
								}

								return nil
							}),
						},
					},
				},
			})

			// Take clickhouse cluster down.
			err = dcm.Down()
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

type roleResourceBuilder struct {
	name    string
	cluster string
}

func newRoleResourceBuilder(name string) *roleResourceBuilder {
	return &roleResourceBuilder{name: name}
}

func (b *roleResourceBuilder) WithClusterName(cluster string) *roleResourceBuilder {
	b.cluster = cluster

	return b
}

func (b *roleResourceBuilder) Build() string {
	ret := fmt.Sprintf(`
resource "clickhousedbops_role" "%s" {
  name = "%s"
`, resourceName, b.name)

	if b.cluster != "" {
		ret = fmt.Sprintf(`%scluster_name = "%s"
`, ret, b.cluster)
	}

	ret = ret + `}
`

	return ret
}
