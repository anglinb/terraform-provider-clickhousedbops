package statecheck

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func CheckNotExist(ctx context.Context, resourceAddress string, checker func(context.Context, map[string]string) (bool, error)) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		for address, r := range s.RootModule().Resources {
			if resourceAddress == address {
				exists, err := checker(ctx, r.Primary.Attributes)
				if err != nil {
					return err
				}

				if exists {
					return fmt.Errorf("expected resource to NOT exist, but it does")
				}

				return nil
			}
		}

		return fmt.Errorf("root module has no resource %q", resourceAddress)
	}
}
