package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/anglinb/terraform-provider-clickhousedbops/pkg/provider"
)

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	opts := providerserver.ServeOpts{
		Address: "registry.terraform.io/anglinb/clickhousedbops",
		Debug:   debug,
	}

	err := providerserver.Serve(context.Background(), provider.New(), opts)
	if err != nil {
		log.Fatal(err.Error())
	}
}
