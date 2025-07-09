package dbops

import (
	"github.com/anglinb/terraform-provider-clickhousedbops/internal/clickhouseclient"
)

type impl struct {
	clickhouseClient clickhouseclient.ClickhouseClient
}

func NewClient(clickhouseClient clickhouseclient.ClickhouseClient) (Client, error) {
	return &impl{
		clickhouseClient: clickhouseClient,
	}, nil
}
