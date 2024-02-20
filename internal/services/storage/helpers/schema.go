package helpers

import (
	"context"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
)

type StorageIDValidationFunc func(id, storageDomainSuffix string) error

func ImporterValidatingStorageResourceId(validateFunc StorageIDValidationFunc) *schema.ResourceImporter {
	return &schema.ResourceImporter{
		StateContext: func(_ context.Context, d *pluginsdk.ResourceData, meta interface{}) ([]*pluginsdk.ResourceData, error) {
			storageDomainSuffix := meta.(*clients.Client).Storage.StorageDomainSuffix
			log.Printf("[DEBUG] Importing Storage Resource - parsing %q using domain suffix %q", d.Id(), storageDomainSuffix)
			return []*pluginsdk.ResourceData{d}, validateFunc(d.Id(), storageDomainSuffix)
		},
	}
}
