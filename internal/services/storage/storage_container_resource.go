// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package storage

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/go-azure-helpers/resourcemanager/commonids"
	"github.com/hashicorp/terraform-provider-azurerm/helpers/tf"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/helpers"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/migration"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/parse"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/validate"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/validation"
	"github.com/hashicorp/terraform-provider-azurerm/internal/timeouts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/containers"
)

func resourceStorageContainer() *pluginsdk.Resource {
	return &pluginsdk.Resource{
		Create: resourceStorageContainerCreate,
		Read:   resourceStorageContainerRead,
		Delete: resourceStorageContainerDelete,
		Update: resourceStorageContainerUpdate,

		Importer: helpers.ImporterValidatingStorageResourceId(func(id, storageDomainSuffix string) error {
			_, err := parse.StorageContainerDataPlaneID(id, storageDomainSuffix)
			return err
		}),

		SchemaVersion: 1,
		StateUpgraders: pluginsdk.StateUpgrades(map[int]pluginsdk.StateUpgrade{
			0: migration.ContainerV0ToV1{},
		}),

		Timeouts: &pluginsdk.ResourceTimeout{
			Create: pluginsdk.DefaultTimeout(30 * time.Minute),
			Read:   pluginsdk.DefaultTimeout(5 * time.Minute),
			Update: pluginsdk.DefaultTimeout(30 * time.Minute),
			Delete: pluginsdk.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*pluginsdk.Schema{
			"name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.StorageContainerName,
			},

			"storage_account_name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.StorageAccountName,
			},

			"container_access_type": {
				Type:     pluginsdk.TypeString,
				Optional: true,
				Default:  "private",
				ValidateFunc: validation.StringInSlice([]string{
					string(containers.Blob),
					string(containers.Container),
					"private",
				}, false),
			},

			"metadata": MetaDataComputedSchema(),

			// TODO: support for ACL's, Legal Holds and Immutability Policies
			"has_immutability_policy": {
				Type:     pluginsdk.TypeBool,
				Computed: true,
			},

			"has_legal_hold": {
				Type:     pluginsdk.TypeBool,
				Computed: true,
			},

			"resource_manager_id": {
				Type:     pluginsdk.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceStorageContainerCreate(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForCreate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	containerName := d.Get("name").(string)
	accountName := d.Get("storage_account_name").(string)
	accessLevelRaw := d.Get("container_access_type").(string)
	accessLevel := expandStorageContainerAccessLevel(accessLevelRaw)

	metaDataRaw := d.Get("metadata").(map[string]interface{})
	metaData := ExpandMetaData(metaDataRaw)

	account, err := storageClient.FindAccount(ctx, accountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Container %q: %v", accountName, containerName, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", accountName)
	}

	client, err := storageClient.ContainersDataPlaneClient(ctx, *account, storageClient.DataPlaneOperationSupportingAnyAuthMethod())
	if err != nil {
		return fmt.Errorf("building storage client: %v", err)
	}

	id := parse.NewStorageContainerDataPlaneId(accountName, storageClient.AzureEnvironment.StorageEndpointSuffix, containerName)

	exists, err := client.Exists(ctx, containerName)
	if err != nil {
		return fmt.Errorf("checking for existing %s: %v", id, err)
	}
	if exists != nil && *exists {
		return tf.ImportAsExistsError("azurerm_storage_container", id.ID())
	}

	log.Printf("[INFO] Creating %s", id)
	input := containers.CreateInput{
		AccessLevel: accessLevel,
		MetaData:    metaData,
	}

	if err = client.Create(ctx, containerName, input); err != nil {
		return fmt.Errorf("creating %s: %v", id, err)
	}

	d.SetId(id.ID())

	return resourceStorageContainerRead(d, meta)
}

func resourceStorageContainerUpdate(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageContainerDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Container %q: %v", id.AccountName, id.Name, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountName)
	}

	if d.HasChange("container_access_type") {
		log.Printf("[DEBUG] Updating Access Level for %s...", id)

		// Updating metadata does not work with AAD authentication, returns a cryptic 404
		client, err := storageClient.ContainersDataPlaneClient(ctx, *account, storageClient.DataPlaneOperationSupportingOnlySharedKeyAuth())
		if err != nil {
			return fmt.Errorf("building Containers Client: %v", err)
		}

		accessLevelRaw := d.Get("container_access_type").(string)
		accessLevel := expandStorageContainerAccessLevel(accessLevelRaw)

		if err = client.UpdateAccessLevel(ctx, id.Name, accessLevel); err != nil {
			return fmt.Errorf("updating Access Level for %s: %v", id, err)
		}

		log.Printf("[DEBUG] Updated Access Level for %s", id)
	}

	if d.HasChange("metadata") {
		log.Printf("[DEBUG] Updating Metadata for %s...", id)

		client, err := storageClient.ContainersDataPlaneClient(ctx, *account, storageClient.DataPlaneOperationSupportingAnyAuthMethod())
		if err != nil {
			return fmt.Errorf("building Containers Client: %v", err)
		}

		metaDataRaw := d.Get("metadata").(map[string]interface{})
		metaData := ExpandMetaData(metaDataRaw)

		if err = client.UpdateMetaData(ctx, id.Name, metaData); err != nil {
			return fmt.Errorf("updating Metadata for %s: %v", id, err)
		}

		log.Printf("[DEBUG] Updated Metadata for %s", id)
	}

	return resourceStorageContainerRead(d, meta)
}

func resourceStorageContainerRead(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	subscriptionId := meta.(*clients.Client).Account.SubscriptionId
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageContainerDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Container %q: %v", id.AccountName, id.Name, err)
	}
	if account == nil {
		log.Printf("[DEBUG] Unable to locate Account %q for Storage Container %q - assuming removed & removing from state", id.AccountName, id.Name)
		d.SetId("")
		return nil
	}

	client, err := storageClient.ContainersDataPlaneClient(ctx, *account, storageClient.DataPlaneOperationSupportingAnyAuthMethod())
	if err != nil {
		return fmt.Errorf("building Containers Client: %v", err)
	}

	props, err := client.Get(ctx, id.Name)
	if err != nil {
		return fmt.Errorf("retrieving %s: %v", id, err)
	}
	if props == nil {
		log.Printf("[DEBUG] Container %q was not found in Account %q / Resource Group %q - assuming removed & removing from state", id.Name, id.AccountName, account.ResourceGroup)
		d.SetId("")
		return nil
	}

	d.Set("name", id.Name)
	d.Set("storage_account_name", id.AccountName)

	d.Set("container_access_type", flattenStorageContainerAccessLevel(props.AccessLevel))

	if err = d.Set("metadata", FlattenMetaData(props.MetaData)); err != nil {
		return fmt.Errorf("setting `metadata`: %v", err)
	}

	d.Set("has_immutability_policy", props.HasImmutabilityPolicy)
	d.Set("has_legal_hold", props.HasLegalHold)

	resourceManagerId := commonids.NewStorageContainerID(subscriptionId, account.ResourceGroup, id.AccountName, id.Name)
	d.Set("resource_manager_id", resourceManagerId.ID())

	return nil
}

func resourceStorageContainerDelete(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageContainerDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Container %q: %v", id.AccountName, id.Name, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountName)
	}

	client, err := storageClient.ContainersDataPlaneClient(ctx, *account, storageClient.DataPlaneOperationSupportingAnyAuthMethod())
	if err != nil {
		return fmt.Errorf("building Containers Client: %v", err)
	}

	if err = client.Delete(ctx, id.Name); err != nil {
		return fmt.Errorf("deleting %s: %v", id, err)
	}

	return nil
}

func expandStorageContainerAccessLevel(input string) containers.AccessLevel {
	// for historical reasons, "private" above is an empty string in the API
	// so the enum doesn't 1:1 match. You could argue the SDK should handle this
	// but this is suitable for now
	if input == "private" {
		return containers.Private
	}

	return containers.AccessLevel(input)
}

func flattenStorageContainerAccessLevel(input containers.AccessLevel) string {
	// for historical reasons, "private" above is an empty string in the API
	if input == containers.Private {
		return "private"
	}

	return string(input)
}
