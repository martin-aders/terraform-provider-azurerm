package cosmosdb

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/hashicorp/go-azure-helpers/polling"
)

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.

type GremlinResourcesMigrateGremlinGraphToManualThroughputOperationResponse struct {
	Poller       polling.LongRunningPoller
	HttpResponse *http.Response
	Model        *ThroughputSettingsGetResults
}

// GremlinResourcesMigrateGremlinGraphToManualThroughput ...
func (c CosmosDBClient) GremlinResourcesMigrateGremlinGraphToManualThroughput(ctx context.Context, id GraphId) (result GremlinResourcesMigrateGremlinGraphToManualThroughputOperationResponse, err error) {
	req, err := c.preparerForGremlinResourcesMigrateGremlinGraphToManualThroughput(ctx, id)
	if err != nil {
		err = autorest.NewErrorWithError(err, "cosmosdb.CosmosDBClient", "GremlinResourcesMigrateGremlinGraphToManualThroughput", nil, "Failure preparing request")
		return
	}

	result, err = c.senderForGremlinResourcesMigrateGremlinGraphToManualThroughput(ctx, req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "cosmosdb.CosmosDBClient", "GremlinResourcesMigrateGremlinGraphToManualThroughput", result.HttpResponse, "Failure sending request")
		return
	}

	return
}

// GremlinResourcesMigrateGremlinGraphToManualThroughputThenPoll performs GremlinResourcesMigrateGremlinGraphToManualThroughput then polls until it's completed
func (c CosmosDBClient) GremlinResourcesMigrateGremlinGraphToManualThroughputThenPoll(ctx context.Context, id GraphId) error {
	result, err := c.GremlinResourcesMigrateGremlinGraphToManualThroughput(ctx, id)
	if err != nil {
		return fmt.Errorf("performing GremlinResourcesMigrateGremlinGraphToManualThroughput: %+v", err)
	}

	if err := result.Poller.PollUntilDone(); err != nil {
		return fmt.Errorf("polling after GremlinResourcesMigrateGremlinGraphToManualThroughput: %+v", err)
	}

	return nil
}

// preparerForGremlinResourcesMigrateGremlinGraphToManualThroughput prepares the GremlinResourcesMigrateGremlinGraphToManualThroughput request.
func (c CosmosDBClient) preparerForGremlinResourcesMigrateGremlinGraphToManualThroughput(ctx context.Context, id GraphId) (*http.Request, error) {
	queryParameters := map[string]interface{}{
		"api-version": defaultApiVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPost(),
		autorest.WithBaseURL(c.baseUri),
		autorest.WithPath(fmt.Sprintf("%s/throughputSettings/default/migrateToManualThroughput", id.ID())),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// senderForGremlinResourcesMigrateGremlinGraphToManualThroughput sends the GremlinResourcesMigrateGremlinGraphToManualThroughput request. The method will close the
// http.Response Body if it receives an error.
func (c CosmosDBClient) senderForGremlinResourcesMigrateGremlinGraphToManualThroughput(ctx context.Context, req *http.Request) (future GremlinResourcesMigrateGremlinGraphToManualThroughputOperationResponse, err error) {
	var resp *http.Response
	resp, err = c.Client.Send(req, azure.DoRetryWithRegistration(c.Client))
	if err != nil {
		return
	}

	future.Poller, err = polling.NewPollerFromResponse(ctx, resp, c.Client, req.Method)
	return
}
