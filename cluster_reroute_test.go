// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"net/url"
	"strings"
	"testing"
)

func TestClusterRerouteURLs(t *testing.T) {
	trueFlag := true
	truePtr := &trueFlag

	tests := []struct {
		Service        *ClusterRerouteService
		ExpectedPath   string
		ExpectedParams url.Values
	}{
		{
			Service:      &ClusterRerouteService{},
			ExpectedPath: "/_cluster/reroute",
		},
		{
			Service: &ClusterRerouteService{
				dryRun:  truePtr,
				metrics: []string{"blocks", "nodes"},
			},
			ExpectedPath: "/_cluster/reroute",
			ExpectedParams: url.Values{
				"dry_run": []string{"true"},
				"metric":  []string{"blocks,nodes"},
			},
		},
	}

	for _, tt := range tests {
		gotPath, gotParams, err := tt.Service.buildURL()
		if err != nil {
			t.Fatalf("expected no error; got: %v", err)
		}
		if gotPath != tt.ExpectedPath {
			t.Errorf("expected URL path = %q; got: %q", tt.ExpectedPath, gotPath)
		}
		if gotParams.Encode() != tt.ExpectedParams.Encode() {
			t.Errorf("expected URL params = %v; got: %v", tt.ExpectedParams, gotParams)
		}
	}
}

func TestClusterReroute(t *testing.T) {
	// client := setupTestClientAndCreateIndex(t, SetTraceLog(log.New(os.Stdout, "", 0)))
	client := setupTestClientAndCreateIndex(t)

	t.Run("Commands", func(t *testing.T) {
		testClusterRerouteWithCommands(client, t)
	})
	t.Run("NoBody", func(t *testing.T) {
		testClusterRerouteWithoutBody(client, t)
	})
}

func testClusterRerouteWithCommands(client *Client, t *testing.T) {
	// Get cluster nodes
	var nodes []string
	{
		res, err := client.ClusterState().Do(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for node := range res.Nodes {
			nodes = append(nodes, node)
		}
		if len(nodes) == 0 {
			t.Fatal("expected at least one node in cluster")
		}
	}

	// Perform a nop cluster reroute
	res, err := client.ClusterReroute().
		DryRun(true).
		Add(
			NewMoveAllocationCommand(testIndexName, 0, nodes[0], nodes[0]),
			NewCancelAllocationCommand(testIndexName, 0, nodes[0], true),
		).
		Pretty(true).
		Do(context.Background())
	// Expect an error here: We just test if it's of a specific kind
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if res != nil {
		t.Fatalf("expected res to be != nil; got: %v", res)
	}
	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected an error of type *opensearch.Error, got %T", err)
	}
	if want, have := 400, e.Status; want != have {
		t.Fatalf("expected Status=%d, have %d", want, have)
	}
}

func testClusterRerouteWithoutBody(client *Client, t *testing.T) {
	// Perform a nop cluster reroute
	res, err := client.ClusterReroute().
		DryRun(true).
		Pretty(true).
		RetryFailed(true).
		MasterTimeout("10s").
		Do(context.Background())
	// Expect an error here: We just test if it's of a specific kind
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if res != nil {
		t.Fatalf("expected res to be != nil; got: %v", res)
	}
	if !strings.Contains(err.Error(), "missing allocate commands or raw body") {
		t.Fatalf("expected Error~=%s, have %s", "missing allocate commands or raw body", err.Error())
	}
}
