// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"net/url"
	"testing"
)

func TestClusterState(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Get cluster state
	res, err := client.ClusterState().Index("_all").Metric("_all").Pretty(true).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatalf("expected res to be != nil; got: %v", res)
	}
	if res.ClusterName == "" {
		t.Fatalf("expected a cluster name; got: %q", res.ClusterName)
	}
}

func TestClusterStateURLs(t *testing.T) {
	tests := []struct {
		Service        *ClusterStateService
		ExpectedPath   string
		ExpectedParams url.Values
	}{
		{
			Service: &ClusterStateService{
				indices: []string{},
				metrics: []string{},
			},
			ExpectedPath: "/_cluster/state/_all/_all",
		},
		{
			Service: &ClusterStateService{
				indices: []string{"twitter"},
				metrics: []string{},
			},
			ExpectedPath: "/_cluster/state/_all/twitter",
		},
		{
			Service: &ClusterStateService{
				indices: []string{"twitter", "gplus"},
				metrics: []string{},
			},
			ExpectedPath: "/_cluster/state/_all/twitter%2Cgplus",
		},
		{
			Service: &ClusterStateService{
				indices: []string{},
				metrics: []string{"nodes"},
			},
			ExpectedPath: "/_cluster/state/nodes/_all",
		},
		{
			Service: &ClusterStateService{
				indices: []string{"twitter"},
				metrics: []string{"nodes"},
			},
			ExpectedPath: "/_cluster/state/nodes/twitter",
		},
		{
			Service: &ClusterStateService{
				indices:       []string{"twitter"},
				metrics:       []string{"nodes"},
				masterTimeout: "1s",
			},
			ExpectedPath:   "/_cluster/state/nodes/twitter",
			ExpectedParams: url.Values{"master_timeout": []string{"1s"}},
		},
	}

	for _, test := range tests {
		gotPath, gotParams, err := test.Service.buildURL()
		if err != nil {
			t.Fatalf("expected no error; got: %v", err)
		}
		if gotPath != test.ExpectedPath {
			t.Errorf("expected URL path = %q; got: %q", test.ExpectedPath, gotPath)
		}
		if gotParams.Encode() != test.ExpectedParams.Encode() {
			t.Errorf("expected URL params = %v; got: %v", test.ExpectedParams, gotParams)
		}
	}
}

func TestClusterStateGet(t *testing.T) {
	client := setupTestClientAndCreateIndex(t) //, SetTraceLog(log.New(os.Stdout, "", 0)))

	state, err := client.ClusterState().Pretty(true).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if want, have := "test", state.ClusterName; want != have {
		t.Fatalf("ClusterName: want %q, have %q", want, have)
	}
}
