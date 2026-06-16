// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"testing"
)

func TestIndicesOpenValidate(t *testing.T) {
	client := setupTestClient(t)

	// No index name -> fail with error
	res, err := NewIndicesOpenService(client).Do(context.TODO())
	if err == nil {
		t.Fatalf("expected IndicesOpen to fail without index name")
	}
	if res != nil {
		t.Fatalf("expected result to be == nil; got: %v", res)
	}
}

func TestIndicesOpenBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		index                 string
		waitForActiveShards   string
		clusterManagerTimeout string
		expectedParams        map[string]string
	}{
		{
			index:                 "myindex",
			waitForActiveShards:   "all",
			clusterManagerTimeout: "30s",
			expectedParams: map[string]string{
				"wait_for_active_shards":  "all",
				"cluster_manager_timeout": "30s",
			},
		},
		{
			index:          "myindex",
			expectedParams: map[string]string{},
		},
	}

	for _, tt := range tests {
		svc := NewIndicesOpenService(client).Index(tt.index)
		if tt.waitForActiveShards != "" {
			svc = svc.WaitForActiveShards(tt.waitForActiveShards)
		}
		if tt.clusterManagerTimeout != "" {
			svc = svc.ClusterManagerTimeout(tt.clusterManagerTimeout)
		}

		_, params, err := svc.buildURL()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for k, v := range tt.expectedParams {
			if got := params.Get(k); got != v {
				t.Errorf("param %q: want %q, got %q", k, v, got)
			}
		}
	}
}
