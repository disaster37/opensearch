// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"testing"
)

func TestIndexStatsBuildURL(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	tests := []struct {
		Indices  []string
		Metrics  []string
		Expected string
	}{
		{
			[]string{},
			[]string{},
			"/_stats",
		},
		{
			[]string{"index1"},
			[]string{},
			"/index1/_stats",
		},
		{
			[]string{},
			[]string{"metric1"},
			"/_stats/metric1",
		},
		{
			[]string{"index1"},
			[]string{"metric1"},
			"/index1/_stats/metric1",
		},
		{
			[]string{"index1", "index2"},
			[]string{"metric1"},
			"/index1%2Cindex2/_stats/metric1",
		},
		{
			[]string{"index1", "index2"},
			[]string{"metric1", "metric2"},
			"/index1%2Cindex2/_stats/metric1%2Cmetric2",
		},
	}

	for i, test := range tests {
		path, _, err := client.IndexStats().Index(test.Indices...).Metric(test.Metrics...).buildURL()
		if err != nil {
			t.Fatalf("case #%d: %v", i+1, err)
		}
		if path != test.Expected {
			t.Errorf("case #%d: expected %q; got: %q", i+1, test.Expected, path)
		}
	}
}

func TestIndexStats(t *testing.T) {
	client := setupTestClientAndCreateIndexAndAddDocs(t) //, SetTraceLog(log.New(os.Stdout, "", 0)))

	stats, err := client.IndexStats(testIndexName).Human(true).Pretty(true).Do(context.TODO())
	if err != nil {
		t.Fatalf("expected no error; got: %v", err)
	}
	if stats == nil {
		t.Fatalf("expected response; got: %v", stats)
	}
	stat, found := stats.Indices[testIndexName]
	if !found {
		t.Fatalf("expected stats about index %q; got: %v", testIndexName, found)
	}
	if stat.Total == nil {
		t.Fatalf("expected total to be != nil; got: %v", stat.Total)
	}
	if stat.Total.Docs == nil {
		t.Fatalf("expected total docs to be != nil; got: %v", stat.Total.Docs)
	}
	if stat.Total.Docs.Count == 0 {
		t.Fatalf("expected total docs count to be > 0; got: %d", stat.Total.Docs.Count)
	}
}

func TestIndexStatsWithShards(t *testing.T) {
	client := setupTestClientAndCreateIndexAndAddDocs(t) //, SetTraceLog(log.New(os.Stdout, "", 0)))

	stats, err := client.IndexStats(testIndexName).Level("shards").Human(true).Pretty(true).Do(context.TODO())
	if err != nil {
		t.Fatalf("expected no error; got: %v", err)
	}
	if stats == nil {
		t.Fatalf("expected response; got: %v", stats)
	}
	stat, found := stats.Indices[testIndexName]
	if !found {
		t.Fatalf("expected stats about index %q; got: %v", testIndexName, found)
	}
	if stat.Total == nil {
		t.Fatalf("expected total to be != nil; got: %v", stat.Total)
	}
	if stat.Total.Docs == nil {
		t.Fatalf("expected total docs to be != nil; got: %v", stat.Total.Docs)
	}
	if stat.Total.Docs.Count == 0 {
		t.Fatalf("expected total docs count to be > 0; got: %d", stat.Total.Docs.Count)
	}
	if stat.Shards == nil {
		t.Fatalf("expected shard level information to be != nil; got: %v", stat.Shards)
	}
	shard, found := stat.Shards["0"]
	if !found || shard == nil {
		t.Fatalf("expected shard level information for shard 0; got: %v (found=%v)", shard, found)
	}
	if len(shard) != 1 {
		t.Fatalf("expected shard level information array to be == 1; got: %v", len(shard))
	}
	if shard[0].Docs == nil {
		t.Fatalf("expected docs to be != nil; got: %v", shard[0].Docs)
	}
}
