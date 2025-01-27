// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"sort"
	"testing"
)

func TestFieldCapsURLs(t *testing.T) {
	tests := []struct {
		Service        *FieldCapsService
		ExpectedPath   string
		ExpectedParams url.Values
	}{
		{
			Service:        &FieldCapsService{},
			ExpectedPath:   "/_field_caps",
			ExpectedParams: url.Values{},
		},
		{
			Service: &FieldCapsService{
				index: []string{"index1", "index2"},
			},
			ExpectedPath:   "/index1%2Cindex2/_field_caps",
			ExpectedParams: url.Values{},
		},
		{
			Service: &FieldCapsService{
				index:  []string{"index_*"},
				pretty: boolPtr(true),
			},
			ExpectedPath:   "/index_%2A/_field_caps",
			ExpectedParams: url.Values{"pretty": []string{"true"}},
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

func TestFieldCapsRequestSerialize(t *testing.T) {
	req := &FieldCapsRequest{
		Fields: []string{"creation_date", "answer_count"},
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":["creation_date","answer_count"]}`
	if got != expected {
		t.Fatalf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestFieldCapsRequestDeserialize(t *testing.T) {
	body := `{
		"fields" : ["creation_date", "answer_count"]
	}`

	var request FieldCapsRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		t.Fatalf("unexpected error during unmarshalling: %v", err)
	}

	sort.Sort(lexicographically{request.Fields})

	expectedFields := []string{"answer_count", "creation_date"}
	if !reflect.DeepEqual(request.Fields, expectedFields) {
		t.Fatalf("expected fields to be %v, got %v", expectedFields, request.Fields)
	}
}

func TestFieldCapsResponse(t *testing.T) {
	body := `{
		"_shards": {
				"total": 1,
				"successful": 1,
				"failed": 0
		},
		"fields": {
			"rating": {
				"long": {
					"searchable": true,
					"aggregatable": false,
					"indices": ["index1", "index2"],
					"non_aggregatable_indices": ["index1"]
				},
				"keyword": {
					"searchable": false,
					"aggregatable": true,
					"indices": ["index3", "index4"],
					"non_searchable_indices": ["index4"]
				}
			},
			"title": {
				"text": {
					"searchable": true,
					"aggregatable": false

				}
			}
		}
	}`

	var resp FieldCapsResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Errorf("unexpected error during unmarshalling: %v", err)
	}

	field, ok := resp.Fields["rating"]
	if !ok {
		t.Errorf("expected rating to be in the fields map, didn't find it")
	}
	{
		caps, ok := field["long"]
		if !ok {
			t.Errorf("expected rating.long caps to be found")
		}
		if want, have := true, caps.Searchable; want != have {
			t.Errorf("expected rating.long.searchable to be %v, got %v", want, have)
		}
		if want, have := false, caps.Aggregatable; want != have {
			t.Errorf("expected rating.long.aggregatable to be %v, got %v", want, have)
		}
		if want, have := []string{"index1", "index2"}, caps.Indices; !reflect.DeepEqual(want, have) {
			t.Errorf("expected rating.long.indices to be %v, got %v", want, have)
		}
		if want, have := []string{"index1"}, caps.NonAggregatableIndices; !reflect.DeepEqual(want, have) {
			t.Errorf("expected rating.long.non_aggregatable_indices to be %v, got %v", want, have)
		}
		if want, have := 0, len(caps.NonSearchableIndices); want != have {
			t.Errorf("expected rating.keyword.non_searchable_indices to be %v, got %v", want, have)
		}
	}
	{
		caps, ok := field["keyword"]
		if !ok {
			t.Errorf("expected rating.keyword caps to be found")
		}
		if want, have := false, caps.Searchable; want != have {
			t.Errorf("expected rating.keyword.searchable to be %v, got %v", want, have)
		}
		if want, have := true, caps.Aggregatable; want != have {
			t.Errorf("expected rating.keyword.aggregatable to be %v, got %v", want, have)
		}
		if want, have := []string{"index3", "index4"}, caps.Indices; !reflect.DeepEqual(want, have) {
			t.Errorf("expected rating.keyword.indices to be %v, got %v", want, have)
		}
		if want, have := 0, len(caps.NonAggregatableIndices); want != have {
			t.Errorf("expected rating.keyword.non_aggregatable_indices to be %v, got %v", want, have)
		}
		if want, have := []string{"index4"}, caps.NonSearchableIndices; !reflect.DeepEqual(want, have) {
			t.Errorf("expected rating.keyword.non_searchable_indices to be %v, got %v", want, have)
		}
	}
}

func TestFieldCapsIntegrationTest(t *testing.T) {
	client := setupTestClientAndCreateIndexAndAddDocs(t) //, SetTraceLog(log.New(os.Stdout, "", 0)))

	res, err := client.FieldCaps("_all").Fields("user", "message", "retweets", "created").Pretty(true).Do(context.TODO())
	if err != nil {
		t.Fatalf("expected no error; got: %v", err)
	}
	if res == nil {
		t.Fatalf("expected response; got: %v", res)
	}
}
