package types_test

import (
	"fmt"
	json "github.com/goccy/go-json"
	"time"

	"github.com/disaster37/opensearch/v3/types"
)

func ExampleOpenSearchError() {
	err := &types.OpenSearchError{
		Status: 404,
		Details: &types.OpenSearchErrorDetails{
			Type:   "index_not_found_exception",
			Reason: "no such index [my-index]",
		},
	}
	fmt.Println(err.Error())

	statusOnly := &types.OpenSearchError{Status: 500}
	fmt.Println(statusOnly.Error())

	// Output:
	// opensearch: Error 404: no such index [my-index] [type=index_not_found_exception]
	// opensearch: Error 500
}

func ExampleIsNotFound() {
	err := &types.OpenSearchError{Status: 404}
	fmt.Println(types.IsNotFound(err))
	fmt.Println(types.IsNotFound(&types.OpenSearchError{Status: 500}))

	// Output:
	// true
	// false
}

func ExampleIsConflict() {
	err := &types.OpenSearchError{Status: 409}
	fmt.Println(types.IsConflict(err))
	fmt.Println(types.IsConflict(&types.OpenSearchError{Status: 404}))

	// Output:
	// true
	// false
}

func ExampleAcknowledgedResponse() {
	resp := types.AcknowledgedResponse{
		Acknowledged:       true,
		ShardsAcknowledged: true,
		Index:              "my-index",
	}
	b, _ := json.Marshal(resp)
	fmt.Println(string(b))
	fmt.Printf("acknowledged=%v shards_acknowledged=%v index=%s\n", resp.Acknowledged, resp.ShardsAcknowledged, resp.Index)

	// Output:
	// {"acknowledged":true,"shards_acknowledged":true,"index":"my-index"}
	// acknowledged=true shards_acknowledged=true index=my-index
}

func ExampleShardsInfo() {
	info := types.ShardsInfo{
		Total:      5,
		Successful: 4,
		Failed:     1,
	}
	b, _ := json.Marshal(info)
	fmt.Println(string(b))
	fmt.Printf("total=%d successful=%d failed=%d\n", info.Total, info.Successful, info.Failed)

	// Output:
	// {"total":5,"successful":4,"failed":1}
	// total=5 successful=4 failed=1
}

func ExampleBroadcastResponse() {
	resp := types.BroadcastResponse{
		Shards: &types.ShardsInfo{
			Total:      10,
			Successful: 9,
			Failed:     1,
		},
		Total:      10,
		Successful: 9,
		Failed:     1,
	}
	b, _ := json.Marshal(resp)
	fmt.Println(string(b))
	fmt.Printf("total=%d successful=%d failed=%d\n", resp.Total, resp.Successful, resp.Failed)

	// Output:
	// {"_shards":{"total":10,"successful":9,"failed":1},"total":10,"successful":9,"failed":1}
	// total=10 successful=9 failed=1
}

func ExampleDocumentVersion() {
	seqNo := int64(42)
	primaryTerm := int64(1)
	ver := types.DocumentVersion{
		SeqNo:       &seqNo,
		PrimaryTerm: &primaryTerm,
	}
	fmt.Printf("seq_no=%d primary_term=%d\n", *ver.SeqNo, *ver.PrimaryTerm)

	// Output:
	// seq_no=42 primary_term=1
}

func ExampleUnixMilliTime() {
	ts := time.Date(2024, time.January, 15, 12, 0, 0, 0, time.UTC)
	umt := types.UnixMilliTime{Time: ts}

	b, _ := json.Marshal(umt)
	fmt.Println(string(b))

	var decoded types.UnixMilliTime
	_ = json.Unmarshal(b, &decoded)
	fmt.Println(decoded.UTC().Format(time.RFC3339))

	// Output:
	// 1705320000000
	// 2024-01-15T12:00:00Z
}

func ExampleListResponse() {
	resp := types.ListResponse[string]{
		Items: []string{"policy-1", "policy-2", "policy-3"},
		Total: 3,
	}
	b, _ := json.Marshal(resp)
	fmt.Println(string(b))
	fmt.Printf("total=%d items=%v\n", resp.Total, resp.Items)

	// Output:
	// {"items":["policy-1","policy-2","policy-3"],"total":3}
	// total=3 items=[policy-1 policy-2 policy-3]
}
