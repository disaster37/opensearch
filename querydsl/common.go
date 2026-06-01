package querydsl

import (
	"strconv"

	json "github.com/goccy/go-json"
)

func itoa(i int) string { return strconv.Itoa(i) }

func pipelineBucketsPath(paths []string) any {
	switch len(paths) {
	case 0:
		return nil
	case 1:
		return paths[0]
	default:
		return paths
	}
}

// sourcePipeline is a helper for pipeline aggregation Source() methods.
// It assembles the body and wraps it under the given agg type key.
func sourcePipeline(aggType string, body map[string]any, meta map[string]any, script *Script) (any, error) {
	if script != nil {
		body["script"], _ = script.Source()
	}
	if len(meta) > 0 {
		body["meta"] = meta
	}
	return map[string]any{aggType: body}, nil
}

// sourceAgg serializes the body of an aggregation along with optional
// sub-aggregations and meta, returning the wrapped { "agg_type": { ... } } map.
func sourceAgg(aggType string, body map[string]any, subs map[string]Aggregation, meta map[string]any, script *Script) (any, error) {
	if script != nil {
		body["script"], _ = script.Source()
	}
	if len(subs) > 0 {
		aggsMap := make(map[string]any, len(subs))
		for name, aggregate := range subs {
			src, err := aggregate.Source()
			if err != nil {
				return nil, err
			}
			aggsMap[name] = src
		}
		body["aggregations"] = aggsMap
	}
	if len(meta) > 0 {
		body["meta"] = meta
	}
	return map[string]any{aggType: body}, nil
}

var nilByte = []byte("null")

// marshalStruct serializes a struct using json tags, returning a map
// with zero/empty values omitted. This drives the Source() method of
// query/aggregation builders.
func marshalStruct(v any) (any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
