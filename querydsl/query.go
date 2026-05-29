package querydsl

// Query is the common interface implemented by all OpenSearch query builders.
// Its sole purpose is to produce a JSON-serializable representation of the
// query, typically a map[string]any, that can be embedded in the
// request body sent to OpenSearch.
//
// All concrete query types (BoolQuery, MatchQuery, TermQuery, RangeQuery,
// etc.) implement this interface via a Source method that returns the
// corresponding JSON DSL fragment:
//
//	{"bool": {"must": [...]}}            // BoolQuery
//	{"match": {"field": "query text"}}   // MatchQuery
//	{"term": {"field": "value"}}         // TermQuery
//
// Consumers of the query DSL build a tree of Query values and pass them
// to a search request, which calls Source recursively to assemble the
// complete request body.
type Query interface {
	// Source returns the JSON-serializable query request body fragment.
	// The returned value is typically map[string]any and is ready
	// for JSON marshaling.
	Source() (any, error)
}
