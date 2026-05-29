package querydsl

// MissingAggregation produces a single bucket of documents that have no value
// for the specified field (i.e. the field is null or not present). Sub-aggregations
// are computed only on the documents within this bucket.
//
// Typical use: count documents where "email" is not set.
//
// JSON output shape:
//
//	{"missing": {"field": "email"}}
type MissingAggregation struct {
	Field   string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewMissingAggregation returns a zero-value MissingAggregation.
func NewMissingAggregation() MissingAggregation { return MissingAggregation{} }

func (a MissingAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	return sourceAgg("missing", body, a.SubAggs, a.Meta, nil)
}
