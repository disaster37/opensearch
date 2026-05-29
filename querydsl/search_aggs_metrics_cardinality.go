package querydsl

// CardinalityAggregation computes an approximate count of distinct values
// using the HyperLogLog++ algorithm. The PrecisionThreshold option allows
// trading memory for accuracy; higher values yield more precise results.
//
// JSON output shape:
//
//	{"cardinality": {"field": "author"}}
type CardinalityAggregation struct {
	Field              string
	Script             *Script
	Format             string
	Missing            any
	PrecisionThreshold *int64
	Rehash             *bool
	SubAggs            map[string]Aggregation
	Meta               map[string]any
}

// NewCardinalityAggregation returns a new CardinalityAggregation with default settings.
func NewCardinalityAggregation() CardinalityAggregation { return CardinalityAggregation{} }

func (a CardinalityAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.PrecisionThreshold != nil {
		body["precision_threshold"] = *a.PrecisionThreshold
	}
	if a.Rehash != nil {
		body["rehash"] = *a.Rehash
	}
	return sourceAgg("cardinality", body, a.SubAggs, a.Meta, a.Script)
}
