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
func NewCardinalityAggregation() *CardinalityAggregation { return &CardinalityAggregation{} }

// WithField sets the field to compute the cardinality on.
func (a *CardinalityAggregation) WithField(field string) *CardinalityAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *CardinalityAggregation) WithScript(script *Script) *CardinalityAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *CardinalityAggregation) WithFormat(format string) *CardinalityAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *CardinalityAggregation) WithMissing(missing any) *CardinalityAggregation {
	a.Missing = missing
	return a
}

// WithPrecisionThreshold sets the precision threshold for the HyperLogLog++ algorithm.
func (a *CardinalityAggregation) WithPrecisionThreshold(v int64) *CardinalityAggregation {
	a.PrecisionThreshold = &v
	return a
}

// WithRehash sets whether to rehash values before counting.
func (a *CardinalityAggregation) WithRehash(v bool) *CardinalityAggregation {
	a.Rehash = &v
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *CardinalityAggregation) WithSubAggs(subAggs map[string]Aggregation) *CardinalityAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *CardinalityAggregation) WithMeta(meta map[string]any) *CardinalityAggregation {
	a.Meta = meta
	return a
}

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
