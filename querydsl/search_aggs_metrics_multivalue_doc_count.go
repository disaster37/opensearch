package querydsl

// MultiValueDocCountAggregation counts documents that hold two or more values
// for the aggregated field (added in OpenSearch 3.8.0, PR #20472).
//
// The server parser declares (script=true, missing=true, format=false), so
// unlike [ValueCountAggregation] this aggregation has no Format field.
//
// JSON output shape:
//
//	{"multivalue_doc_count": {"field": "field_name"}}
//
// The result is serialized as an InternalValueCount, i.e. {"value": N},
// identical to value_count — use [Aggregations.MultiValueDocCount] to read it.
type MultiValueDocCountAggregation struct {
	Field   string
	Script  *Script
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewMultiValueDocCountAggregation returns a new MultiValueDocCountAggregation
// with default settings.
func NewMultiValueDocCountAggregation() *MultiValueDocCountAggregation {
	return &MultiValueDocCountAggregation{}
}

// WithField sets the field to count multi-value documents on.
func (a *MultiValueDocCountAggregation) WithField(field string) *MultiValueDocCountAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *MultiValueDocCountAggregation) WithScript(script *Script) *MultiValueDocCountAggregation {
	a.Script = script
	return a
}

// WithMissing sets the value to use when the field is missing.
func (a *MultiValueDocCountAggregation) WithMissing(missing any) *MultiValueDocCountAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *MultiValueDocCountAggregation) WithSubAggs(subAggs map[string]Aggregation) *MultiValueDocCountAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MultiValueDocCountAggregation) WithMeta(meta map[string]any) *MultiValueDocCountAggregation {
	a.Meta = meta
	return a
}

func (a MultiValueDocCountAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	return sourceAgg("multivalue_doc_count", body, a.SubAggs, a.Meta, a.Script)
}
