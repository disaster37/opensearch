package querydsl

type RareTermsAggregation struct {
	Field          string
	IncludeExclude *TermsAggregationIncludeExclude
	MaxDocCount    *int
	Precision      *float64
	Missing        any
	SubAggs        map[string]Aggregation
	Meta           map[string]any
}

func NewRareTermsAggregation() *RareTermsAggregation { return &RareTermsAggregation{} }

// WithField sets the field to aggregate on.
func (a *RareTermsAggregation) WithField(field string) *RareTermsAggregation {
	a.Field = field
	return a
}

// WithMaxDocCount sets the maximum document count for a term to be considered "rare".
func (a *RareTermsAggregation) WithMaxDocCount(maxDocCount int) *RareTermsAggregation {
	a.MaxDocCount = &maxDocCount
	return a
}

// WithPrecision sets the precision of the internal CuckooFilters.
func (a *RareTermsAggregation) WithPrecision(precision float64) *RareTermsAggregation {
	a.Precision = &precision
	return a
}

// WithMissing sets the value used for documents without the field.
func (a *RareTermsAggregation) WithMissing(missing any) *RareTermsAggregation {
	a.Missing = missing
	return a
}

// WithIncludeExclude sets the include/exclude filter for term values.
func (a *RareTermsAggregation) WithIncludeExclude(ie *TermsAggregationIncludeExclude) *RareTermsAggregation {
	a.IncludeExclude = ie
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *RareTermsAggregation) WithSubAggregation(name string, sub Aggregation) *RareTermsAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *RareTermsAggregation) WithMeta(meta map[string]any) *RareTermsAggregation {
	a.Meta = meta
	return a
}

func (a RareTermsAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.MaxDocCount != nil {
		body["max_doc_count"] = *a.MaxDocCount
	}
	if a.Precision != nil {
		body["precision"] = *a.Precision
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.IncludeExclude != nil {
		ie, _ := a.IncludeExclude.Source()
		if m, ok := ie.(map[string]any); ok {
			for k, v := range m {
				body[k] = v
			}
		}
	}
	return sourceAgg("rare_terms", body, a.SubAggs, a.Meta, nil)
}
