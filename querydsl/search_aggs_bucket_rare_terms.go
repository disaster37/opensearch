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

func NewRareTermsAggregation() RareTermsAggregation { return RareTermsAggregation{} }

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
