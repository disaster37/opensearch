package querydsl

type TermsSetQuery struct {
	Field                    string
	Values                   []any
	MinimumShouldMatchField  string
	MinimumShouldMatchScript *Script
	Boost                    *float64
	QueryName                string
}

func NewTermsSetQuery(field string, values ...any) TermsSetQuery {
	return TermsSetQuery{Field: field, Values: values}
}

func (q TermsSetQuery) Source() (any, error) {
	params := map[string]any{"terms": q.Values}
	if q.MinimumShouldMatchField != "" {
		params["minimum_should_match_field"] = q.MinimumShouldMatchField
	}
	if q.MinimumShouldMatchScript != nil {
		params["minimum_should_match_script"], _ = q.MinimumShouldMatchScript.Source()
	}
	if q.Boost != nil {
		params["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"terms_set": map[string]any{q.Field: params}}, nil
}
