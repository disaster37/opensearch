package querydsl

type TermsSetQuery struct {
	Field                    string
	Values                   []any
	MinimumShouldMatchField  string
	MinimumShouldMatchScript *Script
	Boost                    *float64
	QueryName                string
}

func NewTermsSetQuery(field string, values ...any) *TermsSetQuery {
	return &TermsSetQuery{Field: field, Values: values}
}

// WithMinimumShouldMatchField sets the field whose value determines the minimum number of terms that must match.
func (q *TermsSetQuery) WithMinimumShouldMatchField(field string) *TermsSetQuery {
	q.MinimumShouldMatchField = field
	return q
}

// WithMinimumShouldMatchScript sets the script that computes the minimum number of terms that must match.
func (q *TermsSetQuery) WithMinimumShouldMatchScript(script *Script) *TermsSetQuery {
	q.MinimumShouldMatchScript = script
	return q
}

// WithBoost sets the boost factor for the query.
func (q *TermsSetQuery) WithBoost(boost float64) *TermsSetQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the optional query name for identification in responses.
func (q *TermsSetQuery) WithQueryName(queryName string) *TermsSetQuery {
	q.QueryName = queryName
	return q
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
