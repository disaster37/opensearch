package querydsl

import "fmt"

// CombinedFieldsQuery searches across multiple text fields by treating them as if they
// were indexed into one combined field. It scores using the total term frequency across
// all fields, providing better ranking than [MultiMatchQuery] for multi-field searches.
//
// Typical use: searching for "john smith" across "first_name" and "last_name" fields.
//
// JSON DSL output:
//
//	{
//	  "combined_fields": {
//	    "query": "search text",
//	    "fields": ["field1", "field2^2.0"]
//	  }
//	}
type CombinedFieldsQuery struct {
	Query                           any
	Fields                          []string
	FieldBoosts                     map[string]*float64
	AutoGenerateSynonymsPhraseQuery *bool
	Operator                        string
	MinimumShouldMatch              string
	ZeroTermsQuery                  string
}

// NewCombinedFieldsQuery creates a new CombinedFieldsQuery for the given text and fields.
func NewCombinedFieldsQuery(text any, fields ...string) *CombinedFieldsQuery {
	return &CombinedFieldsQuery{Query: text, Fields: fields, FieldBoosts: map[string]*float64{}}
}

// WithAutoGenerateSynonymsPhraseQuery sets whether synonyms are treated as phrase queries.
func (q *CombinedFieldsQuery) WithAutoGenerateSynonymsPhraseQuery(v bool) *CombinedFieldsQuery {
	q.AutoGenerateSynonymsPhraseQuery = &v
	return q
}

// WithOperator sets the boolean operator used between terms (AND or OR).
func (q *CombinedFieldsQuery) WithOperator(operator string) *CombinedFieldsQuery {
	q.Operator = operator
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *CombinedFieldsQuery) WithMinimumShouldMatch(v string) *CombinedFieldsQuery {
	q.MinimumShouldMatch = v
	return q
}

// WithZeroTermsQuery sets the behaviour when the analyzer removes all tokens (none or all).
func (q *CombinedFieldsQuery) WithZeroTermsQuery(v string) *CombinedFieldsQuery {
	q.ZeroTermsQuery = v
	return q
}

func (q CombinedFieldsQuery) Source() (any, error) {
	fields := make([]string, 0, len(q.Fields))
	for _, f := range q.Fields {
		if b, ok := q.FieldBoosts[f]; ok && b != nil {
			fields = append(fields, fmt.Sprintf("%s^%f", f, *b))
		} else {
			fields = append(fields, f)
		}
	}
	body := map[string]any{"query": q.Query, "fields": fields}
	if q.AutoGenerateSynonymsPhraseQuery != nil {
		body["auto_generate_synonyms_phrase_query"] = *q.AutoGenerateSynonymsPhraseQuery
	}
	if q.Operator != "" {
		body["operator"] = q.Operator
	}
	if q.MinimumShouldMatch != "" {
		body["minimum_should_match"] = q.MinimumShouldMatch
	}
	if q.ZeroTermsQuery != "" {
		body["zero_terms_query"] = q.ZeroTermsQuery
	}
	return map[string]any{"combined_fields": body}, nil
}
