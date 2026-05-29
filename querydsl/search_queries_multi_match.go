package querydsl

import "fmt"

type MultiMatchQuery struct {
	Query              any
	Fields             []string
	FieldBoosts        map[string]*float64
	Type               string
	Operator           string
	Analyzer           string
	Boost              *float64
	Slop               *int
	Fuzziness          string
	PrefixLength       *int
	MaxExpansions      *int
	MinimumShouldMatch string
	Rewrite            string
	FuzzyRewrite       string
	TieBreaker         *float64
	Lenient            *bool
	CutoffFrequency    *float64
	ZeroTermsQuery     string
	QueryName          string
}

func NewMultiMatchQuery(text any, fields ...string) MultiMatchQuery {
	return MultiMatchQuery{Query: text, Fields: fields, FieldBoosts: map[string]*float64{}}
}

func (q MultiMatchQuery) Source() (any, error) {
	fields := make([]string, 0, len(q.Fields))
	for _, f := range q.Fields {
		if b, ok := q.FieldBoosts[f]; ok && b != nil {
			fields = append(fields, fmt.Sprintf("%s^%f", f, *b))
		} else {
			fields = append(fields, f)
		}
	}
	body := map[string]any{"query": q.Query, "fields": fields}
	if q.Type != "" {
		body["type"] = q.Type
	}
	if q.Operator != "" {
		body["operator"] = q.Operator
	}
	if q.Analyzer != "" {
		body["analyzer"] = q.Analyzer
	}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.Slop != nil {
		body["slop"] = *q.Slop
	}
	if q.Fuzziness != "" {
		body["fuzziness"] = q.Fuzziness
	}
	if q.PrefixLength != nil {
		body["prefix_length"] = *q.PrefixLength
	}
	if q.MaxExpansions != nil {
		body["max_expansions"] = *q.MaxExpansions
	}
	if q.MinimumShouldMatch != "" {
		body["minimum_should_match"] = q.MinimumShouldMatch
	}
	if q.Rewrite != "" {
		body["rewrite"] = q.Rewrite
	}
	if q.FuzzyRewrite != "" {
		body["fuzzy_rewrite"] = q.FuzzyRewrite
	}
	if q.TieBreaker != nil {
		body["tie_breaker"] = *q.TieBreaker
	}
	if q.Lenient != nil {
		body["lenient"] = *q.Lenient
	}
	if q.CutoffFrequency != nil {
		body["cutoff_frequency"] = *q.CutoffFrequency
	}
	if q.ZeroTermsQuery != "" {
		body["zero_terms_query"] = q.ZeroTermsQuery
	}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	return map[string]any{"multi_match": body}, nil
}
