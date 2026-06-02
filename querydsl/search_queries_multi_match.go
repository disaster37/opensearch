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

func NewMultiMatchQuery(text any, fields ...string) *MultiMatchQuery {
	return &MultiMatchQuery{Query: text, Fields: fields, FieldBoosts: map[string]*float64{}}
}

// WithType sets the multi-match query type (best_fields, most_fields, cross_fields, phrase, phrase_prefix).
func (q *MultiMatchQuery) WithType(t string) *MultiMatchQuery {
	q.Type = t
	return q
}

// WithOperator sets the boolean logic used when analyzing text (AND or OR).
func (q *MultiMatchQuery) WithOperator(operator string) *MultiMatchQuery {
	q.Operator = operator
	return q
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *MultiMatchQuery) WithAnalyzer(analyzer string) *MultiMatchQuery {
	q.Analyzer = analyzer
	return q
}

// WithBoost sets the boost factor for this query.
func (q *MultiMatchQuery) WithBoost(boost float64) *MultiMatchQuery {
	q.Boost = &boost
	return q
}

// WithSlop sets the maximum number of positions allowed between matching tokens for phrase queries.
func (q *MultiMatchQuery) WithSlop(slop int) *MultiMatchQuery {
	q.Slop = &slop
	return q
}

// WithFuzziness sets the fuzziness for fuzzy matching (e.g., "AUTO", "1", "2").
func (q *MultiMatchQuery) WithFuzziness(fuzziness string) *MultiMatchQuery {
	q.Fuzziness = fuzziness
	return q
}

// WithPrefixLength sets the number of beginning characters left unchanged for fuzzy matching.
func (q *MultiMatchQuery) WithPrefixLength(prefixLength int) *MultiMatchQuery {
	q.PrefixLength = &prefixLength
	return q
}

// WithMaxExpansions sets the maximum number of terms to which the query expands for fuzzy matching.
func (q *MultiMatchQuery) WithMaxExpansions(maxExpansions int) *MultiMatchQuery {
	q.MaxExpansions = &maxExpansions
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *MultiMatchQuery) WithMinimumShouldMatch(minimumShouldMatch string) *MultiMatchQuery {
	q.MinimumShouldMatch = minimumShouldMatch
	return q
}

// WithRewrite sets the rewrite method used to rewrite the query.
func (q *MultiMatchQuery) WithRewrite(rewrite string) *MultiMatchQuery {
	q.Rewrite = rewrite
	return q
}

// WithFuzzyRewrite sets the rewrite method used when fuzziness is non-zero.
func (q *MultiMatchQuery) WithFuzzyRewrite(fuzzyRewrite string) *MultiMatchQuery {
	q.FuzzyRewrite = fuzzyRewrite
	return q
}

// WithTieBreaker sets the tie-breaker value for best_fields queries.
func (q *MultiMatchQuery) WithTieBreaker(tieBreaker float64) *MultiMatchQuery {
	q.TieBreaker = &tieBreaker
	return q
}

// WithLenient sets whether format-based errors (like feeding a text to a numeric field) are ignored.
func (q *MultiMatchQuery) WithLenient(lenient bool) *MultiMatchQuery {
	q.Lenient = &lenient
	return q
}

// WithCutoffFrequency sets the cutoff frequency for common terms handling.
func (q *MultiMatchQuery) WithCutoffFrequency(cutoffFrequency float64) *MultiMatchQuery {
	q.CutoffFrequency = &cutoffFrequency
	return q
}

// WithZeroTermsQuery sets the behaviour when the analyzer removes all tokens (none or all).
func (q *MultiMatchQuery) WithZeroTermsQuery(zeroTermsQuery string) *MultiMatchQuery {
	q.ZeroTermsQuery = zeroTermsQuery
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *MultiMatchQuery) WithQueryName(name string) *MultiMatchQuery {
	q.QueryName = name
	return q
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
