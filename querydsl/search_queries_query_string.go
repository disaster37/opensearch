package querydsl

import "fmt"

// QueryStringQuery provides a query parser that supports a rich syntax including AND/OR/NOT
// operators, wildcards, fuzzy matching, proximity searches, field boosting, and range queries.
// It parses the query string and generates the appropriate Lucene query.
//
// Typical use: advanced user-facing search boxes that need to support complex query syntax.
//
// JSON DSL output:
//
//	{
//	  "query_string": {
//	    "query": "(new york city) OR (big apple)",
//	    "default_field": "content"
//	  }
//	}
type QueryStringQuery struct {
	Query                    string
	DefaultField             string
	DefaultOperator          string
	Analyzer                 string
	QuoteAnalyzer            string
	QuoteFieldSuffix         string
	AllowLeadingWildcard     *bool
	LowercaseExpandedTerms   *bool
	EnablePositionIncrements *bool
	AnalyzeWildcard          *bool
	Locale                   string
	Boost                    *float64
	Fuzziness                string
	FuzzyPrefixLength        *int
	FuzzyMaxExpansions       *int
	FuzzyRewrite             string
	PhraseSlop               *int
	Fields                   []string
	FieldBoosts              map[string]*float64
	TieBreaker               *float64
	Rewrite                  string
	MinimumShouldMatch       string
	Lenient                  *bool
	QueryName                string
	TimeZone                 string
	MaxDeterminizedStates    *int
	Escape                   *bool
	Type                     string
}

// NewQueryStringQuery creates a new QueryStringQuery with the given query string.
func NewQueryStringQuery(queryString string) QueryStringQuery {
	return QueryStringQuery{Query: queryString, FieldBoosts: map[string]*float64{}}
}

func (q QueryStringQuery) fields() []string {
	if len(q.Fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(q.Fields))
	for _, f := range q.Fields {
		if b, ok := q.FieldBoosts[f]; ok && b != nil {
			out = append(out, fmt.Sprintf("%s^%f", f, *b))
		} else {
			out = append(out, f)
		}
	}
	return out
}

func (q QueryStringQuery) Source() (any, error) {
	m := map[string]any{"query": q.Query}
	if q.DefaultField != "" {
		m["default_field"] = q.DefaultField
	}
	if f := q.fields(); f != nil {
		m["fields"] = f
	}
	if q.TieBreaker != nil {
		m["tie_breaker"] = *q.TieBreaker
	}
	if q.DefaultOperator != "" {
		m["default_operator"] = q.DefaultOperator
	}
	if q.Analyzer != "" {
		m["analyzer"] = q.Analyzer
	}
	if q.QuoteAnalyzer != "" {
		m["quote_analyzer"] = q.QuoteAnalyzer
	}
	if q.MaxDeterminizedStates != nil {
		m["max_determinized_states"] = *q.MaxDeterminizedStates
	}
	if q.AllowLeadingWildcard != nil {
		m["allow_leading_wildcard"] = *q.AllowLeadingWildcard
	}
	if q.LowercaseExpandedTerms != nil {
		m["lowercase_expanded_terms"] = *q.LowercaseExpandedTerms
	}
	if q.EnablePositionIncrements != nil {
		m["enable_position_increments"] = *q.EnablePositionIncrements
	}
	if q.Fuzziness != "" {
		m["fuzziness"] = q.Fuzziness
	}
	if q.Boost != nil {
		m["boost"] = *q.Boost
	}
	if q.FuzzyPrefixLength != nil {
		m["fuzzy_prefix_length"] = *q.FuzzyPrefixLength
	}
	if q.FuzzyMaxExpansions != nil {
		m["fuzzy_max_expansions"] = *q.FuzzyMaxExpansions
	}
	if q.FuzzyRewrite != "" {
		m["fuzzy_rewrite"] = q.FuzzyRewrite
	}
	if q.PhraseSlop != nil {
		m["phrase_slop"] = *q.PhraseSlop
	}
	if q.AnalyzeWildcard != nil {
		m["analyze_wildcard"] = *q.AnalyzeWildcard
	}
	if q.Rewrite != "" {
		m["rewrite"] = q.Rewrite
	}
	if q.MinimumShouldMatch != "" {
		m["minimum_should_match"] = q.MinimumShouldMatch
	}
	if q.QuoteFieldSuffix != "" {
		m["quote_field_suffix"] = q.QuoteFieldSuffix
	}
	if q.Lenient != nil {
		m["lenient"] = *q.Lenient
	}
	if q.QueryName != "" {
		m["_name"] = q.QueryName
	}
	if q.Locale != "" {
		m["locale"] = q.Locale
	}
	if q.TimeZone != "" {
		m["time_zone"] = q.TimeZone
	}
	if q.Escape != nil {
		m["escape"] = *q.Escape
	}
	if q.Type != "" {
		m["type"] = q.Type
	}
	return map[string]any{"query_string": m}, nil
}
