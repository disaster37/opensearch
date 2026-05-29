package querydsl

import (
	"fmt"
	"strings"
)

// SimpleQueryStringQuery is a query parser that supports a simplified subset of the
// [QueryStringQuery] syntax. It is more robust to errors and never throws exceptions,
// making it suitable for user input that cannot be validated.
//
// Typical use: public-facing search boxes where users may enter malformed queries.
//
// JSON DSL output:
//
//	{
//	  "simple_query_string": {
//	    "query": "\"fried eggs\" +(eggplant | potato) -frittata",
//	    "fields": ["title^5", "body"]
//	  }
//	}
type SimpleQueryStringQuery struct {
	Query                           string
	Analyzer                        string
	QuoteFieldSuffix                string
	DefaultOperator                 string
	Fields                          []string
	FieldBoosts                     map[string]*float64
	MinimumShouldMatch              string
	Flags                           string
	Boost                           *float64
	LowercaseExpandedTerms          *bool
	Lenient                         *bool
	AnalyzeWildcard                 *bool
	Locale                          string
	QueryName                       string
	AutoGenerateSynonymsPhraseQuery *bool
	FuzzyPrefixLength               int
	FuzzyMaxExpansions              int
	FuzzyTranspositions             *bool
}

// NewSimpleQueryStringQuery creates a new SimpleQueryStringQuery with the given text.
func NewSimpleQueryStringQuery(text string) SimpleQueryStringQuery {
	return SimpleQueryStringQuery{
		Query:              text,
		FieldBoosts:        map[string]*float64{},
		FuzzyPrefixLength:  -1,
		FuzzyMaxExpansions: -1,
	}
}

func (q SimpleQueryStringQuery) fields() []string {
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

func (q SimpleQueryStringQuery) Source() (any, error) {
	m := map[string]any{"query": q.Query}
	if f := q.fields(); f != nil {
		m["fields"] = f
	}
	if q.Flags != "" {
		m["flags"] = q.Flags
	}
	if q.Analyzer != "" {
		m["analyzer"] = q.Analyzer
	}
	if q.DefaultOperator != "" {
		m["default_operator"] = strings.ToLower(q.DefaultOperator)
	}
	if q.LowercaseExpandedTerms != nil {
		m["lowercase_expanded_terms"] = *q.LowercaseExpandedTerms
	}
	if q.Lenient != nil {
		m["lenient"] = *q.Lenient
	}
	if q.AnalyzeWildcard != nil {
		m["analyze_wildcard"] = *q.AnalyzeWildcard
	}
	if q.Locale != "" {
		m["locale"] = q.Locale
	}
	if q.QueryName != "" {
		m["_name"] = q.QueryName
	}
	if q.MinimumShouldMatch != "" {
		m["minimum_should_match"] = q.MinimumShouldMatch
	}
	if q.QuoteFieldSuffix != "" {
		m["quote_field_suffix"] = q.QuoteFieldSuffix
	}
	if q.Boost != nil {
		m["boost"] = *q.Boost
	}
	if q.AutoGenerateSynonymsPhraseQuery != nil {
		m["auto_generate_synonyms_phrase_query"] = *q.AutoGenerateSynonymsPhraseQuery
	}
	if q.FuzzyPrefixLength != -1 {
		m["fuzzy_prefix_length"] = q.FuzzyPrefixLength
	}
	if q.FuzzyMaxExpansions != -1 {
		m["fuzzy_max_expansions"] = q.FuzzyMaxExpansions
	}
	if q.FuzzyTranspositions != nil {
		m["fuzzy_transpositions"] = *q.FuzzyTranspositions
	}
	return map[string]any{"simple_query_string": m}, nil
}
