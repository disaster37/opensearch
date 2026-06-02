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
func NewSimpleQueryStringQuery(text string) *SimpleQueryStringQuery {
	return &SimpleQueryStringQuery{
		Query:              text,
		FieldBoosts:        map[string]*float64{},
		FuzzyPrefixLength:  -1,
		FuzzyMaxExpansions: -1,
	}
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *SimpleQueryStringQuery) WithAnalyzer(analyzer string) *SimpleQueryStringQuery {
	q.Analyzer = analyzer
	return q
}

// WithQuoteFieldSuffix sets a suffix appended to quoted terms.
func (q *SimpleQueryStringQuery) WithQuoteFieldSuffix(suffix string) *SimpleQueryStringQuery {
	q.QuoteFieldSuffix = suffix
	return q
}

// WithDefaultOperator sets the default boolean operator (AND or OR).
func (q *SimpleQueryStringQuery) WithDefaultOperator(operator string) *SimpleQueryStringQuery {
	q.DefaultOperator = operator
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *SimpleQueryStringQuery) WithMinimumShouldMatch(v string) *SimpleQueryStringQuery {
	q.MinimumShouldMatch = v
	return q
}

// WithFlags sets the flags controlling which features of the simple query string syntax are enabled.
func (q *SimpleQueryStringQuery) WithFlags(flags string) *SimpleQueryStringQuery {
	q.Flags = flags
	return q
}

// WithBoost sets the boost factor for this query.
func (q *SimpleQueryStringQuery) WithBoost(boost float64) *SimpleQueryStringQuery {
	q.Boost = &boost
	return q
}

// WithLowercaseExpandedTerms sets whether expanded terms should be lowercased.
func (q *SimpleQueryStringQuery) WithLowercaseExpandedTerms(v bool) *SimpleQueryStringQuery {
	q.LowercaseExpandedTerms = &v
	return q
}

// WithLenient sets whether format-based errors are ignored.
func (q *SimpleQueryStringQuery) WithLenient(lenient bool) *SimpleQueryStringQuery {
	q.Lenient = &lenient
	return q
}

// WithAnalyzeWildcard sets whether wildcard and prefix queries should be analyzed.
func (q *SimpleQueryStringQuery) WithAnalyzeWildcard(analyzeWildcard bool) *SimpleQueryStringQuery {
	q.AnalyzeWildcard = &analyzeWildcard
	return q
}

// WithLocale sets the locale for the query.
func (q *SimpleQueryStringQuery) WithLocale(locale string) *SimpleQueryStringQuery {
	q.Locale = locale
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *SimpleQueryStringQuery) WithQueryName(name string) *SimpleQueryStringQuery {
	q.QueryName = name
	return q
}

// WithAutoGenerateSynonymsPhraseQuery sets whether synonyms are treated as phrase queries.
func (q *SimpleQueryStringQuery) WithAutoGenerateSynonymsPhraseQuery(v bool) *SimpleQueryStringQuery {
	q.AutoGenerateSynonymsPhraseQuery = &v
	return q
}

// WithFuzzyPrefixLength sets the number of beginning characters left unchanged for fuzzy matching.
func (q *SimpleQueryStringQuery) WithFuzzyPrefixLength(v int) *SimpleQueryStringQuery {
	q.FuzzyPrefixLength = v
	return q
}

// WithFuzzyMaxExpansions sets the maximum number of terms for fuzzy matching expansion.
func (q *SimpleQueryStringQuery) WithFuzzyMaxExpansions(v int) *SimpleQueryStringQuery {
	q.FuzzyMaxExpansions = v
	return q
}

// WithFuzzyTranspositions sets whether transpositions are counted as a single edit for fuzzy matching.
func (q *SimpleQueryStringQuery) WithFuzzyTranspositions(v bool) *SimpleQueryStringQuery {
	q.FuzzyTranspositions = &v
	return q
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
