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
//
// Fields can be set via struct literal or the With* chaining methods:
//
//	NewQueryStringQuery("(new york city) OR (big apple)").
//	    WithDefaultField("content").
//	    WithBoost(1.5)
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
func NewQueryStringQuery(queryString string) *QueryStringQuery {
	return &QueryStringQuery{Query: queryString, FieldBoosts: map[string]*float64{}}
}

// WithDefaultField sets the default field to search when no field prefix is given in the query string.
func (q *QueryStringQuery) WithDefaultField(field string) *QueryStringQuery {
	q.DefaultField = field
	return q
}

// WithDefaultOperator sets the default operator (AND or OR) used if no explicit operator is present.
func (q *QueryStringQuery) WithDefaultOperator(op string) *QueryStringQuery {
	q.DefaultOperator = op
	return q
}

// WithAnalyzer sets the analyzer used to analyze the query string.
func (q *QueryStringQuery) WithAnalyzer(analyzer string) *QueryStringQuery {
	q.Analyzer = analyzer
	return q
}

// WithQuoteAnalyzer sets the analyzer used for quoted phrases in the query string.
func (q *QueryStringQuery) WithQuoteAnalyzer(analyzer string) *QueryStringQuery {
	q.QuoteAnalyzer = analyzer
	return q
}

// WithQuoteFieldSuffix sets a suffix to append to quoted strings in the query.
func (q *QueryStringQuery) WithQuoteFieldSuffix(suffix string) *QueryStringQuery {
	q.QuoteFieldSuffix = suffix
	return q
}

// WithAllowLeadingWildcard sets whether * or ? are allowed as the first character of a wildcard term.
func (q *QueryStringQuery) WithAllowLeadingWildcard(allow bool) *QueryStringQuery {
	q.AllowLeadingWildcard = &allow
	return q
}

// WithLowercaseExpandedTerms sets whether expanded terms (from wildcard/prefix/fuzzy) are lowercased.
func (q *QueryStringQuery) WithLowercaseExpandedTerms(lowercase bool) *QueryStringQuery {
	q.LowercaseExpandedTerms = &lowercase
	return q
}

// WithEnablePositionIncrements sets whether position increments are enabled in result queries.
func (q *QueryStringQuery) WithEnablePositionIncrements(enable bool) *QueryStringQuery {
	q.EnablePositionIncrements = &enable
	return q
}

// WithAnalyzeWildcard sets whether wildcard and prefix queries should be analyzed.
func (q *QueryStringQuery) WithAnalyzeWildcard(analyze bool) *QueryStringQuery {
	q.AnalyzeWildcard = &analyze
	return q
}

// WithLocale sets the locale to use for string conversions during parsing.
func (q *QueryStringQuery) WithLocale(locale string) *QueryStringQuery {
	q.Locale = locale
	return q
}

// WithBoost sets the boost factor for this query.
func (q *QueryStringQuery) WithBoost(boost float64) *QueryStringQuery {
	q.Boost = &boost
	return q
}

// WithFuzziness sets the fuzziness for fuzzy queries (e.g. "AUTO", "1", "2").
func (q *QueryStringQuery) WithFuzziness(fuzziness string) *QueryStringQuery {
	q.Fuzziness = fuzziness
	return q
}

// WithFuzzyPrefixLength sets the number of beginning characters left unchanged for fuzzy queries.
func (q *QueryStringQuery) WithFuzzyPrefixLength(length int) *QueryStringQuery {
	q.FuzzyPrefixLength = &length
	return q
}

// WithFuzzyMaxExpansions sets the maximum number of terms a fuzzy query will expand to.
func (q *QueryStringQuery) WithFuzzyMaxExpansions(max int) *QueryStringQuery {
	q.FuzzyMaxExpansions = &max
	return q
}

// WithFuzzyRewrite sets the rewrite method for fuzzy queries.
func (q *QueryStringQuery) WithFuzzyRewrite(rewrite string) *QueryStringQuery {
	q.FuzzyRewrite = rewrite
	return q
}

// WithPhraseSlop sets the slop (maximum number of intervening unmatched positions) for phrase queries.
func (q *QueryStringQuery) WithPhraseSlop(slop int) *QueryStringQuery {
	q.PhraseSlop = &slop
	return q
}

// WithFields sets the fields to search. Use [QueryStringQuery.WithFieldBoost] to add a boost per field.
func (q *QueryStringQuery) WithFields(fields ...string) *QueryStringQuery {
	q.Fields = append(q.Fields, fields...)
	return q
}

// WithFieldBoost adds a field with an optional boost to the fields list. Pass nil for no boost.
func (q *QueryStringQuery) WithFieldBoost(field string, boost *float64) *QueryStringQuery {
	q.Fields = append(q.Fields, field)
	if q.FieldBoosts == nil {
		q.FieldBoosts = map[string]*float64{}
	}
	if boost != nil {
		q.FieldBoosts[field] = boost
	}
	return q
}

// WithTieBreaker sets the tie-breaker value for multi-field queries.
func (q *QueryStringQuery) WithTieBreaker(tieBreaker float64) *QueryStringQuery {
	q.TieBreaker = &tieBreaker
	return q
}

// WithRewrite sets the rewrite method used to rewrite prefix and wildcard queries.
func (q *QueryStringQuery) WithRewrite(rewrite string) *QueryStringQuery {
	q.Rewrite = rewrite
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *QueryStringQuery) WithMinimumShouldMatch(minimumShouldMatch string) *QueryStringQuery {
	q.MinimumShouldMatch = minimumShouldMatch
	return q
}

// WithLenient sets whether format-based errors (like providing a text value to a numeric field)
// should be silently ignored.
func (q *QueryStringQuery) WithLenient(lenient bool) *QueryStringQuery {
	q.Lenient = &lenient
	return q
}

// WithQueryName sets the named query to identify the query in the response.
func (q *QueryStringQuery) WithQueryName(queryName string) *QueryStringQuery {
	q.QueryName = queryName
	return q
}

// WithTimeZone sets the time zone applied to date values in the query.
func (q *QueryStringQuery) WithTimeZone(timeZone string) *QueryStringQuery {
	q.TimeZone = timeZone
	return q
}

// WithMaxDeterminizedStates sets the maximum number of automaton states for regex queries.
func (q *QueryStringQuery) WithMaxDeterminizedStates(max int) *QueryStringQuery {
	q.MaxDeterminizedStates = &max
	return q
}

// WithEscape sets whether special characters should be automatically escaped.
func (q *QueryStringQuery) WithEscape(escape bool) *QueryStringQuery {
	q.Escape = &escape
	return q
}

// WithType sets the multi-match type to use when multiple fields are specified
// (e.g. "best_fields", "cross_fields", "most_fields", "phrase", "phrase_prefix").
func (q *QueryStringQuery) WithType(t string) *QueryStringQuery {
	q.Type = t
	return q
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
