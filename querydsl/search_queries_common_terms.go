package querydsl

// CommonTermsQuery is a specialized query that distinguishes between common (high-frequency)
// and uncommon (low-frequency) terms. It provides finer control over how common terms affect
// scoring by using different operators or minimum-should-match values for each category.
//
// Deprecated: Common terms queries are deprecated in OpenSearch 2.0+. Use [MatchQuery] instead.
//
// JSON DSL output:
//
//	{
//	  "common": {
//	    "field_name": {
//	      "query": "search text",
//	      "cutoff_frequency": 0.001
//	    }
//	  }
//	}
type CommonTermsQuery struct {
	Field                      string
	Query                      any      `json:"query"`
	CutoffFrequency            *float64 `json:"cutoff_frequency,omitempty"`
	HighFreq                   *float64 `json:"high_freq,omitempty"`
	HighFreqOperator           string   `json:"high_freq_operator,omitempty"`
	HighFreqMinimumShouldMatch string   `json:"-"`
	LowFreq                    *float64 `json:"low_freq,omitempty"`
	LowFreqOperator            string   `json:"low_freq_operator,omitempty"`
	LowFreqMinimumShouldMatch  string   `json:"-"`
	Analyzer                   string   `json:"analyzer,omitempty"`
	Boost                      *float64 `json:"boost,omitempty"`
	QueryName                  string   `json:"_name,omitempty"`
}

// NewCommonTermsQuery creates a new CommonTermsQuery for the given field and text.
func NewCommonTermsQuery(field string, text any) *CommonTermsQuery {
	return &CommonTermsQuery{Field: field, Query: text}
}

// WithCutoffFrequency sets the frequency threshold separating common from uncommon terms.
func (q *CommonTermsQuery) WithCutoffFrequency(v float64) *CommonTermsQuery {
	q.CutoffFrequency = &v
	return q
}

// WithHighFreq sets the minimum score for high-frequency terms.
func (q *CommonTermsQuery) WithHighFreq(v float64) *CommonTermsQuery {
	q.HighFreq = &v
	return q
}

// WithHighFreqOperator sets the boolean operator for high-frequency terms (AND or OR).
func (q *CommonTermsQuery) WithHighFreqOperator(operator string) *CommonTermsQuery {
	q.HighFreqOperator = operator
	return q
}

// WithHighFreqMinimumShouldMatch sets the minimum should match for high-frequency terms.
func (q *CommonTermsQuery) WithHighFreqMinimumShouldMatch(v string) *CommonTermsQuery {
	q.HighFreqMinimumShouldMatch = v
	return q
}

// WithLowFreq sets the minimum score for low-frequency terms.
func (q *CommonTermsQuery) WithLowFreq(v float64) *CommonTermsQuery {
	q.LowFreq = &v
	return q
}

// WithLowFreqOperator sets the boolean operator for low-frequency terms (AND or OR).
func (q *CommonTermsQuery) WithLowFreqOperator(operator string) *CommonTermsQuery {
	q.LowFreqOperator = operator
	return q
}

// WithLowFreqMinimumShouldMatch sets the minimum should match for low-frequency terms.
func (q *CommonTermsQuery) WithLowFreqMinimumShouldMatch(v string) *CommonTermsQuery {
	q.LowFreqMinimumShouldMatch = v
	return q
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *CommonTermsQuery) WithAnalyzer(analyzer string) *CommonTermsQuery {
	q.Analyzer = analyzer
	return q
}

// WithBoost sets the boost factor for this query.
func (q *CommonTermsQuery) WithBoost(boost float64) *CommonTermsQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *CommonTermsQuery) WithQueryName(name string) *CommonTermsQuery {
	q.QueryName = name
	return q
}

func (q CommonTermsQuery) Source() (any, error) {
	m := map[string]any{"query": q.Query}
	if q.CutoffFrequency != nil {
		m["cutoff_frequency"] = *q.CutoffFrequency
	}
	if q.HighFreq != nil {
		m["high_freq"] = *q.HighFreq
	}
	if q.HighFreqOperator != "" {
		m["high_freq_operator"] = q.HighFreqOperator
	}
	if q.LowFreq != nil {
		m["low_freq"] = *q.LowFreq
	}
	if q.LowFreqOperator != "" {
		m["low_freq_operator"] = q.LowFreqOperator
	}
	if q.LowFreqMinimumShouldMatch != "" || q.HighFreqMinimumShouldMatch != "" {
		mm := map[string]any{}
		if q.LowFreqMinimumShouldMatch != "" {
			mm["low_freq"] = q.LowFreqMinimumShouldMatch
		}
		if q.HighFreqMinimumShouldMatch != "" {
			mm["high_freq"] = q.HighFreqMinimumShouldMatch
		}
		m["minimum_should_match"] = mm
	}
	if q.Analyzer != "" {
		m["analyzer"] = q.Analyzer
	}
	if q.Boost != nil {
		m["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		m["_name"] = q.QueryName
	}
	return map[string]any{"common": map[string]any{q.Field: m}}, nil
}
