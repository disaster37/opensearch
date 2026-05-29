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
func NewCommonTermsQuery(field string, text any) CommonTermsQuery {
	return CommonTermsQuery{Field: field, Query: text}
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
