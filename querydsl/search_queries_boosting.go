package querydsl

// BoostingQuery matches documents that match a positive query while reducing the relevance
// score of documents that also match a negative query. Documents matching the negative query
// are not excluded but have their scores multiplied by the negative_boost factor.
//
// Typical use: demoting results that contain certain terms without excluding them entirely.
//
// JSON DSL output:
//
//	{
//	  "boosting": {
//	    "positive": {"match": {"text": "apple"}},
//	    "negative": {"match": {"text": "pie"}},
//	    "negative_boost": 0.5
//	  }
//	}
type BoostingQuery struct {
	Positive      Query
	Negative      Query
	NegativeBoost *float64
	Boost         *float64
}

// NewBoostingQuery creates a new empty BoostingQuery.
func NewBoostingQuery() BoostingQuery {
	return BoostingQuery{}
}

func (q BoostingQuery) Source() (any, error) {
	body := map[string]any{}
	if q.Positive != nil {
		src, err := q.Positive.Source()
		if err != nil {
			return nil, err
		}
		body["positive"] = src
	}
	if q.Negative != nil {
		src, err := q.Negative.Source()
		if err != nil {
			return nil, err
		}
		body["negative"] = src
	}
	if q.NegativeBoost != nil {
		body["negative_boost"] = *q.NegativeBoost
	}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	return map[string]any{"boosting": body}, nil
}
