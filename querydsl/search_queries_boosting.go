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
func NewBoostingQuery() *BoostingQuery {
	return &BoostingQuery{}
}

// WithPositive sets the positive query.
func (q *BoostingQuery) WithPositive(positive Query) *BoostingQuery {
	q.Positive = positive
	return q
}

// WithNegative sets the negative query.
func (q *BoostingQuery) WithNegative(negative Query) *BoostingQuery {
	q.Negative = negative
	return q
}

// WithNegativeBoost sets the negative_boost factor applied to documents matching the negative query.
func (q *BoostingQuery) WithNegativeBoost(negativeBoost float64) *BoostingQuery {
	q.NegativeBoost = &negativeBoost
	return q
}

// WithBoost sets the boost factor for the query.
func (q *BoostingQuery) WithBoost(boost float64) *BoostingQuery {
	q.Boost = &boost
	return q
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
