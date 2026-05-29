package querydsl

type FunctionScoreQuery struct {
	query      Query
	filter     Query
	boost      *float64
	maxBoost   *float64
	scoreMode  string
	boostMode  string
	filters    []Query
	scoreFuncs []ScoreFunction
	minScore   *float64
}

func NewFunctionScoreQuery() *FunctionScoreQuery {
	return &FunctionScoreQuery{}
}

func (q *FunctionScoreQuery) Query(query Query) *FunctionScoreQuery {
	q.query = query
	return q
}

func (q *FunctionScoreQuery) Filter(filter Query) *FunctionScoreQuery {
	q.filter = filter
	return q
}

func (q *FunctionScoreQuery) Add(filter Query, fn ScoreFunction) *FunctionScoreQuery {
	q.filters = append(q.filters, filter)
	q.scoreFuncs = append(q.scoreFuncs, fn)
	return q
}

func (q *FunctionScoreQuery) AddScoreFunc(fn ScoreFunction) *FunctionScoreQuery {
	q.filters = append(q.filters, nil)
	q.scoreFuncs = append(q.scoreFuncs, fn)
	return q
}

func (q *FunctionScoreQuery) ScoreMode(scoreMode string) *FunctionScoreQuery {
	q.scoreMode = scoreMode
	return q
}

func (q *FunctionScoreQuery) BoostMode(boostMode string) *FunctionScoreQuery {
	q.boostMode = boostMode
	return q
}

func (q *FunctionScoreQuery) MaxBoost(maxBoost float64) *FunctionScoreQuery {
	q.maxBoost = &maxBoost
	return q
}

func (q *FunctionScoreQuery) Boost(boost float64) *FunctionScoreQuery {
	q.boost = &boost
	return q
}

func (q *FunctionScoreQuery) MinScore(minScore float64) *FunctionScoreQuery {
	q.minScore = &minScore
	return q
}

func (q *FunctionScoreQuery) Source() (any, error) {
	body := map[string]any{}
	if q.query != nil {
		src, err := q.query.Source()
		if err != nil {
			return nil, err
		}
		body["query"] = src
	}
	if q.filter != nil {
		src, err := q.filter.Source()
		if err != nil {
			return nil, err
		}
		body["filter"] = src
	}
	if len(q.filters) > 0 {
		funcs := make([]any, len(q.filters))
		for i, filter := range q.filters {
			hsh := map[string]any{}
			if filter != nil {
				src, err := filter.Source()
				if err != nil {
					return nil, err
				}
				hsh["filter"] = src
			}
			if w := q.scoreFuncs[i].GetWeight(); w != nil {
				hsh["weight"] = w
			}
			src, err := q.scoreFuncs[i].Source()
			if err != nil {
				return nil, err
			}
			hsh[q.scoreFuncs[i].Name()] = src
			funcs[i] = hsh
		}
		body["functions"] = funcs
	}
	if q.scoreMode != "" {
		body["score_mode"] = q.scoreMode
	}
	if q.boostMode != "" {
		body["boost_mode"] = q.boostMode
	}
	if q.maxBoost != nil {
		body["max_boost"] = *q.maxBoost
	}
	if q.boost != nil {
		body["boost"] = *q.boost
	}
	if q.minScore != nil {
		body["min_score"] = *q.minScore
	}
	return map[string]any{"function_score": body}, nil
}
