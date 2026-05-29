package querydsl

type ScriptScoreQuery struct {
	Query     Query
	Script    *Script
	MinScore  *float64
	Boost     *float64
	QueryName string
}

func NewScriptScoreQuery(query Query, script *Script) ScriptScoreQuery {
	return ScriptScoreQuery{Query: query, Script: script}
}

func (q ScriptScoreQuery) Source() (any, error) {
	body := map[string]any{}
	if q.Query != nil {
		src, err := q.Query.Source()
		if err != nil {
			return nil, err
		}
		body["query"] = src
	}
	if q.Script != nil {
		body["script"], _ = q.Script.Source()
	}
	if q.MinScore != nil {
		body["min_score"] = *q.MinScore
	}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	return map[string]any{"script_score": body}, nil
}
