package querydsl

type ScriptQuery struct {
	Script    *Script
	QueryName string
}

func NewScriptQuery(script *Script) ScriptQuery {
	return ScriptQuery{Script: script}
}

func (q ScriptQuery) Source() (any, error) {
	params := map[string]any{}
	if q.Script != nil {
		params["script"], _ = q.Script.Source()
	}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"script": params}, nil
}
