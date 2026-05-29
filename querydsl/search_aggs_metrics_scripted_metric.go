package querydsl

type ScriptedMetricAggregation struct {
	InitScript    *Script
	MapScript     *Script
	CombineScript *Script
	ReduceScript  *Script
	Params        map[string]any
	Meta          map[string]any
}

func NewScriptedMetricAggregation() ScriptedMetricAggregation {
	return ScriptedMetricAggregation{}
}

func (a ScriptedMetricAggregation) Source() (any, error) {
	body := map[string]any{}
	addScript := func(key string, s *Script) {
		if s != nil {
			body[key], _ = s.Source()
		}
	}
	addScript("init_script", a.InitScript)
	addScript("map_script", a.MapScript)
	addScript("combine_script", a.CombineScript)
	addScript("reduce_script", a.ReduceScript)
	if len(a.Params) > 0 {
		body["params"] = a.Params
	}
	if len(a.Meta) > 0 {
		body["meta"] = a.Meta
	}
	return map[string]any{"scripted_metric": body}, nil
}
