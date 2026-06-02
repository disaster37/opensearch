package querydsl

// ScriptedMetricAggregation executes metric aggregations using scripts for
// init, map, combine, and reduce phases, allowing fully custom metric computation.
type ScriptedMetricAggregation struct {
	InitScript    *Script
	MapScript     *Script
	CombineScript *Script
	ReduceScript  *Script
	Params        map[string]any
	Meta          map[string]any
}

// NewScriptedMetricAggregation returns a new ScriptedMetricAggregation with default settings.
func NewScriptedMetricAggregation() *ScriptedMetricAggregation {
	return &ScriptedMetricAggregation{}
}

// WithInitScript sets the script executed once per shard before any documents are collected.
func (a *ScriptedMetricAggregation) WithInitScript(script *Script) *ScriptedMetricAggregation {
	a.InitScript = script
	return a
}

// WithMapScript sets the script executed once per document collected.
func (a *ScriptedMetricAggregation) WithMapScript(script *Script) *ScriptedMetricAggregation {
	a.MapScript = script
	return a
}

// WithCombineScript sets the script executed once per shard after document collection.
func (a *ScriptedMetricAggregation) WithCombineScript(script *Script) *ScriptedMetricAggregation {
	a.CombineScript = script
	return a
}

// WithReduceScript sets the script executed once on the coordinating node to reduce shard results.
func (a *ScriptedMetricAggregation) WithReduceScript(script *Script) *ScriptedMetricAggregation {
	a.ReduceScript = script
	return a
}

// WithParams sets the parameters available to all scripts in this aggregation.
func (a *ScriptedMetricAggregation) WithParams(params map[string]any) *ScriptedMetricAggregation {
	a.Params = params
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *ScriptedMetricAggregation) WithMeta(meta map[string]any) *ScriptedMetricAggregation {
	a.Meta = meta
	return a
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
