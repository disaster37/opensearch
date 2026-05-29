package querydsl

type MultiValuesSourceFieldConfig struct {
	FieldName string
	Missing   any
	Script    *Script
	TimeZone  string
}

func (f MultiValuesSourceFieldConfig) Source() (any, error) {
	m := map[string]any{}
	if f.Missing != nil {
		m["missing"] = f.Missing
	}
	if f.Script != nil {
		m["script"], _ = f.Script.Source()
	}
	if f.FieldName != "" {
		m["field"] = f.FieldName
	}
	if f.TimeZone != "" {
		m["time_zone"] = f.TimeZone
	}
	return m, nil
}

type WeightedAvgAggregation struct {
	Fields    map[string]*MultiValuesSourceFieldConfig
	ValueType string
	Format    string
	Value     *MultiValuesSourceFieldConfig
	Weight    *MultiValuesSourceFieldConfig
	SubAggs   map[string]Aggregation
	Meta      map[string]any
}

func NewWeightedAvgAggregation() WeightedAvgAggregation {
	return WeightedAvgAggregation{Fields: map[string]*MultiValuesSourceFieldConfig{}}
}

func (a WeightedAvgAggregation) Source() (any, error) {
	body := map[string]any{}
	if len(a.Fields) > 0 {
		f := map[string]any{}
		for name, cfg := range a.Fields {
			f[name], _ = cfg.Source()
		}
		body["fields"] = f
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.ValueType != "" {
		body["value_type"] = a.ValueType
	}
	if a.Value != nil {
		body["value"], _ = a.Value.Source()
	}
	if a.Weight != nil {
		body["weight"], _ = a.Weight.Source()
	}
	return sourceAgg("weighted_avg", body, a.SubAggs, a.Meta, nil)
}
