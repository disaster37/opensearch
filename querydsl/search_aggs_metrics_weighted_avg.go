package querydsl

// MultiValuesSourceFieldConfig configures a field source for weighted average aggregation.
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

// WeightedAvgAggregation computes a weighted average of numeric values extracted
// from the aggregated documents. The value and weight sources can each be a field
// or a script.
type WeightedAvgAggregation struct {
	Fields    map[string]*MultiValuesSourceFieldConfig
	ValueType string
	Format    string
	Value     *MultiValuesSourceFieldConfig
	Weight    *MultiValuesSourceFieldConfig
	SubAggs   map[string]Aggregation
	Meta      map[string]any
}

// NewWeightedAvgAggregation returns a new WeightedAvgAggregation with default settings.
func NewWeightedAvgAggregation() *WeightedAvgAggregation {
	return &WeightedAvgAggregation{Fields: map[string]*MultiValuesSourceFieldConfig{}}
}

// WithFields sets the fields map for the aggregation.
func (a *WeightedAvgAggregation) WithFields(fields map[string]*MultiValuesSourceFieldConfig) *WeightedAvgAggregation {
	a.Fields = fields
	return a
}

// WithValueType sets the value type hint for the aggregation.
func (a *WeightedAvgAggregation) WithValueType(valueType string) *WeightedAvgAggregation {
	a.ValueType = valueType
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *WeightedAvgAggregation) WithFormat(format string) *WeightedAvgAggregation {
	a.Format = format
	return a
}

// WithValue sets the value source configuration.
func (a *WeightedAvgAggregation) WithValue(value *MultiValuesSourceFieldConfig) *WeightedAvgAggregation {
	a.Value = value
	return a
}

// WithWeight sets the weight source configuration.
func (a *WeightedAvgAggregation) WithWeight(weight *MultiValuesSourceFieldConfig) *WeightedAvgAggregation {
	a.Weight = weight
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *WeightedAvgAggregation) WithSubAggs(subAggs map[string]Aggregation) *WeightedAvgAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *WeightedAvgAggregation) WithMeta(meta map[string]any) *WeightedAvgAggregation {
	a.Meta = meta
	return a
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
