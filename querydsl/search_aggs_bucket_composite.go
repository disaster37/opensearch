package querydsl

// CompositeAggregation is a multi-bucket values source based aggregation
// that can be used to calculate unique composite values from source documents.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-composite-aggregation.html
type CompositeAggregation struct {
	After         map[string]any                     `json:"after,omitempty"`
	Size          *int                               `json:"size,omitempty"`
	ValuesSources []CompositeAggregationValuesSource `json:"-"`
	SubAggs       map[string]Aggregation             `json:"-"`
	Meta          map[string]any                     `json:"meta,omitempty"`
}

func NewCompositeAggregation() *CompositeAggregation {
	return &CompositeAggregation{}
}

// WithSize sets the number of composite buckets to return.
func (a *CompositeAggregation) WithSize(size int) *CompositeAggregation {
	a.Size = &size
	return a
}

// AggregateAfter sets the after key for pagination.
func (a *CompositeAggregation) AggregateAfter(after map[string]any) *CompositeAggregation {
	a.After = after
	return a
}

// Sources appends values sources to the composite aggregation.
func (a *CompositeAggregation) Sources(sources ...CompositeAggregationValuesSource) *CompositeAggregation {
	a.ValuesSources = append(a.ValuesSources, sources...)
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *CompositeAggregation) SubAggregation(name string, subAggregation Aggregation) *CompositeAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

// WithMeta sets meta data for the aggregation.
func (a *CompositeAggregation) WithMeta(metaData map[string]any) *CompositeAggregation {
	a.Meta = metaData
	return a
}

func (a CompositeAggregation) Source() (any, error) {
	sources := make([]any, len(a.ValuesSources))
	for i, s := range a.ValuesSources {
		src, err := s.Source()
		if err != nil {
			return nil, err
		}
		sources[i] = src
	}

	body := map[string]any{}
	body["sources"] = sources
	if a.Size != nil {
		body["size"] = *a.Size
	}
	if a.After != nil {
		body["after"] = a.After
	}

	return sourceAgg("composite", body, a.SubAggs, a.Meta, nil)
}

// -- Generic interface for CompositeAggregationValuesSource --

// CompositeAggregationValuesSource specifies the interface that
// all implementations for CompositeAggregation's Sources method
// need to implement.
type CompositeAggregationValuesSource interface {
	Source() (any, error)
}

// -- CompositeAggregationTermsValuesSource --

type CompositeAggregationTermsValuesSource struct {
	Name          string  `json:"name"`
	TermsField    string  `json:"terms_field,omitempty"`
	Script        *Script `json:"-"`
	ValueType     string  `json:"value_type,omitempty"`
	Missing       any     `json:"missing,omitempty"`
	MissingBucket *bool   `json:"missing_bucket,omitempty"`
	Order         string  `json:"order,omitempty"`
}

func NewCompositeAggregationTermsValuesSource(name string) CompositeAggregationTermsValuesSource {
	return CompositeAggregationTermsValuesSource{Name: name}
}

func (a CompositeAggregationTermsValuesSource) Field(field string) CompositeAggregationTermsValuesSource {
	a.TermsField = field
	return a
}

func (a CompositeAggregationTermsValuesSource) SetScript(script *Script) CompositeAggregationTermsValuesSource {
	a.Script = script
	return a
}

func (a CompositeAggregationTermsValuesSource) ValueTypeValue(valueType string) CompositeAggregationTermsValuesSource {
	a.ValueType = valueType
	return a
}

func (a CompositeAggregationTermsValuesSource) OrderValue(order string) CompositeAggregationTermsValuesSource {
	a.Order = order
	return a
}

func (a CompositeAggregationTermsValuesSource) Asc() CompositeAggregationTermsValuesSource {
	a.Order = "asc"
	return a
}

func (a CompositeAggregationTermsValuesSource) Desc() CompositeAggregationTermsValuesSource {
	a.Order = "desc"
	return a
}

func (a CompositeAggregationTermsValuesSource) MissingValue(missing any) CompositeAggregationTermsValuesSource {
	a.Missing = missing
	return a
}

func (a CompositeAggregationTermsValuesSource) MissingBucketValue(missingBucket bool) CompositeAggregationTermsValuesSource {
	a.MissingBucket = &missingBucket
	return a
}

func (a CompositeAggregationTermsValuesSource) Source() (any, error) {
	values := make(map[string]any)

	if a.TermsField != "" {
		values["field"] = a.TermsField
	}
	if a.Script != nil {
		values["script"], _ = a.Script.Source()
	}
	if a.Missing != nil {
		values["missing"] = a.Missing
	}
	if a.MissingBucket != nil {
		values["missing_bucket"] = *a.MissingBucket
	}
	if a.ValueType != "" {
		values["value_type"] = a.ValueType
	}
	if a.Order != "" {
		values["order"] = a.Order
	}

	nameMap := make(map[string]any)
	nameMap["terms"] = values

	sourceMap := make(map[string]any)
	sourceMap[a.Name] = nameMap

	return sourceMap, nil
}

// -- CompositeAggregationHistogramValuesSource --

type CompositeAggregationHistogramValuesSource struct {
	Name           string  `json:"name"`
	HistogramField string  `json:"histogram_field,omitempty"`
	Script         *Script `json:"-"`
	ValueType      string  `json:"value_type,omitempty"`
	Missing        any     `json:"missing,omitempty"`
	MissingBucket  *bool   `json:"missing_bucket,omitempty"`
	Order          string  `json:"order,omitempty"`
	Interval       float64 `json:"interval"`
}

func NewCompositeAggregationHistogramValuesSource(name string, interval float64) CompositeAggregationHistogramValuesSource {
	return CompositeAggregationHistogramValuesSource{Name: name, Interval: interval}
}

func (a CompositeAggregationHistogramValuesSource) Field(field string) CompositeAggregationHistogramValuesSource {
	a.HistogramField = field
	return a
}

func (a CompositeAggregationHistogramValuesSource) SetScript(script *Script) CompositeAggregationHistogramValuesSource {
	a.Script = script
	return a
}

func (a CompositeAggregationHistogramValuesSource) ValueTypeValue(valueType string) CompositeAggregationHistogramValuesSource {
	a.ValueType = valueType
	return a
}

func (a CompositeAggregationHistogramValuesSource) MissingValue(missing any) CompositeAggregationHistogramValuesSource {
	a.Missing = missing
	return a
}

func (a CompositeAggregationHistogramValuesSource) MissingBucketValue(missingBucket bool) CompositeAggregationHistogramValuesSource {
	a.MissingBucket = &missingBucket
	return a
}

func (a CompositeAggregationHistogramValuesSource) OrderValue(order string) CompositeAggregationHistogramValuesSource {
	a.Order = order
	return a
}

func (a CompositeAggregationHistogramValuesSource) Asc() CompositeAggregationHistogramValuesSource {
	a.Order = "asc"
	return a
}

func (a CompositeAggregationHistogramValuesSource) Desc() CompositeAggregationHistogramValuesSource {
	a.Order = "desc"
	return a
}

func (a CompositeAggregationHistogramValuesSource) IntervalValue(interval float64) CompositeAggregationHistogramValuesSource {
	a.Interval = interval
	return a
}

func (a CompositeAggregationHistogramValuesSource) Source() (any, error) {
	values := make(map[string]any)

	if a.HistogramField != "" {
		values["field"] = a.HistogramField
	}
	if a.Script != nil {
		values["script"], _ = a.Script.Source()
	}
	if a.Missing != nil {
		values["missing"] = a.Missing
	}
	if a.MissingBucket != nil {
		values["missing_bucket"] = *a.MissingBucket
	}
	if a.ValueType != "" {
		values["value_type"] = a.ValueType
	}
	if a.Order != "" {
		values["order"] = a.Order
	}
	values["interval"] = a.Interval

	nameMap := make(map[string]any)
	nameMap["histogram"] = values

	sourceMap := make(map[string]any)
	sourceMap[a.Name] = nameMap

	return sourceMap, nil
}

// -- CompositeAggregationDateHistogramValuesSource --

type CompositeAggregationDateHistogramValuesSource struct {
	Name               string  `json:"name"`
	DateHistogramField string  `json:"date_histogram_field,omitempty"`
	Script             *Script `json:"-"`
	ValueType          string  `json:"value_type,omitempty"`
	Missing            any     `json:"missing,omitempty"`
	MissingBucket      *bool   `json:"missing_bucket,omitempty"`
	Order              string  `json:"order,omitempty"`
	Interval           any     `json:"interval,omitempty"`
	FixedInterval      any     `json:"fixed_interval,omitempty"`
	CalendarInterval   any     `json:"calendar_interval,omitempty"`
	Format             string  `json:"format,omitempty"`
	TimeZone           string  `json:"time_zone,omitempty"`
}

func NewCompositeAggregationDateHistogramValuesSource(name string) CompositeAggregationDateHistogramValuesSource {
	return CompositeAggregationDateHistogramValuesSource{Name: name}
}

func (a CompositeAggregationDateHistogramValuesSource) Field(field string) CompositeAggregationDateHistogramValuesSource {
	a.DateHistogramField = field
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) SetScript(script *Script) CompositeAggregationDateHistogramValuesSource {
	a.Script = script
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) ValueTypeValue(valueType string) CompositeAggregationDateHistogramValuesSource {
	a.ValueType = valueType
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) MissingValue(missing any) CompositeAggregationDateHistogramValuesSource {
	a.Missing = missing
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) MissingBucketValue(missingBucket bool) CompositeAggregationDateHistogramValuesSource {
	a.MissingBucket = &missingBucket
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) OrderValue(order string) CompositeAggregationDateHistogramValuesSource {
	a.Order = order
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) Asc() CompositeAggregationDateHistogramValuesSource {
	a.Order = "asc"
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) Desc() CompositeAggregationDateHistogramValuesSource {
	a.Order = "desc"
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) IntervalValue(interval any) CompositeAggregationDateHistogramValuesSource {
	a.Interval = interval
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) FixedIntervalValue(fixedInterval any) CompositeAggregationDateHistogramValuesSource {
	a.FixedInterval = fixedInterval
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) CalendarIntervalValue(calendarInterval any) CompositeAggregationDateHistogramValuesSource {
	a.CalendarInterval = calendarInterval
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) FormatValue(format string) CompositeAggregationDateHistogramValuesSource {
	a.Format = format
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) TimeZoneValue(timeZone string) CompositeAggregationDateHistogramValuesSource {
	a.TimeZone = timeZone
	return a
}

func (a CompositeAggregationDateHistogramValuesSource) Source() (any, error) {
	values := make(map[string]any)

	if a.DateHistogramField != "" {
		values["field"] = a.DateHistogramField
	}
	if a.Script != nil {
		values["script"], _ = a.Script.Source()
	}
	if a.Missing != nil {
		values["missing"] = a.Missing
	}
	if a.MissingBucket != nil {
		values["missing_bucket"] = *a.MissingBucket
	}
	if a.ValueType != "" {
		values["value_type"] = a.ValueType
	}
	if a.Order != "" {
		values["order"] = a.Order
	}
	if a.Format != "" {
		values["format"] = a.Format
	}
	if v := a.Interval; v != nil {
		values["interval"] = v
	}
	if v := a.FixedInterval; v != nil {
		values["fixed_interval"] = v
	}
	if v := a.CalendarInterval; v != nil {
		values["calendar_interval"] = v
	}
	if a.TimeZone != "" {
		values["time_zone"] = a.TimeZone
	}

	nameMap := make(map[string]any)
	nameMap["date_histogram"] = values

	sourceMap := make(map[string]any)
	sourceMap[a.Name] = nameMap

	return sourceMap, nil
}
