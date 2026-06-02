package querydsl

// PercentilesAggregation computes one or more percentiles over numeric values
// extracted from the aggregated documents. Supports the TDigest and HDR
// histogram algorithms for approximate percentile calculation. Defaults to
// computing the 1st, 5th, 25th, 50th, 75th, 95th, and 99th percentiles.
//
// JSON output shape:
//
//	{"percentiles": {"field": "load_time"}}
type PercentilesAggregation struct {
	Field                          string
	Script                         *Script
	Format                         string
	Missing                        any
	Percentiles                    []float64
	Method                         string
	Compression                    *float64
	NumberOfSignificantValueDigits *int
	Estimator                      string
	SubAggs                        map[string]Aggregation
	Meta                           map[string]any
}

// NewPercentilesAggregation returns a new PercentilesAggregation using the TDigest method.
func NewPercentilesAggregation() *PercentilesAggregation {
	return &PercentilesAggregation{Method: "tdigest"}
}

// WithField sets the field to compute percentiles on.
func (a *PercentilesAggregation) WithField(field string) *PercentilesAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *PercentilesAggregation) WithScript(script *Script) *PercentilesAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output values.
func (a *PercentilesAggregation) WithFormat(format string) *PercentilesAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *PercentilesAggregation) WithMissing(missing any) *PercentilesAggregation {
	a.Missing = missing
	return a
}

// WithPercentiles sets the percentile values to compute (e.g. [50, 95, 99]).
func (a *PercentilesAggregation) WithPercentiles(percentiles []float64) *PercentilesAggregation {
	a.Percentiles = percentiles
	return a
}

// WithMethod sets the algorithm to use ("tdigest" or "hdr").
func (a *PercentilesAggregation) WithMethod(method string) *PercentilesAggregation {
	a.Method = method
	return a
}

// WithCompression sets the compression factor for the TDigest algorithm.
func (a *PercentilesAggregation) WithCompression(v float64) *PercentilesAggregation {
	a.Compression = &v
	return a
}

// WithNumberOfSignificantValueDigits sets the resolution for the HDR histogram algorithm.
func (a *PercentilesAggregation) WithNumberOfSignificantValueDigits(v int) *PercentilesAggregation {
	a.NumberOfSignificantValueDigits = &v
	return a
}

// WithEstimator sets the estimator method string.
func (a *PercentilesAggregation) WithEstimator(estimator string) *PercentilesAggregation {
	a.Estimator = estimator
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *PercentilesAggregation) WithSubAggs(subAggs map[string]Aggregation) *PercentilesAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *PercentilesAggregation) WithMeta(meta map[string]any) *PercentilesAggregation {
	a.Meta = meta
	return a
}

func (a PercentilesAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if len(a.Percentiles) > 0 {
		body["percents"] = a.Percentiles
	}
	switch a.Method {
	case "tdigest":
		if a.Compression != nil {
			body[a.Method] = map[string]any{"compression": *a.Compression}
		}
	case "hdr":
		if a.NumberOfSignificantValueDigits != nil {
			body[a.Method] = map[string]any{"number_of_significant_value_digits": *a.NumberOfSignificantValueDigits}
		}
	}
	if a.Estimator != "" {
		body["estimator"] = a.Estimator
	}
	return sourceAgg("percentiles", body, a.SubAggs, a.Meta, a.Script)
}
