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
func NewPercentilesAggregation() PercentilesAggregation {
	return PercentilesAggregation{Method: "tdigest"}
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
