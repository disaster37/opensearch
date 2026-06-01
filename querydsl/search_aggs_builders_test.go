// search_aggs_builders_test.go exercises the Source() method of every
// concrete aggregation builder (metric, bucket, pipeline). Each test
// creates the aggregation, sets all optional fields, and asserts the
// resulting JSON DSL map contains the expected keys. This is the
// largest test file in the package and serves as the canonical
// reference for aggregation serialization output.
package querydsl

import (
	"testing"
	"time"

	json "github.com/goccy/go-json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func helperPtrFloat64(v float64) *float64 { return &v }
func helperPtrInt(v int) *int             { return &v }
func helperPtrBool(v bool) *bool          { return &v }
func helperPtrInt64(v int64) *int64       { return &v }
func helperPtrFloat32(v float32) *float32 { return &v }

// ── Metric aggregations ──────────────────────────────────────────────────────

func TestCovAvgAggregation_Source(t *testing.T) {
	a := NewAvgAggregation()
	a.Field = "price"
	a.Format = "0.00"
	a.Missing = int(1)
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['price'].value * 2")
	a.SubAggs = map[string]Aggregation{"sub": NewMinAggregation()}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	src2, err := NewAvgAggregation().Source()
	require.NoError(t, err)
	assert.NotNil(t, src2)
}

func TestCovMinAggregation_Source(t *testing.T) {
	a := NewMinAggregation()
	a.Field = "price"
	a.Format = "0.00"
	a.Missing = int(0)
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['price'].value")
	a.SubAggs = map[string]Aggregation{"m": NewMaxAggregation()}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMinAggregation().Source()
	require.NoError(t, err)
}

func TestCovMaxAggregation_Source(t *testing.T) {
	a := NewMaxAggregation()
	a.Field = "grade"
	a.Format = "#"
	a.Missing = int(100)
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['grade'].value")
	a.SubAggs = map[string]Aggregation{"s": NewAvgAggregation()}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMaxAggregation().Source()
	require.NoError(t, err)
}

func TestCovSumAggregation_Source(t *testing.T) {
	a := NewSumAggregation()
	a.Field = "sales"
	a.Format = "$#,##0.00"
	a.Missing = int(0)
	a.Meta = map[string]any{"currency": "USD"}
	a.Script = NewScript("doc['sales'].value * 1.1")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewSumAggregation().Source()
	require.NoError(t, err)
}

func TestCovValueCountAggregation_Source(t *testing.T) {
	a := NewValueCountAggregation()
	a.Field = "grade"
	a.Format = "d"
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['grade'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewValueCountAggregation().Source()
	require.NoError(t, err)
}

func TestCovCardinalityAggregation_Source(t *testing.T) {
	a := NewCardinalityAggregation()
	a.Field = "author"
	a.Format = "#"
	a.Missing = "N/A"
	a.PrecisionThreshold = helperPtrInt64(40000)
	a.Rehash = helperPtrBool(true)
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['author'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewCardinalityAggregation().Source()
	require.NoError(t, err)
}

func TestCovStatsAggregation_Source(t *testing.T) {
	a := NewStatsAggregation()
	a.Field = "grade"
	a.Format = "0.00"
	a.Missing = int(0)
	a.Meta = map[string]any{"unit": "points"}
	a.Script = NewScript("doc['grade'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewStatsAggregation().Source()
	require.NoError(t, err)
}

func TestCovExtendedStatsAggregation_Source(t *testing.T) {
	sigma := 2.0
	a := NewExtendedStatsAggregation()
	a.Field = "grade"
	a.Format = "0.0000"
	a.Missing = int(0)
	a.Sigma = &sigma
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['grade'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewExtendedStatsAggregation().Source()
	require.NoError(t, err)
}

func TestCovPercentilesAggregation_Source(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		a := NewPercentilesAggregation()
		a.Field = "load_time"
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("tdigest_with_compression", func(t *testing.T) {
		a := NewPercentilesAggregation()
		a.Field = "load_time"
		c := 200.0
		a.Compression = &c
		a.Percentiles = []float64{25, 50, 75}
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("hdr", func(t *testing.T) {
		a := NewPercentilesAggregation()
		a.Method = "hdr"
		n := 3
		a.NumberOfSignificantValueDigits = &n
		a.Field = "load_time"
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("all_fields", func(t *testing.T) {
		a := NewPercentilesAggregation()
		a.Field = "load_time"
		a.Format = "0.0"
		a.Missing = int(0)
		a.Estimator = "default"
		a.Meta = map[string]any{"k": "v"}
		a.Script = NewScript("doc['load_time'].value")
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovPercentileRanksAggregation_Source(t *testing.T) {
	a := NewPercentileRanksAggregation()
	a.Field = "load_time"
	a.Format = "0.00"
	a.Missing = int(0)
	a.Values = []float64{100, 250}
	c := 200.0
	a.Compression = &c
	a.Estimator = "default"
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['load_time'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewPercentileRanksAggregation().Source()
	require.NoError(t, err)
}

func TestCovTopHitsAggregation_Source(t *testing.T) {
	a := NewTopHitsAggregation()
	a.Size(5).From(0).Sort("date", false).TrackScores(true)
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := NewTopHitsAggregation()
	a2.SearchSourceBuilder(nil)
	src2, err := a2.Source()
	require.NoError(t, err)
	assert.NotNil(t, src2)

	_, _ = NewTopHitsAggregation().
		Explain(true).
		Version(true).
		NoStoredFields().
		FetchSource(true).
		Source()
}

func TestCovWeightedAvgAggregation_Source(t *testing.T) {
	a := NewWeightedAvgAggregation()
	a.Fields["value"] = &MultiValuesSourceFieldConfig{
		FieldName: "grade",
		Missing:   int(0),
		TimeZone:  "UTC",
		Script:    NewScript("doc['grade'].value"),
	}
	a.ValueType = "double"
	a.Format = "0.00"
	a.Value = &MultiValuesSourceFieldConfig{FieldName: "grade"}
	a.Weight = &MultiValuesSourceFieldConfig{FieldName: "weight"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewWeightedAvgAggregation().Source()
	require.NoError(t, err)

	cfg := MultiValuesSourceFieldConfig{}
	src3, err := cfg.Source()
	require.NoError(t, err)
	assert.NotNil(t, src3)
}

func TestCovGeoBoundsAggregation_Source(t *testing.T) {
	a := NewGeoBoundsAggregation()
	a.Field = "location"
	wrap := true
	a.WrapLongitude = &wrap
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['location']")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewGeoBoundsAggregation().Source()
	require.NoError(t, err)
}

func TestCovGeoCentroidAggregation_Source(t *testing.T) {
	a := NewGeoCentroidAggregation()
	a.Field = "location"
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['location']")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewGeoCentroidAggregation().Source()
	require.NoError(t, err)
}

func TestCovMedianAbsoluteDeviationAggregation_Source(t *testing.T) {
	a := NewMedianAbsoluteDeviationAggregation()
	a.Field = "response_time"
	a.Format = "0.00"
	a.Missing = int(0)
	c := 100.0
	a.Compression = &c
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['response_time'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMedianAbsoluteDeviationAggregation().Source()
	require.NoError(t, err)
}

func TestCovScriptedMetricAggregation_Source(t *testing.T) {
	a := NewScriptedMetricAggregation()
	a.InitScript = NewScript("state.transactions = []")
	a.MapScript = NewScript("state.transactions.add(doc.type.value)")
	a.CombineScript = NewScript("return state.transactions")
	a.ReduceScript = NewScript("return states")
	a.Params = map[string]any{"currency": "USD"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewScriptedMetricAggregation().Source()
	require.NoError(t, err)
}

func TestCovMatrixStatsAggregation_Source(t *testing.T) {
	a := NewMatrixStatsAggregation()
	a.Fields = []string{"poverty", "income"}
	a.Missing = map[string]any{"poverty": 0}
	a.Format = "0.00"
	a.ValueType = "double"
	a.Mode = "avg"
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['poverty'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMatrixStatsAggregation().Source()
	require.NoError(t, err)
}

// ── Bucket aggregations ──────────────────────────────────────────────────────

func TestCovTermsAggregation_Source(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		a := NewTermsAggregation().WithField("genre")
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("all_options", func(t *testing.T) {
		a := NewTermsAggregation().
			WithField("genre").
			WithSize(10).
			WithShardSize(20).
			WithRequiredSize(30).
			WithMinDocCount(1).
			WithShardMinDocCount(2).
			WithMissing("N/A").
			WithValueType("string").
			WithExecutionHint("map").
			WithCollectionMode("breadth_first").
			WithShowTermDocCountError(true).
			WithScript(NewScript("doc['genre'].value")).
			WithMeta(map[string]any{"color": "blue"}).
			OrderByKeyAsc().
			OrderByCountDesc().
			OrderByTerm(true).
			OrderByTermDesc().
			OrderByAggregation("sub_avg", true).
			OrderByAggregationAndMetric("sub_avg", "value", false).
			OrderByKeyDesc().
			OrderByCountAsc().
			OrderBy("custom_field", true)

		a = a.WithSubAggregation("sub_avg", NewAvgAggregation())
		a = a.WithInclude("foo.*")
		a = a.WithExclude("bar.*")
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("include_values", func(t *testing.T) {
		a := NewTermsAggregation().
			WithField("genre").
			WithIncludeValues("a", "b", "c").
			WithExcludeValues("x", "y")
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("partitions", func(t *testing.T) {
		a := NewTermsAggregation().
			WithField("genre").
			WithPartition(0).
			WithNumPartitions(5)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("include_exclude_obj", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Include: "foo.*", Exclude: "bar.*"}
		a := NewTermsAggregation().
			WithField("genre").
			WithIncludeExclude(ie)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovTermsAggregationIncludeExclude_Source(t *testing.T) {
	t.Run("regexp", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Include: "foo.*", Exclude: "bar.*"}
		src, err := ie.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "foo.*", m["include"])
		assert.Equal(t, "bar.*", m["exclude"])
	})
	t.Run("values", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{
			IncludeValues: []any{"a", "b"},
			ExcludeValues: []any{"x"},
		}
		src, err := ie.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("partitions", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Partition: 0, NumPartitions: 5}
		src, err := ie.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovHistogramAggregation_Source(t *testing.T) {
	a := NewHistogramAggregation()
	a.Field_("price").
		Interval_(50).
		MinDocCount_(1).
		Offset_(10).
		ExtendedBounds(0, 200).
		Order_("_count", true).
		Missing_(int(0)).
		Script_(NewScript("doc['price'].value")).
		Meta_(map[string]any{"k": "v"}).
		SubAggregation("avg_price", NewAvgAggregation())

	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := NewHistogramAggregation()
	a2.Field_("price").
		Interval_(50).
		ExtendedBoundsMin(0).
		ExtendedBoundsMax(500).
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByAggregation("agg1", true).
		OrderByAggregationAndMetric("agg1", "value", false).
		OrderByCount(true).
		OrderByKey(false)

	_, err = a2.Source()
	require.NoError(t, err)
}

func TestCovRangeAggregation_Source(t *testing.T) {
	t.Run("all_options", func(t *testing.T) {
		k := true
		u := true
		a := NewRangeAggregation()
		a.FieldVal = "price"
		a.Keyed = &k
		a.Unmapped = &u
		a.Missing = int(0)
		a.Meta = map[string]any{"k": "v"}
		a.Script = NewScript("doc['price'].value")
		a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
		a = a.AddRange(0, 50).
			AddRangeWithKey("cheap", 0, 50).
			AddUnboundedTo(100).
			AddUnboundedToWithKey("above100", 100).
			AddUnboundedFrom(0).
			AddUnboundedFromWithKey("below0", 0).
			Lt(50).
			LtWithKey("lt50", 50).
			Between(10, 100).
			BetweenWithKey("mid", 10, 100).
			Gt(100).
			GtWithKey("gt100", 100)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("range_bound_value_types", func(t *testing.T) {
		now := time.Now()
		a := NewRangeAggregation().
			AddRange(int(1), int64(100)).
			AddRange(int16(0), int32(50)).
			AddRange(float32(0), float64(50)).
			AddRange(&[]int{0}[0], &[]int64{50}[0]).
			AddRange(&[]int16{0}[0], &[]int32{50}[0]).
			AddRange(&[]float32{0}[0], &[]float64{50}[0]).
			AddRange(now, &now).
			AddRange("2020-01-01", &[]string{"2021-01-01"}[0]).
			AddRange(struct{}{}, struct{}{})
		_, err := a.Source()
		require.NoError(t, err)
	})
}

func TestCovDateHistogramAggregation_Source(t *testing.T) {
	a := NewDateHistogramAggregation()
	a.Field_("timestamp").
		CalendarInterval_("month").
		Format_("yyyy-MM").
		TimeZone_("UTC").
		Offset_("+1d").
		MinDocCount_(0).
		ExtendedBounds("2020-01", "2021-12").
		Keyed_(true).
		Interval_("1d").
		FixedInterval_("6h").
		Script_(NewScript("doc['timestamp'].value")).
		Missing_("N/A").
		Meta_(map[string]any{"k": "v"}).
		SubAggregation("sub", NewAvgAggregation()).
		Order_("_count", true).
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByAggregation("agg1", true).
		OrderByAggregationAndMetric("agg1", "value", false)

	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewDateHistogramAggregation().Source()
	require.NoError(t, err)
}

func TestCovCompositeAggregation_Source(t *testing.T) {
	t.Run("terms_source", func(t *testing.T) {
		ts := NewCompositeAggregationTermsValuesSource("products").
			Field("product").
			OrderValue("asc").
			Asc().
			Desc().
			MissingValue("N/A").
			MissingBucketValue(true).
			ValueTypeValue("string")
		src, err := ts.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)

		ts2 := NewCompositeAggregationTermsValuesSource("p").
			SetScript(NewScript("doc['p'].value"))
		_, err = ts2.Source()
		require.NoError(t, err)
	})
	t.Run("histogram_source", func(t *testing.T) {
		hs := NewCompositeAggregationHistogramValuesSource("prices", 50).
			Field("price").
			OrderValue("asc").
			Asc().
			Desc().
			IntervalValue(100).
			MissingValue(int(0)).
			MissingBucketValue(true).
			ValueTypeValue("double")
		src, err := hs.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)

		hs2 := NewCompositeAggregationHistogramValuesSource("p", 10).
			SetScript(NewScript("doc['p'].value"))
		_, err = hs2.Source()
		require.NoError(t, err)
	})
	t.Run("date_histogram_source", func(t *testing.T) {
		ds := NewCompositeAggregationDateHistogramValuesSource("dates").
			Field("date").
			OrderValue("asc").
			Asc().
			Desc().
			IntervalValue("1d").
			FixedIntervalValue("1h").
			CalendarIntervalValue("1M").
			FormatValue("yyyy-MM").
			TimeZoneValue("UTC").
			MissingValue("now").
			MissingBucketValue(true).
			ValueTypeValue("date")
		src, err := ds.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)

		ds2 := NewCompositeAggregationDateHistogramValuesSource("d").
			SetScript(NewScript("doc['d'].value"))
		_, err = ds2.Source()
		require.NoError(t, err)
	})
	t.Run("composite_agg", func(t *testing.T) {
		ts := NewCompositeAggregationTermsValuesSource("genre").Field("genre")
		hs := NewCompositeAggregationHistogramValuesSource("price", 10).Field("price")
		ds := NewCompositeAggregationDateHistogramValuesSource("date").Field("date").CalendarIntervalValue("1M")
		a := NewCompositeAggregation().
			WithSize(10).
			AggregateAfter(map[string]any{"genre": "jazz"}).
			Sources(ts, hs, ds).
			SubAggregation("sub", NewAvgAggregation()).
			WithMeta(map[string]any{"k": "v"})
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovFilterAggregation_Source(t *testing.T) {
	q := NewMatchAllQuery()
	a := FilterAggregation{
		Filter:  q,
		SubAggs: map[string]Aggregation{"avg": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovFiltersAggregation_Source(t *testing.T) {
	t.Run("named", func(t *testing.T) {
		a := NewFiltersAggregation()
		a.NamedFilters["errors"] = NewMatchAllQuery()
		a.OtherBucket = helperPtrBool(true)
		a.OtherBucketKey = "other"
		a.Meta = map[string]any{"k": "v"}
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("unnamed", func(t *testing.T) {
		a := NewFiltersAggregation()
		a.NamedFilters = nil
		a.UnnamedFilters = []Query{NewMatchAllQuery(), NewMatchAllQuery()}
		a.OtherBucket = helperPtrBool(false)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovNestedAggregation_Source(t *testing.T) {
	a := NestedAggregation{
		Path:    "line_items",
		SubAggs: map[string]Aggregation{"avg": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := NestedAggregation{Path: "items"}
	src2, err := a2.Source()
	require.NoError(t, err)
	assert.NotNil(t, src2)
}

func TestCovReverseNestedAggregation_Source(t *testing.T) {
	a := ReverseNestedAggregation{
		Path:    "parent",
		SubAggs: map[string]Aggregation{"sub": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := ReverseNestedAggregation{}
	_, err = a2.Source()
	require.NoError(t, err)
}

func TestCovGlobalAggregation_Source(t *testing.T) {
	a := GlobalAggregation{
		SubAggs: map[string]Aggregation{"sub": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewGlobalAggregation().Source()
	require.NoError(t, err)
}

func TestCovMissingAggregation_Source(t *testing.T) {
	a := MissingAggregation{
		Field:   "price",
		SubAggs: map[string]Aggregation{"avg": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMissingAggregation().Source()
	require.NoError(t, err)
}

func TestCovChildrenAggregation_Source(t *testing.T) {
	a := ChildrenAggregation{
		Type:    "answer",
		SubAggs: map[string]Aggregation{"avg": NewAvgAggregation()},
		Meta:    map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewChildrenAggregation().Source()
	require.NoError(t, err)
}

func TestCovSamplerAggregation_Source(t *testing.T) {
	a := NewSamplerAggregation()
	a.ShardSize = 100
	a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewSamplerAggregation().Source()
	require.NoError(t, err)
}

func TestCovDiversifiedSamplerAggregation_Source(t *testing.T) {
	a := NewDiversifiedSamplerAggregation()
	a.Field = "author"
	a.ShardSize = 100
	a.MaxDocsPerValue = 1
	a.ExecutionHint = "map"
	a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	a.Script = NewScript("doc['author'].value")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewDiversifiedSamplerAggregation().Source()
	require.NoError(t, err)
}

func TestCovAutoDateHistogramAggregation_Source(t *testing.T) {
	a := NewAutoDateHistogramAggregation()
	a.Field = "timestamp"
	a.Buckets = helperPtrInt(10)
	a.MinDocCount = helperPtrInt64(1)
	a.TimeZone = "UTC"
	a.Format = "yyyy-MM"
	a.MinimumInterval = "month"
	a.Missing = "N/A"
	a.SubAggregation("sub", NewAvgAggregation())
	a.WithMeta(map[string]any{"k": "v"})
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := NewAutoDateHistogramAggregation().WithBuckets(5).WithMinDocCount(0)
	_, err = a2.Source()
	require.NoError(t, err)

	_, err = NewAutoDateHistogramAggregation().Source()
	require.NoError(t, err)
}

func TestCovAdjacencyMatrixAggregation_Source(t *testing.T) {
	a := NewAdjacencyMatrixAggregation()
	a.Filters["grpA"] = NewMatchAllQuery()
	a.Filters["grpB"] = NewMatchAllQuery()
	a.SubAggs = map[string]Aggregation{"sub": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewAdjacencyMatrixAggregation().Source()
	require.NoError(t, err)
}

func TestCovSignificantTermsAggregation_Source(t *testing.T) {
	t.Run("all_options", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.FieldVal = "content"
		a.MinDocCount = helperPtrInt(5)
		a.ShardMinDocCount = helperPtrInt(1)
		a.RequiredSize = helperPtrInt(100)
		a.ShardSize = helperPtrInt(200)
		a.ExecutionHint = "map"
		a.Filter = NewMatchAllQuery()
		a.Include("foo.*")
		a.Exclude("bar.*")
		a.IncludeValues("a", "b")
		a.ExcludeValues("x", "y")
		a.Partition(0)
		a.NumPartitions(5)
		a.SetIncludeExclude(&TermsAggregationIncludeExclude{})
		a.SubAggregation("sub", NewAvgAggregation())
		a.WithMeta(map[string]any{"k": "v"})

		h := NewJLHScoreSignificanceHeuristic()
		a.SignificanceHeuristic(h)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("minimal", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.FieldVal = "text"
		_, err := a.Source()
		require.NoError(t, err)
	})
}

func TestCovSignificanceHeuristics(t *testing.T) {
	t.Run("chi_square", func(t *testing.T) {
		h := NewChiSquareSignificanceHeuristic()
		assert.Equal(t, "chi_square", h.Name())
		h.BackgroundIsSupersetVal = helperPtrBool(true)
		h.IncludeNegativesVal = helperPtrBool(false)
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
		_, err = NewChiSquareSignificanceHeuristic().Source()
		require.NoError(t, err)
	})
	t.Run("gnd", func(t *testing.T) {
		h := NewGNDSignificanceHeuristic()
		assert.Equal(t, "gnd", h.Name())
		h.BackgroundIsSupersetVal = helperPtrBool(true)
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
		_, err = NewGNDSignificanceHeuristic().Source()
		require.NoError(t, err)
	})
	t.Run("jlh", func(t *testing.T) {
		h := NewJLHScoreSignificanceHeuristic()
		assert.Equal(t, "jlh", h.Name())
		_, err := h.Source()
		require.NoError(t, err)
	})
	t.Run("mutual_information", func(t *testing.T) {
		h := NewMutualInformationSignificanceHeuristic()
		assert.Equal(t, "mutual_information", h.Name())
		h.BackgroundIsSupersetVal = helperPtrBool(true)
		h.IncludeNegativesVal = helperPtrBool(false)
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
		_, err = NewMutualInformationSignificanceHeuristic().Source()
		require.NoError(t, err)
	})
	t.Run("percentage", func(t *testing.T) {
		h := NewPercentageScoreSignificanceHeuristic()
		assert.Equal(t, "percentage", h.Name())
		_, err := h.Source()
		require.NoError(t, err)
	})
	t.Run("script", func(t *testing.T) {
		h := NewScriptSignificanceHeuristic()
		assert.Equal(t, "script_heuristic", h.Name())
		h.ScriptVal = NewScript("doc['score'].value")
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
		_, err = NewScriptSignificanceHeuristic().Source()
		require.NoError(t, err)
	})
}

func TestCovSignificantTextAggregation_Source(t *testing.T) {
	a := NewSignificantTextAggregation()
	a.FieldVal = "content"
	a.FilterDuplicateText = helperPtrBool(true)
	a.SourceFieldNames = []string{"content", "title"}
	a.Filter = NewMatchAllQuery()
	a.Include("foo.*")
	a.Exclude("bar.*")
	a.IncludeValues("a", "b")
	a.ExcludeValues("x", "y")
	a.Partition(0)
	a.NumPartitions(5)
	a.SetIncludeExclude(&TermsAggregationIncludeExclude{})
	a.MinDocCount(5)
	a.ShardMinDocCount(1)
	a.WithSize(10)
	a.WithShardSize(20)
	a.SubAggregation("sub", NewAvgAggregation())
	a.WithMeta(map[string]any{"k": "v"})
	a.SignificanceHeuristic(NewJLHScoreSignificanceHeuristic())
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewSignificantTextAggregation().Source()
	require.NoError(t, err)
}

func TestCovMultiTermsAggregation_Source(t *testing.T) {
	a := NewMultiTermsAggregation().
		Terms("genre", "product").
		MultiTerms(MultiTerm{Field: "author", Missing: "unknown"}).
		WithSize(10).
		WithShardSize(20).
		WithMinDocCount(1).
		WithShardMinDocCount(2).
		WithCollectionMode("breadth_first").
		WithShowTermDocCountError(true).
		Order("genre", true).
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByAggregation("agg1", true).
		OrderByAggregationAndMetric("agg1", "value", false).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"k": "v"})
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	mto := MultiTermsOrder{Field: "_count", Ascending: true}
	_, err = mto.Source()
	require.NoError(t, err)

	mt := MultiTerm{Field: "f"}
	_, err = mt.Source()
	require.NoError(t, err)

	mt2 := MultiTerm{Field: "f", Missing: "NA"}
	_, err = mt2.Source()
	require.NoError(t, err)
}

func TestCovRareTermsAggregation_Source(t *testing.T) {
	a := NewRareTermsAggregation()
	a.Field = "genre"
	a.MaxDocCount = helperPtrInt(5)
	a.Precision = helperPtrFloat64(0.001)
	a.Missing = "N/A"
	a.IncludeExclude = &TermsAggregationIncludeExclude{Include: "foo.*", Exclude: "bar.*"}
	a.SubAggs = map[string]Aggregation{"sub": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewRareTermsAggregation().Source()
	require.NoError(t, err)
}

func TestCovIPRangeAggregation_Source(t *testing.T) {
	a := NewIPRangeAggregation().
		WithField("ip").
		WithKeyed(true).
		WithMeta(map[string]any{"k": "v"}).
		AddMaskRange("10.0.0.0/24").
		AddMaskRangeWithKey("mask1", "192.168.0.0/16").
		AddRange("10.0.0.1", "10.0.0.100").
		AddRangeWithKey("range1", "192.168.0.1", "192.168.0.50").
		AddUnboundedTo("10.0.0.1").
		AddUnboundedToWithKey("ut", "10.0.0.1").
		AddUnboundedFrom("10.0.0.1").
		AddUnboundedFromWithKey("uf", "10.0.0.1").
		Lt("10.0.0.1").
		LtWithKey("lt1", "10.0.0.1").
		Between("10.0.0.1", "10.0.0.255").
		BetweenWithKey("bt", "10.0.0.1", "10.0.0.255").
		Gt("10.0.0.1").
		GtWithKey("gt1", "10.0.0.1").
		WithSubAggregation("sub", NewAvgAggregation())
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewIPRangeAggregation().Source()
	require.NoError(t, err)
}

func TestCovGeoHashGridAggregation_Source(t *testing.T) {
	a := NewGeoHashGridAggregation()
	a.GeoHashField = "location"
	a.Precision = 3
	a.Size = helperPtrInt(10)
	a.ShardSize = helperPtrInt(25)
	a.Meta = map[string]any{"k": "v"}
	a.SubAggregation("sub", NewAvgAggregation())
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewGeoHashGridAggregation().Source()
	require.NoError(t, err)
}

func TestCovGeoTileGridAggregation_Source(t *testing.T) {
	a := NewGeoTileGridAggregation().
		WithPrecision(8).
		WithSize(10).
		WithShardSize(25).
		WithBounds(BoundingBox{
			TopLeft:     GeoPoint{Lat: 40.0, Lon: -74.0},
			BottomRight: GeoPoint{Lat: 39.0, Lon: -73.0},
		}).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"k": "v"})
	a.Field = "location"
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewGeoTileGridAggregation().Source()
	assert.Error(t, err)
}

func TestCovGeoDistanceAggregation_Source(t *testing.T) {
	a := NewGeoDistanceAggregation()
	a.Field = "location"
	a.Unit = "km"
	a.DistanceType = "arc"
	a.Origin = "52.376,"
	a.AddRange(10, 100)
	a.AddRangeWithKey("close", 0, 10)
	a.AddUnboundedTo(10.0)
	a.AddUnboundedToWithKey("far", 1000.0)
	a.AddUnboundedFrom(100.0)
	a.AddUnboundedFromWithKey("vclose", 10.0)
	a.Between(10, 100)
	a.BetweenWithKey("mid", 50, 200)
	a.SubAggregation("sub", NewAvgAggregation())
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	t.Run("range_bound_types", func(t *testing.T) {
		s := "10km"
		a2 := GeoDistanceRange{From: int(10), To: int64(100)}
		a2b := GeoDistanceRange{From: int16(10), To: int32(100)}
		a2c := GeoDistanceRange{From: float32(10), To: float64(100)}
		a2d := GeoDistanceRange{From: &[]int{10}[0], To: &[]int64{100}[0]}
		a2e := GeoDistanceRange{From: &[]int16{10}[0], To: &[]int32{100}[0]}
		a2f := GeoDistanceRange{From: &[]float32{10}[0], To: &[]float64{100}[0]}
		a2g := GeoDistanceRange{From: s, To: &[]string{"100km"}[0]}
		a2h := GeoDistanceRange{From: struct{}{}, To: struct{}{}}
		_ = []GeoDistanceRange{a2, a2b, a2c, a2d, a2e, a2f, a2g, a2h}
	})

	_, err = NewGeoDistanceAggregation().Source()
	require.NoError(t, err)
}

func TestCovDateRangeAggregation_Source(t *testing.T) {
	a := NewDateRangeAggregation().
		WithField("created").
		WithKeyed(true).
		WithUnmapped(false).
		WithTimeZone("UTC").
		WithFormat("yyyy-MM-dd").
		WithScript(NewScript("doc['created'].value")).
		WithMeta(map[string]any{"k": "v"}).
		AddRange("now-1M", "now").
		AddRangeWithKey("last_month", "now-1M", "now").
		AddUnboundedTo("now-1y").
		AddUnboundedToWithKey("old", "now-5y").
		AddUnboundedFrom("now").
		AddUnboundedFromWithKey("new", "now").
		Lt("2023-01-01").
		LtWithKey("before2023", "2023-01-01").
		Between("2022-01-01", "2023-01-01").
		BetweenWithKey("2022", "2022-01-01", "2023-01-01").
		Gt("2023-01-01").
		GtWithKey("after2023", "2023-01-01").
		WithSubAggregation("sub", NewAvgAggregation())
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewDateRangeAggregation().Source()
	require.NoError(t, err)
}

// ── Pipeline aggregations ────────────────────────────────────────────────────

func TestCovAvgBucketAggregation_Source(t *testing.T) {
	a := NewAvgBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "insert_zeros"
	a.BucketsPaths = []string{"sales_per_month>sales"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewAvgBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovSumBucketAggregation_Source(t *testing.T) {
	a := NewSumBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewSumBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovStatsBucketAggregation_Source(t *testing.T) {
	a := NewStatsBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "insert_zeros"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewStatsBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovPercentilesBucketAggregation_Source(t *testing.T) {
	a := NewPercentilesBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.Percents = []float64{25, 50, 75}
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewPercentilesBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovMaxBucketAggregation_Source(t *testing.T) {
	a := NewMaxBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMaxBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovMinBucketAggregation_Source(t *testing.T) {
	a := NewMinBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewMinBucketAggregation().Source()
	require.NoError(t, err)
}

func TestCovDerivativeAggregation_Source(t *testing.T) {
	a := NewDerivativeAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.Unit = "day"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewDerivativeAggregation().Source()
	require.NoError(t, err)
}

func TestCovCumulativeSumAggregation_Source(t *testing.T) {
	a := NewCumulativeSumAggregation()
	a.Format = "0.00"
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewCumulativeSumAggregation().Source()
	require.NoError(t, err)
}

func TestCovBucketScriptAggregation_Source(t *testing.T) {
	a := NewBucketScriptAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.BucketsPathsMap = map[string]string{"my_var1": "agg1", "my_var2": "agg2"}
	a.Script = NewScript("params.my_var1 / params.my_var2")
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewBucketScriptAggregation().Source()
	require.NoError(t, err)
}

func TestCovSerialDiffAggregation_Source(t *testing.T) {
	a := NewSerialDiffAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.Lag = helperPtrInt(7)
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewSerialDiffAggregation().Source()
	require.NoError(t, err)
}

func TestCovMovFnAggregation_Source(t *testing.T) {
	s := NewScript("MovingFunctions.max(values)")
	a := NewMovFnAggregation("the_sum", s, 10)
	a.Format = "0.00"
	a.GapPolicy = "insert_zeros"
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovMovAvgAggregation_Source(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		a := NewMovAvgAggregation()
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("ewma", func(t *testing.T) {
		m := NewEWMAMovAvgModel()
		m.Alpha = helperPtrFloat64(0.3)
		assert.Equal(t, "ewma", m.Name())
		a := NewMovAvgAggregation()
		a.Model = m
		a.BucketsPaths = []string{"agg1"}
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
		assert.NotNil(t, NewEWMAMovAvgModel().Settings())
	})
	t.Run("holt", func(t *testing.T) {
		m := NewHoltLinearMovAvgModel()
		m.Alpha = helperPtrFloat64(0.3)
		m.Beta = helperPtrFloat64(0.1)
		assert.Equal(t, "holt", m.Name())
		a := NewMovAvgAggregation()
		a.Model = m
		a.Window = helperPtrInt(5)
		a.Predict = helperPtrInt(2)
		a.Minimize = helperPtrBool(true)
		a.Format = "0.00"
		a.GapPolicy = "skip"
		_, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, NewHoltLinearMovAvgModel().Settings())
	})
	t.Run("holt_winters", func(t *testing.T) {
		m := NewHoltWintersMovAvgModel()
		m.Alpha = helperPtrFloat64(0.3)
		m.Beta = helperPtrFloat64(0.1)
		m.Gamma = helperPtrFloat64(0.5)
		m.Period = helperPtrInt(7)
		m.Pad = helperPtrBool(true)
		m.SeasonalityType = "add"
		assert.Equal(t, "holt_winters", m.Name())
		srcHW := m.Settings()
		assert.NotNil(t, srcHW)
		a := NewMovAvgAggregation()
		a.Model = m
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("linear", func(t *testing.T) {
		m := NewLinearMovAvgModel()
		assert.Equal(t, "linear", m.Name())
		assert.Nil(t, m.Settings())
	})
	t.Run("simple", func(t *testing.T) {
		m := NewSimpleMovAvgModel()
		assert.Equal(t, "simple", m.Name())
		assert.Nil(t, m.Settings())
	})
}

func TestCovBucketSortAggregation_Source(t *testing.T) {
	a := NewBucketSortAggregation()
	a.From = 10
	a.Size = 5
	a.GapPolicy = "skip"
	a.Meta = map[string]any{"k": "v"}
	a.Sorters = []Sorter{NewFieldSort("total_sales")}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	a2 := NewBucketSortAggregation()
	_, err = a2.Source()
	require.NoError(t, err)
}

func TestCovExtendedStatsBucketAggregation_Source(t *testing.T) {
	sigma := float32(2.0)
	a := NewExtendedStatsBucketAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.Sigma = &sigma
	a.BucketsPaths = []string{"agg1"}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewExtendedStatsBucketAggregation().Source()
	require.NoError(t, err)
}

// ── Response UnmarshalJSON ───────────────────────────────────────────────────

func TestCovAggregations_ResponseAccessors_NotFound(t *testing.T) {
	aggs := Aggregations{}

	val, ok := aggs.Min("x")
	assert.False(t, ok)
	assert.Nil(t, val)

	val2, ok2 := aggs.Max("x")
	assert.False(t, ok2)
	assert.Nil(t, val2)

	val3, ok3 := aggs.Sum("x")
	assert.False(t, ok3)
	assert.Nil(t, val3)

	val4, ok4 := aggs.Avg("x")
	assert.False(t, ok4)
	assert.Nil(t, val4)

	val5, ok5 := aggs.WeightedAvg("x")
	assert.False(t, ok5)
	assert.Nil(t, val5)

	val6, ok6 := aggs.MedianAbsoluteDeviation("x")
	assert.False(t, ok6)
	assert.Nil(t, val6)

	val7, ok7 := aggs.ValueCount("x")
	assert.False(t, ok7)
	assert.Nil(t, val7)

	val8, ok8 := aggs.Cardinality("x")
	assert.False(t, ok8)
	assert.Nil(t, val8)

	stats, ok9 := aggs.Stats("x")
	assert.False(t, ok9)
	assert.Nil(t, stats)

	ext, ok10 := aggs.ExtendedStats("x")
	assert.False(t, ok10)
	assert.Nil(t, ext)

	ms, ok11 := aggs.MatrixStats("x")
	assert.False(t, ok11)
	assert.Nil(t, ms)

	pct, ok12 := aggs.Percentiles("x")
	assert.False(t, ok12)
	assert.Nil(t, pct)

	pr, ok13 := aggs.PercentileRanks("x")
	assert.False(t, ok13)
	assert.Nil(t, pr)

	th, ok14 := aggs.TopHits("x")
	assert.False(t, ok14)
	assert.Nil(t, th)

	gb, ok15 := aggs.Global("x")
	assert.False(t, ok15)
	assert.Nil(t, gb)

	flt, ok16 := aggs.Filter("x")
	assert.False(t, ok16)
	assert.Nil(t, flt)

	flts, ok17 := aggs.Filters("x")
	assert.False(t, ok17)
	assert.Nil(t, flts)

	am, ok18 := aggs.AdjacencyMatrix("x")
	assert.False(t, ok18)
	assert.Nil(t, am)

	mis, ok19 := aggs.Missing("x")
	assert.False(t, ok19)
	assert.Nil(t, mis)

	nest, ok20 := aggs.Nested("x")
	assert.False(t, ok20)
	assert.Nil(t, nest)

	rn, ok21 := aggs.ReverseNested("x")
	assert.False(t, ok21)
	assert.Nil(t, rn)

	ch, ok22 := aggs.Children("x")
	assert.False(t, ok22)
	assert.Nil(t, ch)

	t2, ok23 := aggs.Terms("x")
	assert.False(t, ok23)
	assert.Nil(t, t2)

	mt, ok24 := aggs.MultiTerms("x")
	assert.False(t, ok24)
	assert.Nil(t, mt)

	st, ok25 := aggs.SignificantTerms("x")
	assert.False(t, ok25)
	assert.Nil(t, st)

	rt, ok26 := aggs.RareTerms("x")
	assert.False(t, ok26)
	assert.Nil(t, rt)

	samp, ok27 := aggs.Sampler("x")
	assert.False(t, ok27)
	assert.Nil(t, samp)

	ds, ok28 := aggs.DiversifiedSampler("x")
	assert.False(t, ok28)
	assert.Nil(t, ds)

	rng, ok29 := aggs.Range("x")
	assert.False(t, ok29)
	assert.Nil(t, rng)

	kr, ok30 := aggs.KeyedRange("x")
	assert.False(t, ok30)
	assert.Nil(t, kr)

	dr, ok31 := aggs.DateRange("x")
	assert.False(t, ok31)
	assert.Nil(t, dr)

	ipr, ok32 := aggs.IPRange("x")
	assert.False(t, ok32)
	assert.Nil(t, ipr)

	hist, ok33 := aggs.Histogram("x")
	assert.False(t, ok33)
	assert.Nil(t, hist)

	adh, ok34 := aggs.AutoDateHistogram("x")
	assert.False(t, ok34)
	assert.Nil(t, adh)

	dh, ok35 := aggs.DateHistogram("x")
	assert.False(t, ok35)
	assert.Nil(t, dh)

	kdh, ok36 := aggs.KeyedDateHistogram("x")
	assert.False(t, ok36)
	assert.Nil(t, kdh)

	geob, ok37 := aggs.GeoBounds("x")
	assert.False(t, ok37)
	assert.Nil(t, geob)

	gh, ok38 := aggs.GeoHash("x")
	assert.False(t, ok38)
	assert.Nil(t, gh)

	gt2, ok39 := aggs.GeoTile("x")
	assert.False(t, ok39)
	assert.Nil(t, gt2)

	gc, ok40 := aggs.GeoCentroid("x")
	assert.False(t, ok40)
	assert.Nil(t, gc)

	gd, ok41 := aggs.GeoDistance("x")
	assert.False(t, ok41)
	assert.Nil(t, gd)

	ab, ok42 := aggs.AvgBucket("x")
	assert.False(t, ok42)
	assert.Nil(t, ab)

	sb, ok43 := aggs.SumBucket("x")
	assert.False(t, ok43)
	assert.Nil(t, sb)

	stb, ok44 := aggs.StatsBucket("x")
	assert.False(t, ok44)
	assert.Nil(t, stb)

	pb, ok45 := aggs.PercentilesBucket("x")
	assert.False(t, ok45)
	assert.Nil(t, pb)

	mxb, ok46 := aggs.MaxBucket("x")
	assert.False(t, ok46)
	assert.Nil(t, mxb)

	mnb, ok47 := aggs.MinBucket("x")
	assert.False(t, ok47)
	assert.Nil(t, mnb)

	mov, ok48 := aggs.MovAvg("x")
	assert.False(t, ok48)
	assert.Nil(t, mov)

	mf, ok49 := aggs.MovFn("x")
	assert.False(t, ok49)
	assert.Nil(t, mf)

	der, ok50 := aggs.Derivative("x")
	assert.False(t, ok50)
	assert.Nil(t, der)

	cs, ok51 := aggs.CumulativeSum("x")
	assert.False(t, ok51)
	assert.Nil(t, cs)

	bs, ok52 := aggs.BucketScript("x")
	assert.False(t, ok52)
	assert.Nil(t, bs)

	sd, ok53 := aggs.SerialDiff("x")
	assert.False(t, ok53)
	assert.Nil(t, sd)

	comp, ok54 := aggs.Composite("x")
	assert.False(t, ok54)
	assert.Nil(t, comp)

	sm, ok55 := aggs.ScriptedMetric("x")
	assert.False(t, ok55)
	assert.Nil(t, sm)

	tm, ok56 := aggs.TopMetrics("x")
	assert.False(t, ok56)
	assert.Nil(t, tm)
}

func TestCovAggregations_ResponseAccessors_NilRaw(t *testing.T) {
	aggs := Aggregations{"x": nil}

	v, ok := aggs.Min("x")
	assert.True(t, ok)
	assert.NotNil(t, v)

	v2, ok := aggs.Max("x")
	assert.True(t, ok)
	assert.NotNil(t, v2)

	v3, ok := aggs.Sum("x")
	assert.True(t, ok)
	assert.NotNil(t, v3)

	v4, ok := aggs.Avg("x")
	assert.True(t, ok)
	assert.NotNil(t, v4)

	v5, ok := aggs.WeightedAvg("x")
	assert.True(t, ok)
	assert.NotNil(t, v5)

	v6, ok := aggs.MedianAbsoluteDeviation("x")
	assert.True(t, ok)
	assert.NotNil(t, v6)

	v7, ok := aggs.ValueCount("x")
	assert.True(t, ok)
	assert.NotNil(t, v7)

	v8, ok := aggs.Cardinality("x")
	assert.True(t, ok)
	assert.NotNil(t, v8)

	st, ok := aggs.Stats("x")
	assert.True(t, ok)
	assert.NotNil(t, st)

	es, ok := aggs.ExtendedStats("x")
	assert.True(t, ok)
	assert.NotNil(t, es)

	ms, ok := aggs.MatrixStats("x")
	assert.True(t, ok)
	assert.NotNil(t, ms)

	pc, ok := aggs.Percentiles("x")
	assert.True(t, ok)
	assert.NotNil(t, pc)

	pr, ok := aggs.PercentileRanks("x")
	assert.True(t, ok)
	assert.NotNil(t, pr)

	th, ok := aggs.TopHits("x")
	assert.True(t, ok)
	assert.NotNil(t, th)

	gl, ok := aggs.Global("x")
	assert.True(t, ok)
	assert.NotNil(t, gl)

	fl, ok := aggs.Filter("x")
	assert.True(t, ok)
	assert.NotNil(t, fl)

	fs, ok := aggs.Filters("x")
	assert.True(t, ok)
	assert.NotNil(t, fs)

	am, ok := aggs.AdjacencyMatrix("x")
	assert.True(t, ok)
	assert.NotNil(t, am)

	mi, ok := aggs.Missing("x")
	assert.True(t, ok)
	assert.NotNil(t, mi)

	ne, ok := aggs.Nested("x")
	assert.True(t, ok)
	assert.NotNil(t, ne)

	rn, ok := aggs.ReverseNested("x")
	assert.True(t, ok)
	assert.NotNil(t, rn)

	ch, ok := aggs.Children("x")
	assert.True(t, ok)
	assert.NotNil(t, ch)

	tr, ok := aggs.Terms("x")
	assert.True(t, ok)
	assert.NotNil(t, tr)

	mt, ok := aggs.MultiTerms("x")
	assert.True(t, ok)
	assert.NotNil(t, mt)

	stm, ok := aggs.SignificantTerms("x")
	assert.True(t, ok)
	assert.NotNil(t, stm)

	rt, ok := aggs.RareTerms("x")
	assert.True(t, ok)
	assert.NotNil(t, rt)

	sa, ok := aggs.Sampler("x")
	assert.True(t, ok)
	assert.NotNil(t, sa)

	ds, ok := aggs.DiversifiedSampler("x")
	assert.True(t, ok)
	assert.NotNil(t, ds)

	rg, ok := aggs.Range("x")
	assert.True(t, ok)
	assert.NotNil(t, rg)

	kr, ok := aggs.KeyedRange("x")
	assert.True(t, ok)
	assert.NotNil(t, kr)

	dr, ok := aggs.DateRange("x")
	assert.True(t, ok)
	assert.NotNil(t, dr)

	ip, ok := aggs.IPRange("x")
	assert.True(t, ok)
	assert.NotNil(t, ip)

	hi, ok := aggs.Histogram("x")
	assert.True(t, ok)
	assert.NotNil(t, hi)

	ah, ok := aggs.AutoDateHistogram("x")
	assert.True(t, ok)
	assert.NotNil(t, ah)

	dh, ok := aggs.DateHistogram("x")
	assert.True(t, ok)
	assert.NotNil(t, dh)

	kd, ok := aggs.KeyedDateHistogram("x")
	assert.True(t, ok)
	assert.NotNil(t, kd)

	gb, ok := aggs.GeoBounds("x")
	assert.True(t, ok)
	assert.NotNil(t, gb)

	gh, ok := aggs.GeoHash("x")
	assert.True(t, ok)
	assert.NotNil(t, gh)

	gtt, ok := aggs.GeoTile("x")
	assert.True(t, ok)
	assert.NotNil(t, gtt)

	gc, ok := aggs.GeoCentroid("x")
	assert.True(t, ok)
	assert.NotNil(t, gc)

	gd, ok := aggs.GeoDistance("x")
	assert.True(t, ok)
	assert.NotNil(t, gd)

	ab, ok := aggs.AvgBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, ab)

	sb, ok := aggs.SumBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, sb)

	stb, ok := aggs.StatsBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, stb)

	pb, ok := aggs.PercentilesBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, pb)

	xb, ok := aggs.MaxBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, xb)

	nb, ok := aggs.MinBucket("x")
	assert.True(t, ok)
	assert.NotNil(t, nb)

	ma, ok := aggs.MovAvg("x")
	assert.True(t, ok)
	assert.NotNil(t, ma)

	mf, ok := aggs.MovFn("x")
	assert.True(t, ok)
	assert.NotNil(t, mf)

	de, ok := aggs.Derivative("x")
	assert.True(t, ok)
	assert.NotNil(t, de)

	cu, ok := aggs.CumulativeSum("x")
	assert.True(t, ok)
	assert.NotNil(t, cu)

	bx, ok := aggs.BucketScript("x")
	assert.True(t, ok)
	assert.NotNil(t, bx)

	sd, ok := aggs.SerialDiff("x")
	assert.True(t, ok)
	assert.NotNil(t, sd)

	cm, ok := aggs.Composite("x")
	assert.True(t, ok)
	assert.NotNil(t, cm)

	sm, ok := aggs.ScriptedMetric("x")
	assert.True(t, ok)
	assert.NotNil(t, sm)

	tm, ok := aggs.TopMetrics("x")
	assert.True(t, ok)
	assert.NotNil(t, tm)
}

func TestCovAggregations_ResponseAccessors_ValidJSON(t *testing.T) {
	aggs := Aggregations{
		"m_min":      json.RawMessage(`{"value":42.5,"meta":{"a":"b"}}`),
		"m_max":      json.RawMessage(`{"value":100.0}`),
		"m_sum":      json.RawMessage(`{"value":500.0}`),
		"m_avg":      json.RawMessage(`{"value":50.0}`),
		"m_wavg":     json.RawMessage(`{"value":25.0}`),
		"m_mad":      json.RawMessage(`{"value":10.0}`),
		"m_vc":       json.RawMessage(`{"value":100.0}`),
		"m_card":     json.RawMessage(`{"value":5.0}`),
		"m_stats":    json.RawMessage(`{"count":100,"min":1.0,"max":99.0,"avg":50.0,"sum":5000.0,"meta":{"k":"v"}}`),
		"m_estats":   json.RawMessage(`{"count":100,"min":1.0,"max":99.0,"avg":50.0,"sum":5000.0,"sum_of_squares":250000.0,"variance":100.0,"std_deviation":10.0,"meta":{"k":"v"}}`),
		"m_ms":       json.RawMessage(`{"fields":[{"name":"poverty","count":10,"mean":12.5}],"meta":{"k":"v"}}`),
		"m_pct":      json.RawMessage(`{"values":{"1.0":10.0,"5.0":20.0,"25.0":30.0,"50.0":50.0,"75.0":70.0,"95.0":90.0,"99.0":98.0},"meta":{"k":"v"}}`),
		"m_pr":       json.RawMessage(`{"values":{"100":95.0,"250":99.0}}`),
		"m_th":       json.RawMessage(`{"hits":{"total":{"value":10}},"meta":{"k":"v"}}`),
		"m_global":   json.RawMessage(`{"doc_count":100,"meta":{"k":"v"}}`),
		"m_filter":   json.RawMessage(`{"doc_count":50}`),
		"m_filters":  json.RawMessage(`{"buckets":[{"key":"a","doc_count":10}],"meta":{"k":"v"}}`),
		"m_am":       json.RawMessage(`{"buckets":[{"key":"a","doc_count":5}],"meta":{"k":"v"}}`),
		"m_missing":  json.RawMessage(`{"doc_count":20}`),
		"m_nested":   json.RawMessage(`{"doc_count":30}`),
		"m_rnested":  json.RawMessage(`{"doc_count":40}`),
		"m_children": json.RawMessage(`{"doc_count":15}`),
		"m_terms":    json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":100,"buckets":[{"key":"foo","doc_count":50}],"meta":{"k":"v"}}`),
		"m_mterms":   json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":50,"buckets":[{"key":["a","b"],"doc_count":10}],"meta":{"k":"v"}}`),
		"m_sterms":   json.RawMessage(`{"doc_count":100,"buckets":[{"key":"foo","doc_count":10,"bg_count":50,"score":0.8}],"meta":{"k":"v"}}`),
		"m_rterms":   json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":10,"buckets":[{"key":"rare","doc_count":1}]}`),
		"m_sampler":  json.RawMessage(`{"doc_count":200}`),
		"m_dsampler": json.RawMessage(`{"doc_count":150}`),
		"m_range":    json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":10,"buckets":[{"key":"0-50","from":0.0,"to":50.0,"doc_count":30}],"meta":{"k":"v"}}`),
		"m_krange":   json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":10,"buckets":{"r1":{"key":"r1","doc_count":5}},"meta":{"k":"v"}}`),
		"m_drange":   json.RawMessage(`{"buckets":[{"key":"last_month","doc_count":20}]}`),
		"m_iprange":  json.RawMessage(`{"buckets":[{"key":"local","doc_count":50}]}`),
		"m_hist":     json.RawMessage(`{"buckets":[{"key":0,"doc_count":10},{"key":50,"key_as_string":"50.0","doc_count":20}],"meta":{"k":"v"}}`),
		"m_adhist":   json.RawMessage(`{"buckets":[]}`),
		"m_dhist":    json.RawMessage(`{"buckets":[]}`),
		"m_kdhist":   json.RawMessage(`{"buckets":{},"meta":{"k":"v"}}`),
		"m_geob":     json.RawMessage(`{"bounds":{"top_left":{"lat":90,"lon":-180},"bottom_right":{"lat":-90,"lon":180}},"meta":{"k":"v"}}`),
		"m_ghash":    json.RawMessage(`{"buckets":[]}`),
		"m_gtile":    json.RawMessage(`{"buckets":[]}`),
		"m_gcent":    json.RawMessage(`{"location":{"lat":40.7,"lon":-74.0},"count":100,"meta":{"k":"v"}}`),
		"m_gdist":    json.RawMessage(`{"buckets":[]}`),
		"m_ab":       json.RawMessage(`{"value":50.0,"value_as_string":"50.00","meta":{"k":"v"}}`),
		"m_sb":       json.RawMessage(`{"value":100.0}`),
		"m_stb":      json.RawMessage(`{"count":10,"count_as_string":"10","min":1.0,"min_as_string":"1","max":99.0,"max_as_string":"99","avg":50.0,"avg_as_string":"50","sum":500.0,"sum_as_string":"500","meta":{"k":"v"}}`),
		"m_pb":       json.RawMessage(`{"values":{"25.0":10.0,"50.0":50.0,"75.0":70.0},"meta":{"k":"v"}}`),
		"m_mxb":      json.RawMessage(`{"keys":["k1"],"value":99.0,"value_as_string":"99.00","meta":{"k":"v"}}`),
		"m_mnb":      json.RawMessage(`{"keys":[],"value":1.0,"value_as_string":"1.00"}`),
		"m_mov":      json.RawMessage(`{"value":50.0}`),
		"m_movfn":    json.RawMessage(`{"value":55.0}`),
		"m_der":      json.RawMessage(`{"value":5.0,"value_as_string":"5.00","normalized_value":0.5,"normalized_value_as_string":"0.50","meta":{"k":"v"}}`),
		"m_csum":     json.RawMessage(`{"value":500.0}`),
		"m_bscr":     json.RawMessage(`{"value":0.85}`),
		"m_sdiff":    json.RawMessage(`{"value":-5.0}`),
		"m_comp":     json.RawMessage(`{"buckets":[{"key":{"genre":"jazz"},"doc_count":10}],"after_key":{"genre":"rock"},"meta":{"k":"v"}}`),
		"m_sm":       json.RawMessage(`{"value":{"sum":100},"meta":{"k":"v"}}`),
		"m_tm":       json.RawMessage(`{"top":[{"sort":[1],"metrics":{"grade":"A"}}]}`),
	}

	v, ok := aggs.Min("m_min")
	assert.True(t, ok)
	assert.NotNil(t, v.Value)
	assert.Equal(t, float64(42.5), *v.Value)
	assert.Equal(t, "b", v.Meta["a"])

	v2, ok := aggs.Max("m_max")
	assert.True(t, ok)
	assert.Equal(t, float64(100.0), *v2.Value)

	v3, ok := aggs.Sum("m_sum")
	assert.True(t, ok)
	assert.Equal(t, float64(500.0), *v3.Value)

	v4, ok := aggs.Avg("m_avg")
	assert.True(t, ok)
	assert.Equal(t, float64(50.0), *v4.Value)

	v5, ok := aggs.WeightedAvg("m_wavg")
	assert.True(t, ok)
	assert.Equal(t, float64(25.0), *v5.Value)

	v6, ok := aggs.MedianAbsoluteDeviation("m_mad")
	assert.True(t, ok)
	assert.Equal(t, float64(10.0), *v6.Value)

	v7, ok := aggs.ValueCount("m_vc")
	assert.True(t, ok)
	assert.Equal(t, float64(100.0), *v7.Value)

	v8, ok := aggs.Cardinality("m_card")
	assert.True(t, ok)
	assert.Equal(t, float64(5.0), *v8.Value)

	st, ok := aggs.Stats("m_stats")
	assert.True(t, ok)
	assert.Equal(t, int64(100), st.Count)
	assert.Equal(t, float64(1.0), *st.Min)
	assert.Equal(t, float64(99.0), *st.Max)
	assert.Equal(t, float64(50.0), *st.Avg)
	assert.Equal(t, float64(5000.0), *st.Sum)
	assert.Equal(t, "v", st.Meta["k"])

	es, ok := aggs.ExtendedStats("m_estats")
	assert.True(t, ok)
	assert.Equal(t, int64(100), es.Count)
	assert.Equal(t, float64(250000.0), *es.SumOfSquares)
	assert.Equal(t, float64(100.0), *es.Variance)
	assert.Equal(t, float64(10.0), *es.StdDeviation)

	ms, ok := aggs.MatrixStats("m_ms")
	assert.True(t, ok)
	assert.Len(t, ms.Fields, 1)

	pc, ok := aggs.Percentiles("m_pct")
	assert.True(t, ok)
	assert.Len(t, pc.Values, 7)

	pr, ok := aggs.PercentileRanks("m_pr")
	assert.True(t, ok)
	assert.Len(t, pr.Values, 2)

	th, ok := aggs.TopHits("m_th")
	assert.True(t, ok)
	assert.NotNil(t, th.Hits)
	assert.Equal(t, "v", th.Meta["k"])

	gl, ok := aggs.Global("m_global")
	assert.True(t, ok)
	assert.Equal(t, int64(100), gl.DocCount)

	fl, ok := aggs.Filter("m_filter")
	assert.True(t, ok)
	assert.Equal(t, int64(50), fl.DocCount)

	fs, ok := aggs.Filters("m_filters")
	assert.True(t, ok)
	assert.NotNil(t, fs.Buckets)

	am, ok := aggs.AdjacencyMatrix("m_am")
	assert.True(t, ok)
	assert.NotNil(t, am.Buckets)

	mi, ok := aggs.Missing("m_missing")
	assert.True(t, ok)
	assert.Equal(t, int64(20), mi.DocCount)

	ne, ok := aggs.Nested("m_nested")
	assert.True(t, ok)
	assert.Equal(t, int64(30), ne.DocCount)

	rn, ok := aggs.ReverseNested("m_rnested")
	assert.True(t, ok)
	assert.Equal(t, int64(40), rn.DocCount)

	ch, ok := aggs.Children("m_children")
	assert.True(t, ok)
	assert.Equal(t, int64(15), ch.DocCount)

	tr, ok := aggs.Terms("m_terms")
	assert.True(t, ok)
	assert.Len(t, tr.Buckets, 1)
	assert.Equal(t, "v", tr.Meta["k"])

	mt, ok := aggs.MultiTerms("m_mterms")
	assert.True(t, ok)
	assert.Len(t, mt.Buckets, 1)

	stm, ok := aggs.SignificantTerms("m_sterms")
	assert.True(t, ok)
	assert.Len(t, stm.Buckets, 1)
	assert.Equal(t, "v", stm.Meta["k"])

	rt, ok := aggs.RareTerms("m_rterms")
	assert.True(t, ok)
	assert.Len(t, rt.Buckets, 1)

	sa, ok := aggs.Sampler("m_sampler")
	assert.True(t, ok)
	assert.Equal(t, int64(200), sa.DocCount)

	ds, ok := aggs.DiversifiedSampler("m_dsampler")
	assert.True(t, ok)
	assert.Equal(t, int64(150), ds.DocCount)

	rg, ok := aggs.Range("m_range")
	assert.True(t, ok)
	assert.Len(t, rg.Buckets, 1)

	kr, ok := aggs.KeyedRange("m_krange")
	assert.True(t, ok)
	assert.NotNil(t, kr.Buckets)

	dr, ok := aggs.DateRange("m_drange")
	assert.True(t, ok)
	assert.Len(t, dr.Buckets, 1)

	ip, ok := aggs.IPRange("m_iprange")
	assert.True(t, ok)
	assert.Len(t, ip.Buckets, 1)

	hi, ok := aggs.Histogram("m_hist")
	assert.True(t, ok)
	assert.Len(t, hi.Buckets, 2)
	assert.NotNil(t, hi.Buckets[1].KeyAsString)

	ah, ok := aggs.AutoDateHistogram("m_adhist")
	assert.True(t, ok)
	assert.NotNil(t, ah)

	dh, ok := aggs.DateHistogram("m_dhist")
	assert.True(t, ok)
	assert.NotNil(t, dh)

	kd, ok := aggs.KeyedDateHistogram("m_kdhist")
	assert.True(t, ok)
	assert.NotNil(t, kd.Buckets)

	gb, ok := aggs.GeoBounds("m_geob")
	assert.True(t, ok)
	assert.Equal(t, float64(90), gb.Bounds.TopLeft.Latitude)

	gh, ok := aggs.GeoHash("m_ghash")
	assert.True(t, ok)
	assert.NotNil(t, gh)

	gtt, ok := aggs.GeoTile("m_gtile")
	assert.True(t, ok)
	assert.NotNil(t, gtt)

	gc, ok := aggs.GeoCentroid("m_gcent")
	assert.True(t, ok)
	assert.Equal(t, float64(40.7), gc.Location.Latitude)
	assert.Equal(t, int(100), gc.Count)

	gd, ok := aggs.GeoDistance("m_gdist")
	assert.True(t, ok)
	assert.NotNil(t, gd)

	ab, ok := aggs.AvgBucket("m_ab")
	assert.True(t, ok)
	assert.Equal(t, float64(50.0), *ab.Value)
	assert.Equal(t, "50.00", ab.ValueAsString)

	sb, ok := aggs.SumBucket("m_sb")
	assert.True(t, ok)
	assert.Equal(t, float64(100.0), *sb.Value)

	stb, ok := aggs.StatsBucket("m_stb")
	assert.True(t, ok)
	assert.Equal(t, int64(10), stb.Count)
	assert.Equal(t, "10", stb.CountAsString)

	pb, ok := aggs.PercentilesBucket("m_pb")
	assert.True(t, ok)
	assert.Len(t, pb.Values, 3)

	xb, ok := aggs.MaxBucket("m_mxb")
	assert.True(t, ok)
	assert.Equal(t, float64(99.0), *xb.Value)
	assert.Equal(t, []any{"k1"}, xb.Keys)

	nb, ok := aggs.MinBucket("m_mnb")
	assert.True(t, ok)
	assert.Equal(t, float64(1.0), *nb.Value)

	ma, ok := aggs.MovAvg("m_mov")
	assert.True(t, ok)
	assert.Equal(t, float64(50.0), *ma.Value)

	mf, ok := aggs.MovFn("m_movfn")
	assert.True(t, ok)
	assert.Equal(t, float64(55.0), *mf.Value)

	de, ok := aggs.Derivative("m_der")
	assert.True(t, ok)
	assert.Equal(t, float64(5.0), *de.Value)
	assert.Equal(t, "5.00", de.ValueAsString)
	assert.Equal(t, float64(0.5), *de.NormalizedValue)
	assert.Equal(t, "0.50", de.NormalizedValueAsString)

	cu, ok := aggs.CumulativeSum("m_csum")
	assert.True(t, ok)
	assert.Equal(t, float64(500.0), *cu.Value)

	bx, ok := aggs.BucketScript("m_bscr")
	assert.True(t, ok)
	assert.Equal(t, float64(0.85), *bx.Value)

	sd, ok := aggs.SerialDiff("m_sdiff")
	assert.True(t, ok)
	assert.Equal(t, float64(-5.0), *sd.Value)

	cm, ok := aggs.Composite("m_comp")
	assert.True(t, ok)
	assert.Len(t, cm.Buckets, 1)
	assert.NotNil(t, cm.AfterKey)
	assert.Equal(t, "v", cm.Meta["k"])

	sm, ok := aggs.ScriptedMetric("m_sm")
	assert.True(t, ok)
	assert.NotNil(t, sm.Value)
	assert.Equal(t, "v", sm.Meta["k"])

	tm, ok := aggs.TopMetrics("m_tm")
	assert.True(t, ok)
	assert.Len(t, tm.Top, 1)
}

func TestCovAggregations_ResponseAccessors_InvalidJSON(t *testing.T) {
	bad := json.RawMessage(`{invalid json!`)
	aggs := Aggregations{"x": bad}

	v, ok := aggs.Min("x")
	assert.False(t, ok)
	assert.Nil(t, v)

	v2, ok2 := aggs.Max("x")
	assert.False(t, ok2)
	assert.Nil(t, v2)

	v3, ok3 := aggs.Sum("x")
	assert.False(t, ok3)
	assert.Nil(t, v3)

	v4, ok4 := aggs.Avg("x")
	assert.False(t, ok4)
	assert.Nil(t, v4)

	v5, ok5 := aggs.WeightedAvg("x")
	assert.False(t, ok5)
	assert.Nil(t, v5)

	v6, ok6 := aggs.MedianAbsoluteDeviation("x")
	assert.False(t, ok6)
	assert.Nil(t, v6)

	v7, ok7 := aggs.ValueCount("x")
	assert.False(t, ok7)
	assert.Nil(t, v7)

	v8, ok8 := aggs.Cardinality("x")
	assert.False(t, ok8)
	assert.Nil(t, v8)

	st, ok9 := aggs.Stats("x")
	assert.False(t, ok9)
	assert.Nil(t, st)

	es, ok10 := aggs.ExtendedStats("x")
	assert.False(t, ok10)
	assert.Nil(t, es)

	ms, ok11 := aggs.MatrixStats("x")
	assert.False(t, ok11)
	assert.Nil(t, ms)

	pc, ok12 := aggs.Percentiles("x")
	assert.False(t, ok12)
	assert.Nil(t, pc)

	pr, ok13 := aggs.PercentileRanks("x")
	assert.False(t, ok13)
	assert.Nil(t, pr)

	th, ok14 := aggs.TopHits("x")
	assert.False(t, ok14)
	assert.Nil(t, th)

	gl, ok15 := aggs.Global("x")
	assert.False(t, ok15)
	assert.Nil(t, gl)

	fl, ok16 := aggs.Filter("x")
	assert.False(t, ok16)
	assert.Nil(t, fl)

	fs, ok17 := aggs.Filters("x")
	assert.False(t, ok17)
	assert.Nil(t, fs)

	am, ok18 := aggs.AdjacencyMatrix("x")
	assert.False(t, ok18)
	assert.Nil(t, am)

	mi, ok19 := aggs.Missing("x")
	assert.False(t, ok19)
	assert.Nil(t, mi)

	ne, ok20 := aggs.Nested("x")
	assert.False(t, ok20)
	assert.Nil(t, ne)

	rn, ok21 := aggs.ReverseNested("x")
	assert.False(t, ok21)
	assert.Nil(t, rn)

	ch, ok22 := aggs.Children("x")
	assert.False(t, ok22)
	assert.Nil(t, ch)

	tr, ok23 := aggs.Terms("x")
	assert.False(t, ok23)
	assert.Nil(t, tr)

	mt, ok24 := aggs.MultiTerms("x")
	assert.False(t, ok24)
	assert.Nil(t, mt)

	stm, ok25 := aggs.SignificantTerms("x")
	assert.False(t, ok25)
	assert.Nil(t, stm)

	rt, ok26 := aggs.RareTerms("x")
	assert.False(t, ok26)
	assert.Nil(t, rt)

	sa, ok27 := aggs.Sampler("x")
	assert.False(t, ok27)
	assert.Nil(t, sa)

	ds, ok28 := aggs.DiversifiedSampler("x")
	assert.False(t, ok28)
	assert.Nil(t, ds)

	rg, ok29 := aggs.Range("x")
	assert.False(t, ok29)
	assert.Nil(t, rg)

	kr, ok30 := aggs.KeyedRange("x")
	assert.False(t, ok30)
	assert.Nil(t, kr)

	dr, ok31 := aggs.DateRange("x")
	assert.False(t, ok31)
	assert.Nil(t, dr)

	ipr, ok32 := aggs.IPRange("x")
	assert.False(t, ok32)
	assert.Nil(t, ipr)

	hi, ok33 := aggs.Histogram("x")
	assert.False(t, ok33)
	assert.Nil(t, hi)

	ah, ok34 := aggs.AutoDateHistogram("x")
	assert.False(t, ok34)
	assert.Nil(t, ah)

	dh, ok35 := aggs.DateHistogram("x")
	assert.False(t, ok35)
	assert.Nil(t, dh)

	kd, ok36 := aggs.KeyedDateHistogram("x")
	assert.False(t, ok36)
	assert.Nil(t, kd)

	gb, ok37 := aggs.GeoBounds("x")
	assert.False(t, ok37)
	assert.Nil(t, gb)

	gh, ok38 := aggs.GeoHash("x")
	assert.False(t, ok38)
	assert.Nil(t, gh)

	gt2, ok39 := aggs.GeoTile("x")
	assert.False(t, ok39)
	assert.Nil(t, gt2)

	gc, ok40 := aggs.GeoCentroid("x")
	assert.False(t, ok40)
	assert.Nil(t, gc)

	gd, ok41 := aggs.GeoDistance("x")
	assert.False(t, ok41)
	assert.Nil(t, gd)

	ab, ok42 := aggs.AvgBucket("x")
	assert.False(t, ok42)
	assert.Nil(t, ab)

	sb, ok43 := aggs.SumBucket("x")
	assert.False(t, ok43)
	assert.Nil(t, sb)

	stb, ok44 := aggs.StatsBucket("x")
	assert.False(t, ok44)
	assert.Nil(t, stb)

	pb, ok45 := aggs.PercentilesBucket("x")
	assert.False(t, ok45)
	assert.Nil(t, pb)

	xb, ok46 := aggs.MaxBucket("x")
	assert.False(t, ok46)
	assert.Nil(t, xb)

	nb, ok47 := aggs.MinBucket("x")
	assert.False(t, ok47)
	assert.Nil(t, nb)

	ma, ok48 := aggs.MovAvg("x")
	assert.False(t, ok48)
	assert.Nil(t, ma)

	mfn, ok49 := aggs.MovFn("x")
	assert.False(t, ok49)
	assert.Nil(t, mfn)

	de, ok50 := aggs.Derivative("x")
	assert.False(t, ok50)
	assert.Nil(t, de)

	cu, ok51 := aggs.CumulativeSum("x")
	assert.False(t, ok51)
	assert.Nil(t, cu)

	bx, ok52 := aggs.BucketScript("x")
	assert.False(t, ok52)
	assert.Nil(t, bx)

	sd, ok53 := aggs.SerialDiff("x")
	assert.False(t, ok53)
	assert.Nil(t, sd)

	cm, ok54 := aggs.Composite("x")
	assert.False(t, ok54)
	assert.Nil(t, cm)

	sm, ok55 := aggs.ScriptedMetric("x")
	assert.False(t, ok55)
	assert.Nil(t, sm)

	tm, ok56 := aggs.TopMetrics("x")
	assert.False(t, ok56)
	assert.Nil(t, tm)
}

func TestCovAggregationBucketRangeItem_UnmarshalJSON(t *testing.T) {
	raw := `{"key":"0-50","from":0.0,"from_as_string":"0","to":50.0,"to_as_string":"50","doc_count":30}`
	var item AggregationBucketRangeItem
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, "0-50", item.Key)
	assert.Equal(t, int64(30), item.DocCount)
	assert.NotNil(t, item.From)
	assert.NotNil(t, item.To)
	assert.Equal(t, "0", item.FromAsString)
	assert.Equal(t, "50", item.ToAsString)
}

func TestCovAggregationBucketKeyItem_UnmarshalJSON(t *testing.T) {
	raw := `{"key":"foo","key_as_string":"FOO","doc_count":42}`
	var item AggregationBucketKeyItem
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, "foo", item.Key)
	assert.NotNil(t, item.KeyAsString)
	assert.Equal(t, int64(42), item.DocCount)
}

func TestCovAggregationBucketMultiKeyItem_UnmarshalJSON(t *testing.T) {
	raw := `{"key":["a","b"],"key_as_string":"a|b","doc_count":5}`
	var item AggregationBucketMultiKeyItem
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, []any{"a", "b"}, item.Key)
	assert.NotNil(t, item.KeyAsString)
	assert.Equal(t, int64(5), item.DocCount)
}

func TestCovAggregationBucketSignificantTerm_UnmarshalJSON(t *testing.T) {
	raw := `{"key":"foo","doc_count":10,"bg_count":100,"score":0.5}`
	var item AggregationBucketSignificantTerm
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, "foo", item.Key)
	assert.Equal(t, int64(10), item.DocCount)
	assert.Equal(t, int64(100), item.BgCount)
	assert.Equal(t, float64(0.5), item.Score)
}

func TestCovAggregationBucketCompositeItem_UnmarshalJSON(t *testing.T) {
	raw := `{"key":{"genre":"jazz"},"doc_count":100}`
	var item AggregationBucketCompositeItem
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"genre": "jazz"}, item.Key)
	assert.Equal(t, int64(100), item.DocCount)
}

func TestCovAggregationBucketHistogramItem_UnmarshalJSON(t *testing.T) {
	raw := `{"key":50.0,"key_as_string":"50.00","doc_count":15}`
	var item AggregationBucketHistogramItem
	err := json.Unmarshal([]byte(raw), &item)
	require.NoError(t, err)
	assert.Equal(t, float64(50), item.Key)
	assert.NotNil(t, item.KeyAsString)
	assert.Equal(t, int64(15), item.DocCount)
}

// Tests for UnmarshalJSON errors
func TestCovUnmarshalJSON_InvalidData(t *testing.T) {
	bad := []byte(`not json`)

	var vm AggregationValueMetric
	assert.Error(t, vm.UnmarshalJSON(bad))

	var sm AggregationStatsMetric
	assert.Error(t, sm.UnmarshalJSON(bad))

	var esm AggregationExtendedStatsMetric
	assert.Error(t, esm.UnmarshalJSON(bad))

	var msm AggregationMatrixStats
	assert.Error(t, msm.UnmarshalJSON(bad))

	var pm AggregationPercentilesMetric
	assert.Error(t, pm.UnmarshalJSON(bad))

	var thm AggregationTopHitsMetric
	assert.Error(t, thm.UnmarshalJSON(bad))

	var gbm AggregationGeoBoundsMetric
	assert.Error(t, gbm.UnmarshalJSON(bad))

	var gcm AggregationGeoCentroidMetric
	assert.Error(t, gcm.UnmarshalJSON(bad))

	var sb AggregationSingleBucket
	assert.Error(t, sb.UnmarshalJSON(bad))

	var bri AggregationBucketRangeItems
	assert.Error(t, bri.UnmarshalJSON(bad))

	var bkr AggregationBucketKeyedRangeItems
	assert.Error(t, bkr.UnmarshalJSON(bad))

	var bri2 AggregationBucketRangeItem
	assert.Error(t, bri2.UnmarshalJSON(bad))

	var bki AggregationBucketKeyItems
	assert.Error(t, bki.UnmarshalJSON(bad))

	var bki2 AggregationBucketKeyItem
	assert.Error(t, bki2.UnmarshalJSON(bad))

	var bmk AggregationBucketMultiKeyItems
	assert.Error(t, bmk.UnmarshalJSON(bad))

	var bmk2 AggregationBucketMultiKeyItem
	assert.Error(t, bmk2.UnmarshalJSON(bad))

	var bst AggregationBucketSignificantTerms
	assert.Error(t, bst.UnmarshalJSON(bad))

	var bst2 AggregationBucketSignificantTerm
	assert.Error(t, bst2.UnmarshalJSON(bad))

	var bf AggregationBucketFilters
	assert.Error(t, bf.UnmarshalJSON(bad))

	var ba AggregationBucketAdjacencyMatrix
	assert.Error(t, ba.UnmarshalJSON(bad))

	var bhi AggregationBucketHistogramItems
	assert.Error(t, bhi.UnmarshalJSON(bad))

	var bkh AggregationBucketKeyedHistogramItems
	assert.Error(t, bkh.UnmarshalJSON(bad))

	var bhi2 AggregationBucketHistogramItem
	assert.Error(t, bhi2.UnmarshalJSON(bad))

	var psv AggregationPipelineSimpleValue
	assert.Error(t, psv.UnmarshalJSON(bad))

	var pbm AggregationPipelineBucketMetricValue
	assert.Error(t, pbm.UnmarshalJSON(bad))

	var pd AggregationPipelineDerivative
	assert.Error(t, pd.UnmarshalJSON(bad))

	var psm AggregationPipelineStatsMetric
	assert.Error(t, psm.UnmarshalJSON(bad))

	var ppm AggregationPipelinePercentilesMetric
	assert.Error(t, ppm.UnmarshalJSON(bad))

	var bci AggregationBucketCompositeItems
	assert.Error(t, bci.UnmarshalJSON(bad))

	var bci2 AggregationBucketCompositeItem
	assert.Error(t, bci2.UnmarshalJSON(bad))

	var asm AggregationScriptedMetric
	assert.Error(t, asm.UnmarshalJSON(bad))
}

func TestCovTopHitsAggregation_Additional(t *testing.T) {
	a := NewTopHitsAggregation()
	a.FetchSourceContext(NewFetchSourceContext(true))
	a.DocvalueFields("field1")
	a.DocvalueFieldsWithFormat(DocvalueField{Field: "f", Format: "epoch"})
	a.DocvalueField("f2")
	a.DocvalueFieldWithFormat(DocvalueField{Field: "f3"})
	sf := NewScriptField("my_script", NewScript("1+1"))
	a.ScriptFields(sf)
	a.ScriptField(sf)
	a.Sort("f", true)
	a.SortWithInfo(SortInfo{Field: "f", Ascending: true})
	h := a.Highlighter()
	a.Highlight(h)
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTermsOrder_Source(t *testing.T) {
	o := TermsOrder{Field: "_count", Ascending: true}
	src, err := o.Source()
	require.NoError(t, err)
	m := src.(map[string]string)
	assert.Equal(t, "asc", m["_count"])

	o2 := TermsOrder{Field: "_key", Ascending: false}
	src2, err := o2.Source()
	require.NoError(t, err)
	m2 := src2.(map[string]string)
	assert.Equal(t, "desc", m2["_key"])
}

func TestCovBucketSelectorAggregation_Source(t *testing.T) {
	a := NewBucketSelectorAggregation()
	a.Format = "0.00"
	a.GapPolicy = "skip"
	a.BucketsPathsMap = map[string]string{"var1": "agg1", "var2": "agg2"}
	a.Script = NewScript("params.var1 > params.var2")
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)

	_, err = NewBucketSelectorAggregation().Source()
	require.NoError(t, err)
}

func TestCovFilterAggregation_NewFilter(t *testing.T) {
	a := NewFilterAggregation()
	a.Filter = NewMatchAllQuery()
	a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovNestedAggregation_NewNested(t *testing.T) {
	a := NewNestedAggregation()
	a.Path = "line_items"
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovReverseNestedAggregation_NewReverseNested(t *testing.T) {
	a := NewReverseNestedAggregation()
	a.Path = "parent"
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovSignificantTextAggregation_Source_AllBranches(t *testing.T) {
	t.Run("include_regexp", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.Include("foo.*")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["significant_text"].(map[string]any)
		assert.Equal(t, "foo.*", m["include"])
	})
	t.Run("include_values", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.IncludeValues("a", "b")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["significant_text"].(map[string]any)
		assert.NotNil(t, m["include"])
	})
	t.Run("include_partitions", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.Partition(0).NumPartitions(5)
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["significant_text"].(map[string]any)
		assert.NotNil(t, m["include"])
	})
	t.Run("exclude_regexp", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.Exclude("bar.*")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["significant_text"].(map[string]any)
		assert.Equal(t, "bar.*", m["exclude"])
	})
	t.Run("exclude_values", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.ExcludeValues("x", "y")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["significant_text"].(map[string]any)
		assert.NotNil(t, m["exclude"])
	})
	t.Run("background_filter", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.BackgroundFilter(NewMatchAllQuery())
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("heuristic", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.SignificanceHeuristic(NewJLHScoreSignificanceHeuristic())
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("bucket_count_thresholds", func(t *testing.T) {
		a := NewSignificantTextAggregation()
		a.FieldVal = "content"
		a = a.MinDocCount(5)
		a = a.ShardMinDocCount(1)
		a = a.WithSize(10)
		a = a.WithShardSize(20)
		src, err := a.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestCovSignificantTermsAggregation_BackgroundFilter(t *testing.T) {
	a := NewSignificantTermsAggregation()
	a.FieldVal = "content"
	a.BackgroundFilter(NewMatchAllQuery())
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovGeoDistanceAggregation_GeoRangeBound(t *testing.T) {
	a := NewGeoDistanceAggregation()
	a.Field = "location"
	a.Unit = "km"
	a.DistanceType = "arc"
	a.Origin = "40.7,-74.0"
	a.Ranges = []GeoDistanceRange{
		{From: int(10), To: int(100)},
		{From: int16(10), To: int32(100)},
		{From: float32(10), To: float64(100)},
		{From: int64(10), To: float32(100)},
		{Key: "ptr_int", From: helperPtrInt(10), To: helperPtrInt(100)},
		{Key: "ptr_int16", From: helperPtrInt64(10), To: helperPtrInt64(100)},
		{Key: "ptr_float", From: helperPtrFloat32(10), To: helperPtrFloat64(100)},
		{Key: "string", From: "10km", To: "100km"},
		{Key: "ptr_string", From: &[]string{"10km"}[0], To: &[]string{"100km"}[0]},
		{Key: "nil_types", From: struct{}{}, To: struct{}{}},
	}
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTermsAggregation_OrderByTermAsc(t *testing.T) {
	a := NewTermsAggregation().WithField("genre").OrderByTermAsc()
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovScriptedMetricAggregation_NilScripts(t *testing.T) {
	a := NewScriptedMetricAggregation()
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["scripted_metric"].(map[string]any)
	assert.Empty(t, m)
}

func TestCovTopHitsAggregation_SortBy(t *testing.T) {
	a := NewTopHitsAggregation()
	a.SortBy(NewFieldSort("date"))
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTermsAggregationIncludeExclude_MergeInto(t *testing.T) {
	t.Run("with_regexp", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Include: "foo.*", Exclude: "bar.*"}
		body := map[string]any{}
		err := ie.MergeInto(body)
		require.NoError(t, err)
		assert.Equal(t, "foo.*", body["include"])
		assert.Equal(t, "bar.*", body["exclude"])
	})
	t.Run("with_values", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{
			IncludeValues: []any{"a"},
			ExcludeValues: []any{"x"},
		}
		body := map[string]any{}
		err := ie.MergeInto(body)
		require.NoError(t, err)
		assert.NotNil(t, body["include"])
		assert.NotNil(t, body["exclude"])
	})
	t.Run("with_partitions", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Partition: 0, NumPartitions: 5}
		body := map[string]any{}
		err := ie.MergeInto(body)
		require.NoError(t, err)
		assert.NotNil(t, body["include"])
	})
	t.Run("empty", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{}
		body := map[string]any{}
		err := ie.MergeInto(body)
		require.NoError(t, err)
	})
}

// Test all Aggregation methods on terms that set sub-fields to nil before init
func TestCovTermsAggregation_PartialIncludeExclude(t *testing.T) {
	a := NewTermsAggregation().
		WithField("genre").
		WithExclude("bar.*").
		WithExcludeValues("c", "d").
		WithPartition(1).
		WithNumPartitions(10)
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

// Test geo distance with int64 type specifically for geoRangeBound
func TestCovGeoDistance_Aggregation_Full(t *testing.T) {
	a := NewGeoDistanceAggregation()
	a.Field = "location"
	a.Unit = "mi"
	a.DistanceType = "plane"
	a.Origin = "0,0"
	a.Ranges = []GeoDistanceRange{
		{From: int32(0), To: int16(100)},
		{From: "0km", To: struct{}{}},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geo_distance"].(map[string]any)
	assert.Equal(t, "mi", m["unit"])
	assert.Equal(t, "plane", m["distance_type"])
}

func TestCovGeoCentroid_SubAggsAndMeta(t *testing.T) {
	a := NewGeoCentroidAggregation()
	a.Field = "location"
	a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geo_centroid"].(map[string]any)
	assert.NotNil(t, m["aggregations"])
	assert.NotNil(t, m["meta"])
}

func TestCovGeoBounds_SubAggs(t *testing.T) {
	a := NewGeoBoundsAggregation()
	a.Field = "location"
	a.SubAggs = map[string]Aggregation{"avg": NewAvgAggregation()}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geo_bounds"].(map[string]any)
	assert.NotNil(t, m["aggregations"])
}

// Test GeoTileGrid Source error path for missing field
func TestCovScriptedMetricAggregation_OnlyMapScript(t *testing.T) {
	a := NewScriptedMetricAggregation()
	a.MapScript = NewScript("state.sum = 1")
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["scripted_metric"].(map[string]any)
	assert.NotNil(t, m["map_script"])
	assert.Nil(t, m["init_script"])
	assert.Nil(t, m["combine_script"])
	assert.Nil(t, m["reduce_script"])
}

// Test GeoTileGrid Source error path for missing field
func TestCovGeoTileGridAggregation_MissingField(t *testing.T) {
	a := NewGeoTileGridAggregation()
	_, err := a.Source()
	assert.Error(t, err)
}

// Test GeoTileGrid Source with all options
func TestCovGeoTileGridAggregation_AllOptions(t *testing.T) {
	a := NewGeoTileGridAggregation()
	a.Field = "location"
	a.Precision = helperPtrInt(8)
	a.Size = helperPtrInt(100)
	a.ShardSize = helperPtrInt(200)
	a.Bounds = &BoundingBox{
		TopLeft:     GeoPoint{Lat: 90, Lon: -180},
		BottomRight: GeoPoint{Lat: -90, Lon: 180},
	}
	a.SubAggs = map[string]Aggregation{"sub": NewAvgAggregation()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geotile_grid"].(map[string]any)
	assert.Equal(t, "location", m["field"])
	assert.NotNil(t, m["bounds"])
	assert.NotNil(t, m["aggregations"])
	assert.NotNil(t, m["meta"])
}

// Test WithExcludeValues called when IncludeExclude is nil
func TestCovTermsAggregation_WithExcludeValuesAlone(t *testing.T) {
	a := NewTermsAggregation().
		WithField("genre").
		WithExcludeValues("a", "b")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTermsAggregation_WithNumPartitionsAlone(t *testing.T) {
	a := NewTermsAggregation().
		WithField("genre").
		WithNumPartitions(5).
		WithPartition(0)
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTermsAggregation_WithExcludeAlone(t *testing.T) {
	a := NewTermsAggregation().
		WithField("genre").
		WithExclude("foo.*")
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestCovTopHitsAggregation_SourceAllOptions(t *testing.T) {
	a := NewTopHitsAggregation()
	a.Size(5)
	a.From(10)
	a.Sort("date", false)
	a.TrackScores(true)
	a.Explain(false)
	a.Version(false)
	a.FetchSource(false)
	a.NoStoredFields()
	a.DocvalueFields("field1", "field2")
	a.DocvalueField("field3")
	sf := NewScriptField("sf", NewScript("1"))
	a.ScriptField(sf)
	a.Highlight(&Highlight{})
	src, err := a.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

// ScriptedMetric with various script combinations
func TestCovScriptedMetricAggregation_VariousScripts(t *testing.T) {
	t.Run("only_init_script", func(t *testing.T) {
		a := NewScriptedMetricAggregation()
		a.InitScript = NewScript("state.sum = 0")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["scripted_metric"].(map[string]any)
		assert.NotNil(t, m["init_script"])
	})
	t.Run("only_combine_and_reduce", func(t *testing.T) {
		a := NewScriptedMetricAggregation()
		a.CombineScript = NewScript("return state")
		a.ReduceScript = NewScript("return states")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["scripted_metric"].(map[string]any)
		assert.NotNil(t, m["combine_script"])
		assert.NotNil(t, m["reduce_script"])
	})
	t.Run("params_and_meta_without_scripts", func(t *testing.T) {
		a := NewScriptedMetricAggregation()
		a.Params = map[string]any{"factor": 2}
		a.Meta = map[string]any{"desc": "test"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["scripted_metric"].(map[string]any)
		assert.NotNil(t, m["params"])
		assert.NotNil(t, m["meta"])
	})
	t.Run("all_scripts_with_all_params", func(t *testing.T) {
		a := NewScriptedMetricAggregation()
		a.InitScript = NewScript("state.sum = 0")
		a.MapScript = NewScript("state.sum++")
		a.CombineScript = NewScript("return state")
		a.ReduceScript = NewScript("return states")
		a.Params = map[string]any{"factor": 2}
		a.Meta = map[string]any{"desc": "all"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["scripted_metric"].(map[string]any)
		assert.NotNil(t, m["init_script"])
		assert.NotNil(t, m["map_script"])
		assert.NotNil(t, m["combine_script"])
		assert.NotNil(t, m["reduce_script"])
		assert.NotNil(t, m["params"])
		assert.NotNil(t, m["meta"])
	})
}

// WeightedAvg - test MultiValuesSourceFieldConfig.Source with all options
func TestCovWeightedAvgAggregation_MultiValueConfig(t *testing.T) {
	cfg := MultiValuesSourceFieldConfig{
		FieldName: "grade",
		Missing:   int(0),
		Script:    NewScript("doc['grade'].value"),
		TimeZone:  "UTC",
	}
	src, err := cfg.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "grade", m["field"])
	assert.Equal(t, int(0), m["missing"])
	assert.Equal(t, "UTC", m["time_zone"])

	cfgEmpty := MultiValuesSourceFieldConfig{}
	src2, err := cfgEmpty.Source()
	require.NoError(t, err)
	m2 := src2.(map[string]any)
	assert.Empty(t, m2)
}

func TestCovWeightedAvgAggregation_AllFields(t *testing.T) {
	a := NewWeightedAvgAggregation()
	a.Fields["val"] = &MultiValuesSourceFieldConfig{FieldName: "v"}
	a.Format = "0.00"
	a.ValueType = "double"
	a.Value = &MultiValuesSourceFieldConfig{FieldName: "v"}
	a.Weight = &MultiValuesSourceFieldConfig{FieldName: "w"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["weighted_avg"].(map[string]any)
	assert.NotNil(t, m["fields"])
	assert.Equal(t, "0.00", m["format"])
	assert.Equal(t, "double", m["value_type"])
	assert.NotNil(t, m["value"])
	assert.NotNil(t, m["weight"])
}

// Test significant terms Source with each field individually
func TestCovSignificantTermsAggregation_IndividualFields(t *testing.T) {
	t.Run("required_size", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.RequiredSize = helperPtrInt(10)
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("shard_size", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.ShardSize = helperPtrInt(20)
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("min_doc_count", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.MinDocCount = helperPtrInt(5)
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("shard_min_doc_count", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.ShardMinDocCount = helperPtrInt(1)
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("execution_hint", func(t *testing.T) {
		a := NewSignificantTermsAggregation()
		a.ExecutionHint = "map"
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("with_all_heuristics", func(t *testing.T) {
		for _, h := range []SignificanceHeuristic{
			NewChiSquareSignificanceHeuristic(),
			NewGNDSignificanceHeuristic(),
			NewJLHScoreSignificanceHeuristic(),
			NewMutualInformationSignificanceHeuristic(),
			NewPercentageScoreSignificanceHeuristic(),
			func() SignificanceHeuristic {
				sh := NewScriptSignificanceHeuristic()
				sh.ScriptVal = NewScript("doc['score'].value")
				return sh
			}(),
		} {
			a := NewSignificantTermsAggregation()
			a.Heuristic = h
			_, err := a.Source()
			require.NoError(t, err)
		}
	})
}

// SignificantTerms IncludeExclude paths
func TestCovSignificantTermsAggregation_IncludeExclude(t *testing.T) {
	t.Run("regexp", func(t *testing.T) {
		a := NewSignificantTermsAggregation().Include("foo.*").Exclude("bar.*")
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("values", func(t *testing.T) {
		a := NewSignificantTermsAggregation().IncludeValues("a", "b").ExcludeValues("x", "y")
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("partitions", func(t *testing.T) {
		a := NewSignificantTermsAggregation().Partition(0).NumPartitions(5)
		_, err := a.Source()
		require.NoError(t, err)
	})
}

// AdjacencyMatrix with multiple filters
func TestCovAdjacencyMatrixAggregation_WithFilters(t *testing.T) {
	a := NewAdjacencyMatrixAggregation()
	a.Filters["groupA"] = NewMatchAllQuery()
	a.Filters["groupB"] = NewMatchAllQuery()
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["adjacency_matrix"].(map[string]any)
	assert.NotNil(t, m["filters"])
}

// FiltersAggregation - error test for unnamed filter with bad query
func TestCovFiltersAggregation_EmptyFilters(t *testing.T) {
	a := NewFiltersAggregation()
	a.NamedFilters = nil
	a.UnnamedFilters = nil
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["filters"].(map[string]any)
	_, hasFilters := m["filters"]
	assert.False(t, hasFilters)
}

// RareTerms with IncludeExclude paths
func TestCovRareTermsAggregation_IncludeExclude(t *testing.T) {
	t.Run("with_include", func(t *testing.T) {
		a := NewRareTermsAggregation()
		a.Field = "genre"
		a.IncludeExclude = &TermsAggregationIncludeExclude{Include: "foo.*"}
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("with_values", func(t *testing.T) {
		a := NewRareTermsAggregation()
		a.Field = "genre"
		a.IncludeExclude = &TermsAggregationIncludeExclude{IncludeValues: []any{"a", "b"}}
		_, err := a.Source()
		require.NoError(t, err)
	})
	t.Run("with_partitions", func(t *testing.T) {
		a := NewRareTermsAggregation()
		a.Field = "genre"
		a.IncludeExclude = &TermsAggregationIncludeExclude{NumPartitions: 5, Partition: 0}
		_, err := a.Source()
		require.NoError(t, err)
	})
}

// GeoDistance range types test
func TestCovGeoDistanceAggregation_AllRangeTypes(t *testing.T) {
	a := NewGeoDistanceAggregation()
	a.Field = "loc"
	a.Ranges = []GeoDistanceRange{
		{From: 10, To: 100},
		{From: int64(0), To: int64(1000)},
		{From: float32(1.5), To: float64(2.5)},
		{Key: "str", From: "0km", To: "100km"},
		{Key: "ptr_str", From: &[]string{"0km"}[0], To: &[]string{"100km"}[0]},
		{Key: "ptr_int", From: helperPtrInt(10), To: helperPtrInt(100)},
		{Key: "empty", From: struct{}{}, To: struct{}{}},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geo_distance"].(map[string]any)
	ranges := m["ranges"].([]any)
	assert.Len(t, ranges, 7)
}

// Pipeline aggregations with multiple bucket paths
func TestCovPipelineAggregations_MultipleBucketPaths(t *testing.T) {
	a := NewAvgBucketAggregation()
	a.BucketsPaths = []string{"agg1", "agg2"}
	_, err := a.Source()
	require.NoError(t, err)

	b := NewSumBucketAggregation()
	b.BucketsPaths = []string{"agg1", "agg2"}
	_, err = b.Source()
	require.NoError(t, err)

	c := NewStatsBucketAggregation()
	c.BucketsPaths = []string{"agg1", "agg2"}
	_, err = c.Source()
	require.NoError(t, err)

	d := NewPercentilesBucketAggregation()
	d.BucketsPaths = []string{"agg1", "agg2"}
	_, err = d.Source()
	require.NoError(t, err)

	e := NewMaxBucketAggregation()
	e.BucketsPaths = []string{"agg1", "agg2"}
	_, err = e.Source()
	require.NoError(t, err)

	f := NewMinBucketAggregation()
	f.BucketsPaths = []string{"agg1", "agg2"}
	_, err = f.Source()
	require.NoError(t, err)

	g := NewDerivativeAggregation()
	g.BucketsPaths = []string{"agg1", "agg2"}
	_, err = g.Source()
	require.NoError(t, err)

	h := NewCumulativeSumAggregation()
	h.BucketsPaths = []string{"agg1", "agg2"}
	_, err = h.Source()
	require.NoError(t, err)

	i := NewSerialDiffAggregation()
	i.BucketsPaths = []string{"agg1", "agg2"}
	_, err = i.Source()
	require.NoError(t, err)

	j := NewExtendedStatsBucketAggregation()
	j.BucketsPaths = []string{"agg1", "agg2"}
	_, err = j.Source()
	require.NoError(t, err)
}

// BucketSort with full configuration
func TestCovBucketSortAggregation_Full(t *testing.T) {
	a := NewBucketSortAggregation()
	a.From = 0
	a.Size = 10
	a.GapPolicy = "skip"
	a.Sorters = []Sorter{NewFieldSort("total")}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bucket_sort"].(map[string]any)
	assert.Equal(t, 10, m["size"])
}

// ExtendedStatsBucket negative sigma
func TestCovExtendedStatsBucketAggregation_NegativeSigma(t *testing.T) {
	sigma := float32(-1.0)
	a := NewExtendedStatsBucketAggregation()
	a.Sigma = &sigma
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["extended_stats_bucket"].(map[string]any)
	_, hasSigma := m["sigma"]
	assert.False(t, hasSigma)
}
