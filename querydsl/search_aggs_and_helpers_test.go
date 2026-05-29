// search_aggs_and_helpers_test.go covers aggregation helper
// infrastructure (ScriptedMetric, FilterAggregation, SignificantTerms
// heuristics, IncludeExclude), composite value sources
// (CompositeAggregation), and miscellaneous builder types (InnerHit,
// docvalue_field, field_field, Highlight, Rescore, Collapse, IndexBoost).
package querydsl

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestC3MarshalStructErrors(t *testing.T) {
	_, err := marshalStruct(make(chan int))
	assert.Error(t, err)

	_, err = marshalStruct([]int{1, 2, 3})
	assert.Error(t, err)
}

func TestC3SourcePipelineWithScript(t *testing.T) {
	body := map[string]any{"value": 1}
	script := NewScript("doc['price'].value * 2").Lang("painless").Param("factor", 1.1)
	src, err := sourcePipeline("avg", body, nil, script)
	require.NoError(t, err)
	m := src.(map[string]any)["avg"].(map[string]any)
	assert.Contains(t, m, "script")
}

func TestC3SourcePipelineNilScript(t *testing.T) {
	body := map[string]any{"value": 1}
	src, err := sourcePipeline("avg", body, nil, nil)
	require.NoError(t, err)
	m := src.(map[string]any)["avg"].(map[string]any)
	assert.Nil(t, m["script"])
}

func TestC3SourceAggSubErr(t *testing.T) {
	body := map[string]any{}
	subs := map[string]Aggregation{"bad": mockAggregationError{}}
	_, err := sourceAgg("terms", body, subs, nil, nil)
	assert.Error(t, err)
}

func TestC3ScriptedMetricAllScripts(t *testing.T) {
	a := NewScriptedMetricAggregation()
	a.InitScript = NewScript("state.count = 0")
	a.MapScript = NewScript("state.count++")
	a.CombineScript = NewScript("return state.count")
	a.ReduceScript = NewScript("long sum = 0; for (s in states) sum += s.count; return sum")
	a.Params = map[string]any{"key": "val"}
	a.Meta = map[string]any{"meta_key": "meta_val"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["scripted_metric"].(map[string]any)
	assert.Contains(t, m, "init_script")
	assert.Contains(t, m, "map_script")
	assert.Contains(t, m, "combine_script")
	assert.Contains(t, m, "reduce_script")
	assert.Contains(t, m, "params")
	assert.Contains(t, m, "meta")
}

func TestC3FilterAggSubAggsMeta(t *testing.T) {
	a := NewFilterAggregation()
	a.Filter = NewTermQuery("status", "active")
	a.SubAggs = map[string]Aggregation{"avg_price": AvgAggregation{Field: "price"}}
	a.Meta = map[string]any{"key": "val"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["filter"].(map[string]any)
	assert.Contains(t, m, "filter")
	assert.Contains(t, m, "aggregations")
	assert.Contains(t, m, "meta")
}

func TestC3FilterAggFilterErr(t *testing.T) {
	a := FilterAggregation{Filter: mockQueryError{}}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestC3SignificantTermsFullSource(t *testing.T) {
	minDoc := int(5)
	shardMinDoc := int(2)
	reqSize := int(10)
	shardSize := int(100)
	a := NewSignificantTermsAggregation()
	a.FieldVal = "genre"
	a.RequiredSize = &reqSize
	a.ShardSize = &shardSize
	a.MinDocCount = &minDoc
	a.ShardMinDocCount = &shardMinDoc
	a.ExecutionHint = "map"
	a.Filter = NewTermQuery("type", "post")
	a.Heuristic = NewChiSquareSignificanceHeuristic()
	a.IncludeExclude = &TermsAggregationIncludeExclude{
		Include: "music.*",
		Exclude: "old.*",
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["significant_terms"].(map[string]any)
	assert.Equal(t, "genre", m["field"])
	assert.Equal(t, 10, m["size"])
	assert.Equal(t, 100, m["shard_size"])
	assert.Equal(t, 5, m["min_doc_count"])
	assert.Equal(t, 2, m["shard_min_doc_count"])
	assert.Equal(t, "map", m["execution_hint"])
	assert.Contains(t, m, "background_filter")
}

func TestC3SignificantTermsHeuristics(t *testing.T) {
	trueVal := true
	falseVal := false

	bgSupChisq := trueVal
	incNegChisq := falseVal
	chisq := ChiSquareSignificanceHeuristic{BackgroundIsSupersetVal: &bgSupChisq, IncludeNegativesVal: &incNegChisq}
	src, err := chisq.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, true, m["background_is_superset"])
	assert.Equal(t, false, m["include_negatives"])

	bgSupGND := trueVal
	gnd := GNDSignificanceHeuristic{BackgroundIsSupersetVal: &bgSupGND}
	src, err = gnd.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.Equal(t, true, m["background_is_superset"])

	jlh := NewJLHScoreSignificanceHeuristic()
	src, err = jlh.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.NotNil(t, m)

	bgSupMI := falseVal
	incNegMI := trueVal
	mi := MutualInformationSignificanceHeuristic{BackgroundIsSupersetVal: &bgSupMI, IncludeNegativesVal: &incNegMI}
	src, err = mi.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.Equal(t, false, m["background_is_superset"])
	assert.Equal(t, true, m["include_negatives"])

	pct := NewPercentageScoreSignificanceHeuristic()
	src, err = pct.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.NotNil(t, m)

	scriptHeur := ScriptSignificanceHeuristic{ScriptVal: NewScript("return 1")}
	src, err = scriptHeur.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.Contains(t, m, "script")

	nilScriptHeur := NewScriptSignificanceHeuristic()
	src, err = nilScriptHeur.Source()
	require.NoError(t, err)
	m = src.(map[string]any)
	assert.NotContains(t, m, "script")
}

func TestC3SignificantTermsIncludeExcludePaths(t *testing.T) {
	a := NewSignificantTermsAggregation().Include("rock.*").Exclude("old.*")
	a.FieldVal = "genre"
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["significant_terms"].(map[string]any)
	assert.Equal(t, "rock.*", m["include"])
	assert.Equal(t, "old.*", m["exclude"])

	a2 := NewSignificantTermsAggregation().IncludeValues("rock", "jazz")
	a2.FieldVal = "genre"
	src2, err := a2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)["significant_terms"].(map[string]any)
	assert.NotNil(t, m["include"])

	a3 := NewSignificantTermsAggregation().ExcludeValues("classical", "metal")
	a3.FieldVal = "genre"
	src3, err := a3.Source()
	require.NoError(t, err)
	m = src3.(map[string]any)["significant_terms"].(map[string]any)
	assert.NotNil(t, m["exclude"])

	a4 := NewSignificantTermsAggregation().Partition(0).NumPartitions(3)
	a4.FieldVal = "genre"
	src4, err := a4.Source()
	require.NoError(t, err)
	m = src4.(map[string]any)["significant_terms"].(map[string]any)
	inc := m["include"].(map[string]any)
	assert.Equal(t, 0, inc["partition"])
	assert.Equal(t, 3, inc["num_partitions"])

	a5 := NewSignificantTermsAggregation().SetIncludeExclude(&TermsAggregationIncludeExclude{Include: "pop.*", Exclude: "old.*"})
	a5.FieldVal = "genre"
	src5, err := a5.Source()
	require.NoError(t, err)
	m = src5.(map[string]any)["significant_terms"].(map[string]any)
	assert.Equal(t, "pop.*", m["include"])
	assert.Equal(t, "old.*", m["exclude"])
}

func TestC3InnerHitWithName(t *testing.T) {
	ih := NewInnerHit().Name("my_inner").Size(5).From(0).TrackScores(true).Explain(true).Version(true)
	src, err := ih.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "my_inner", m["name"])
}

func TestC3DistanceFeatureQueryAll(t *testing.T) {
	boost := 1.5
	q := NewDistanceFeatureQuery("location", "now", "100km")
	q.Boost = &boost
	q.QueryName = "my_query"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["distance_feature"].(map[string]any)
	assert.Equal(t, "location", m["field"])
	assert.Equal(t, "100km", m["pivot"])
	assert.Equal(t, "now", m["origin"])
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "my_query", m["_name"])

	q2 := NewDistanceFeatureQuery("location", &GeoPoint{Lat: 40.0, Lon: -74.0}, "10km")
	src2, err := q2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)["distance_feature"].(map[string]any)
	origin := m["origin"].(map[string]float64)
	assert.Equal(t, 40.0, origin["lat"])

	q3 := NewDistanceFeatureQuery("location", GeoPoint{Lat: 35.0, Lon: 139.0}, "5km")
	src3, err := q3.Source()
	require.NoError(t, err)
	m = src3.(map[string]any)["distance_feature"].(map[string]any)
	assert.NotNil(t, m["origin"])

	q4 := NewDistanceFeatureQuery("location", 42, "10km")
	_, err = q4.Source()
	assert.Error(t, err)
}

func TestC3FunctionScoreQueryFilter(t *testing.T) {
	q := NewFunctionScoreQuery()
	q.Filter(NewTermQuery("status", "active"))
	q.ScoreMode("avg")
	q.BoostMode("replace")
	q.MaxBoost(5.0)
	q.Boost(2.0)
	q.MinScore(1.0)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	assert.Contains(t, m, "filter")
	assert.Equal(t, "avg", m["score_mode"])
	assert.Equal(t, "replace", m["boost_mode"])
	assert.Equal(t, 5.0, m["max_boost"])
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, 1.0, m["min_score"])
}

func TestC3FunctionScoreQueryAddFuncNilFilter(t *testing.T) {
	fn := NewRandomFunction().Field("random_score").Seed(42)
	q := NewFunctionScoreQuery()
	q.AddScoreFunc(fn)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	funcs := m["functions"].([]any)
	assert.Equal(t, 1, len(funcs))
}

func TestC3ScriptFunctionNilScript(t *testing.T) {
	fn := NewScriptFunction(nil)
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotContains(t, m, "script")

	fn2 := NewScriptFunction(NewScript("return 1"))
	src2, err := fn2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)
	assert.Contains(t, m, "script")

	fn3 := NewScriptFunction(nil)
	fn3.Script(NewScript("return 2"))
	fn3.Weight(3.0)
	src3, err := fn3.Source()
	require.NoError(t, err)
	m = src3.(map[string]any)
	assert.Contains(t, m, "script")
	assert.Equal(t, 3.0, *fn3.GetWeight())
}

func TestC3IntervalQueryFilterAllPaths(t *testing.T) {
	match1 := NewIntervalQueryRuleMatch("hello")
	match2 := NewIntervalQueryRuleMatch("world")
	match3 := NewIntervalQueryRuleMatch("foo")
	match4 := NewIntervalQueryRuleMatch("bar")
	match5 := NewIntervalQueryRuleMatch("baz")
	match6 := NewIntervalQueryRuleMatch("qux")
	match7 := NewIntervalQueryRuleMatch("quux")
	match8 := NewIntervalQueryRuleMatch("corge")
	script := NewScript("return true")

	f := NewIntervalQueryFilter().
		After(match1).
		Before(match2).
		ContainedBy(match3).
		Containing(match4).
		Overlapping(match5).
		NotContainedBy(match6).
		NotContaining(match7).
		NotOverlapping(match8).
		Script(script)

	src, err := f.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "after")
	assert.Contains(t, m, "before")
	assert.Contains(t, m, "contained_by")
	assert.Contains(t, m, "containing")
	assert.Contains(t, m, "overlapping")
	assert.Contains(t, m, "not_contained_by")
	assert.Contains(t, m, "not_containing")
	assert.Contains(t, m, "not_overlapping")
	assert.Contains(t, m, "script")
}

func TestC3IntervalRulesAllOfAllFields(t *testing.T) {
	r := NewIntervalQueryRuleAllOf(
		NewIntervalQueryRuleMatch("hello"),
		NewIntervalQueryRuleMatch("world"),
	).MaxGaps(5).Ordered(true).Filter(NewIntervalQueryFilter().After(NewIntervalQueryRuleMatch("foo")))
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["all_of"].(map[string]any)
	assert.Equal(t, 5, m["max_gaps"])
	assert.Equal(t, true, m["ordered"])
	assert.Contains(t, m, "filter")
	intervals := m["intervals"].([]any)
	assert.Equal(t, 2, len(intervals))
}

func TestC3IntervalRulesAnyOfWithFilter(t *testing.T) {
	r := NewIntervalQueryRuleAnyOf(
		NewIntervalQueryRuleMatch("hello"),
	).Filter(NewIntervalQueryFilter().Before(NewIntervalQueryRuleMatch("bar")))
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["any_of"].(map[string]any)
	assert.Contains(t, m, "filter")
}

func TestC3HasChildQueryFullCoverage(t *testing.T) {
	boost := 2.5
	scc := int(3)
	q := NewHasChildQuery("answer", NewMatchAllQuery())
	q.Boost = &boost
	q.ScoreMode = "max"
	mc := 1
	xc := 10
	q.MinChildren = &mc
	q.MaxChildren = &xc
	q.ShortCircuitCutoff = &scc
	q.QueryName = "hc_test"
	q.InnerHit = NewInnerHit().Name("my_hit").Size(3)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["has_child"].(map[string]any)
	assert.Contains(t, m, "inner_hits")
}

func TestC3HasChildQueryInnerHitErr(t *testing.T) {
	q := NewHasChildQuery("answer", NewMatchAllQuery())
	q.InnerHit = (*InnerHit)(nil)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["has_child"].(map[string]any)
	assert.NotContains(t, m, "inner_hits")
}

func TestC3HasParentQueryFullCoverage(t *testing.T) {
	boost := 1.5
	score := true
	iu := true
	q := NewHasParentQuery("question", NewMatchAllQuery())
	q.Boost = &boost
	q.Score = &score
	q.QueryName = "hp_test"
	q.InnerHit = NewInnerHit().Name("my_parent_hit").Size(2)
	q.IgnoreUnmapped = &iu
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["has_parent"].(map[string]any)
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, true, m["ignore_unmapped"])
}

func TestC3DocvalueFieldWithFormat(t *testing.T) {
	d := DocvalueField{Field: "timestamp", Format: "epoch_millis"}
	src, err := d.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "timestamp", m["field"])
	assert.Equal(t, "epoch_millis", m["format"])

	d2 := DocvalueField{Field: "name", Format: ""}
	src2, err := d2.Source()
	require.NoError(t, err)
	assert.Equal(t, "name", src2)
}

func TestC3DocvalueFieldsSource(t *testing.T) {
	var d DocvalueFields
	src, err := d.Source()
	require.NoError(t, err)
	assert.Nil(t, src)
}

func TestC3FieldFieldWithFormat(t *testing.T) {
	f := FieldField{Field: "title", Format: "text"}
	src, err := f.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "text", m["format"])

	f2 := FieldField{Field: "name", Format: ""}
	src2, err := f2.Source()
	require.NoError(t, err)
	assert.Equal(t, "name", src2)
}

func TestC3FieldFieldsSource(t *testing.T) {
	var f FieldFields
	src, err := f.Source()
	require.NoError(t, err)
	assert.Nil(t, src)
}

func TestC3HighlightFullSource(t *testing.T) {
	hl := NewHighlight().
		Field("title").
		Fields(NewHighlighterField("body").
			FragmentSize(100).
			NumOfFragments(3).
			FragmentOffset(10).
			HighlightFilter(true).
			Order("score").
			RequireFieldMatch(false).
			BoundaryMaxScan(20).
			BoundaryChars('.', ',').
			HighlighterType("plain").
			Fragmenter("simple").
			HighlightQuery(NewTermQuery("status", "active")).
			NoMatchSize(50).
			PhraseLimit(10).
			MatchedFields("body", "body.ngram").
			Options(map[string]any{"boundary_scanner_locale": "en-US"}).
			ForceSource(true)).
		TagsSchema("styled").
		PreTags("<em>").
		PostTags("</em>").
		Order("score").
		HighlightFilter(true).
		FragmentSize(150).
		NumOfFragments(3).
		Encoder("html").
		RequireFieldMatch(false).
		MaxAnalyzedOffset(1000).
		BoundaryMaxScan(10).
		BoundaryChars(",.").
		BoundaryScannerType("sentence").
		BoundaryScannerLocale("en-US").
		HighlighterType("unified").
		Fragmenter("span").
		HighlightQuery(NewTermQuery("type", "article")).
		NoMatchSize(100).
		Options(map[string]any{"foo": "bar"}).
		ForceSource(true).
		UseExplicitFieldOrder(true)
	src, err := hl.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "tags_schema")
	assert.Contains(t, m, "pre_tags")
	assert.Contains(t, m, "post_tags")
	assert.Contains(t, m, "order")
	assert.Contains(t, m, "highlight_filter")
	assert.Contains(t, m, "fragment_size")
	assert.Contains(t, m, "number_of_fragments")
	assert.Contains(t, m, "encoder")
	assert.Contains(t, m, "require_field_match")
	assert.Contains(t, m, "max_analyzed_offset")
	assert.Contains(t, m, "boundary_max_scan")
	assert.Contains(t, m, "boundary_chars")
	assert.Contains(t, m, "boundary_scanner")
	assert.Contains(t, m, "boundary_scanner_locale")
	assert.Contains(t, m, "type")
	assert.Contains(t, m, "fragmenter")
	assert.Contains(t, m, "highlight_query")
	assert.Contains(t, m, "no_match_size")
	assert.Contains(t, m, "options")
	assert.Contains(t, m, "force_source")
	fields := m["fields"].([]map[string]any)
	assert.Equal(t, 2, len(fields))
}

func TestC3HighlighterFieldAllOptions(t *testing.T) {
	f := NewHighlighterField("content").
		PreTags("<b>").
		PostTags("</b>").
		FragmentSize(100).
		FragmentOffset(5).
		NumOfFragments(2).
		HighlightFilter(true).
		Order("score").
		RequireFieldMatch(false).
		BoundaryMaxScan(50).
		BoundaryChars('.', ',').
		HighlighterType("fvh").
		Fragmenter("span").
		HighlightQuery(NewTermQuery("type", "article")).
		NoMatchSize(30).
		PhraseLimit(5).
		MatchedFields("content", "content.ngram").
		Options(map[string]any{"boundary_scanner_locale": "en"}).
		ForceSource(true)
	src, err := f.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "<b>", m["pre_tags"].([]string)[0])
	assert.Equal(t, "</b>", m["post_tags"].([]string)[0])
	assert.Equal(t, 100, m["fragment_size"])
	assert.Equal(t, 5, m["fragment_offset"])
	assert.Equal(t, 2, m["number_of_fragments"])
	assert.Equal(t, true, m["highlight_filter"])
	assert.Equal(t, "score", m["order"])
	assert.Equal(t, false, m["require_field_match"])
	assert.Equal(t, 50, m["boundary_max_scan"])
	assert.NotNil(t, m["boundary_chars"])
	assert.Equal(t, "fvh", m["type"])
	assert.Equal(t, "span", m["fragmenter"])
	assert.Contains(t, m, "highlight_query")
	assert.Equal(t, 30, m["no_match_size"])
	assert.NotNil(t, m["matched_fields"])
	assert.Equal(t, 5, m["phrase_limit"])
	assert.NotNil(t, m["options"])
	assert.Equal(t, true, m["force_source"])
}

func TestC3RescoreWithWindow(t *testing.T) {
	rc := NewQueryRescorer(NewTermQuery("status", "active")).
		QueryWeight(1.0).
		RescoreQueryWeight(2.0).
		ScoreMode("total")
	r := NewRescore().WindowSize(100).Rescorer(rc)
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 100, m["window_size"])
	assert.Contains(t, m, "query")
}

func TestC3RescoreDefaultWindow(t *testing.T) {
	rc := NewQueryRescorer(NewMatchAllQuery())
	defWindow := 50
	r := &Rescore{rescorer: rc, defaultRescoreWindowSize: &defWindow}
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 50, m["window_size"])
}

func TestC3QueryRescorerAllOptions(t *testing.T) {
	rc := NewQueryRescorer(NewTermQuery("field", "value")).
		QueryWeight(0.5).
		RescoreQueryWeight(1.5).
		ScoreMode("multiply")
	src, err := rc.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 0.5, m["query_weight"])
	assert.Equal(t, 1.5, m["rescore_query_weight"])
	assert.Equal(t, "multiply", m["score_mode"])
}

func TestC3CompositeAggAllValueSources(t *testing.T) {
	boolTrue := true
	a := NewCompositeAggregation().
		WithSize(10).
		AggregateAfter(map[string]any{"product": "abc"}).
		Sources(
			NewCompositeAggregationTermsValuesSource("product").
				Field("product.name").
				SetScript(NewScript("doc['product.name'].value")).
				ValueTypeValue("string").
				Asc().
				Desc().
				OrderValue("asc").
				MissingValue("N/A").
				MissingBucketValue(true),
			NewCompositeAggregationHistogramValuesSource("price_hist", 10.0).
				Field("price").
				SetScript(NewScript("doc['price'].value / 10")).
				ValueTypeValue("double").
				Asc().
				Desc().
				OrderValue("desc").
				MissingValue(0).
				MissingBucketValue(true).
				IntervalValue(5.0),
			NewCompositeAggregationDateHistogramValuesSource("date_hist").
				Field("timestamp").
				SetScript(NewScript("doc['timestamp'].value")).
				ValueTypeValue("date").
				Asc().
				Desc().
				OrderValue("asc").
				MissingValue("1970-01-01").
				MissingBucketValue(true).
				IntervalValue("1d").
				FixedIntervalValue("12h").
				CalendarIntervalValue("month").
				FormatValue("epoch_millis").
				TimeZoneValue("UTC"),
		).
		SubAggregation("avg_price", AvgAggregation{Field: "price"}).
		WithMeta(map[string]any{"foo": "bar"})

	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["composite"].(map[string]any)
	assert.Contains(t, m, "sources")
	assert.Contains(t, m, "size")
	assert.Contains(t, m, "after")
	assert.Contains(t, m, "aggregations")
	assert.Contains(t, m, "meta")
	_ = boolTrue
}

func TestC3FiltersAggNamed(t *testing.T) {
	a := NewFiltersAggregation()
	a.NamedFilters = map[string]Query{
		"active":   NewTermQuery("status", "active"),
		"inactive": NewTermQuery("status", "inactive"),
	}
	other := true
	a.OtherBucket = &other
	a.OtherBucketKey = "other"
	a.SubAggs = map[string]Aggregation{"avg_price": AvgAggregation{Field: "price"}}
	a.Meta = map[string]any{"key": "val"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["filters"].(map[string]any)
	assert.Contains(t, m, "filters")
	assert.Equal(t, true, m["other_bucket"])
	assert.Equal(t, "other", m["other_bucket_key"])
}

func TestC3FiltersAggUnnamed(t *testing.T) {
	a := FiltersAggregation{
		UnnamedFilters: []Query{
			NewTermQuery("status", "active"),
			NewTermQuery("status", "inactive"),
		},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["filters"].(map[string]any)
	assert.NotNil(t, m["filters"])
}

func TestC3MultiTermsAggAllFields(t *testing.T) {
	size := int(10)
	shardSize := int(100)
	minDoc := int(1)
	shardMinDoc := int(2)
	showErr := true
	a := NewMultiTermsAggregation().
		Terms("genre", "year").
		MultiTerms(MultiTerm{Field: "country", Missing: "N/A"}).
		WithSize(size).
		WithShardSize(shardSize).
		WithMinDocCount(minDoc).
		WithShardMinDocCount(shardMinDoc).
		WithShowTermDocCountError(showErr).
		WithCollectionMode("depth_first").
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByAggregation("avg_price", true).
		OrderByAggregationAndMetric("avg_price", "value", false).
		Order("genre", true).
		WithMeta(map[string]any{"k": "v"}).
		SubAggregation("avg_price", AvgAggregation{Field: "price"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["multi_terms"].(map[string]any)
	assert.Contains(t, m, "terms")
	assert.Equal(t, 10, m["size"])
	assert.Equal(t, 100, m["shard_size"])
	assert.Equal(t, 1, m["min_doc_count"])
	assert.Equal(t, 2, m["shard_min_doc_count"])
	assert.Equal(t, true, m["show_term_doc_count_error"])
	assert.Equal(t, "depth_first", m["collect_mode"])
	assert.NotNil(t, m["order"])
	assert.Contains(t, m, "meta")
	assert.Contains(t, m, "aggregations")
}

func TestC3RareTermsAggFull(t *testing.T) {
	maxDoc := int(3)
	prec := 0.001

	a := NewRareTermsAggregation()
	a.Field = "genre"
	a.MaxDocCount = &maxDoc
	a.Precision = &prec
	a.Missing = "unknown"
	a.IncludeExclude = &TermsAggregationIncludeExclude{
		Include: "rock.*",
		Exclude: "old.*",
	}
	a.SubAggs = map[string]Aggregation{"avg_price": AvgAggregation{Field: "price"}}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["rare_terms"].(map[string]any)
	assert.Equal(t, "genre", m["field"])
	assert.Equal(t, 3, m["max_doc_count"])
	assert.Equal(t, 0.001, m["precision"])
	assert.Equal(t, "unknown", m["missing"])
	assert.Contains(t, m, "include")
	assert.Contains(t, m, "exclude")
}

func TestC3DecayFunctionsFull(t *testing.T) {
	decay := 0.5
	origin := "2024-01-01"
	scale := "30d"
	offset := "2d"

	exp := NewExponentialDecayFunction().
		FieldName("date").
		Origin(origin).
		Scale(scale).
		Decay(decay).
		Offset(offset).
		MultiValueMode("max").
		Weight(2.0)
	assert.Equal(t, 2.0, *exp.GetWeight())
	src, err := exp.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "date")
	assert.Equal(t, "max", m["multi_value_mode"])

	gauss := NewGaussDecayFunction().
		FieldName("date").
		Origin(origin).
		Scale(scale).
		Decay(decay).
		Offset(offset).
		MultiValueMode("min").
		Weight(1.5)
	assert.Equal(t, 1.5, *gauss.GetWeight())
	src2, err := gauss.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)
	assert.Contains(t, m, "date")

	linear := NewLinearDecayFunction().
		FieldName("date").
		Origin(origin).
		Scale(scale).
		Decay(decay).
		Offset(offset).
		MultiValueMode("avg").
		Weight(1.0)
	assert.Equal(t, "avg", linear.GetMultiValueMode())
	assert.Equal(t, 1.0, *linear.GetWeight())
	src3, err := linear.Source()
	require.NoError(t, err)
	m = src3.(map[string]any)
	assert.Contains(t, m, "date")
}

func TestC3FieldValueFactorFunctionFull(t *testing.T) {
	factor := 1.2
	missing := 0.5
	wt := 0.8
	fn := NewFieldValueFactorFunction().
		Field("likes").
		Factor(factor).
		Missing(missing).
		Modifier("log1p").
		Weight(wt)
	assert.Equal(t, 0.8, *fn.GetWeight())
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "likes", m["field"])
	assert.Equal(t, 1.2, m["factor"])
	assert.Equal(t, 0.5, m["missing"])
	assert.Equal(t, "log1p", m["modifier"])
}

func TestC3RandomFunctionFull(t *testing.T) {
	wt := 1.5
	fn := NewRandomFunction().
		Field("_seq_no").
		Seed(42).
		Weight(wt)
	assert.Equal(t, 1.5, *fn.GetWeight())
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "_seq_no", m["field"])
	assert.Equal(t, 42, m["seed"])
}

func TestC3WeightFactorFunctionFull(t *testing.T) {
	fn := NewWeightFactorFunction(2.5).Weight(3.0)
	assert.NotNil(t, fn.GetWeight())
	src, err := fn.Source()
	require.NoError(t, err)
	assert.Equal(t, 3.0, src)
}

func TestC3IntervalQueryMatchFull(t *testing.T) {
	r := NewIntervalQueryRuleMatch("quick brown fox").
		MaxGaps(0).
		Ordered(true).
		Analyzer("standard").
		UseField("body").
		Filter(NewIntervalQueryFilter().After(NewIntervalQueryRuleMatch("hello")))
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match"].(map[string]any)
	assert.Contains(t, m, "filter")
}

func TestC3GeoPointFromString(t *testing.T) {
	pt, err := GeoPointFromString("40.7128,-74.0060")
	require.NoError(t, err)
	assert.Equal(t, 40.7128, pt.Lat)
	assert.Equal(t, -74.0060, pt.Lon)

	_, err = GeoPointFromString("invalid")
	assert.Error(t, err)

	_, err = GeoPointFromString("40.7128,notanum")
	assert.Error(t, err)
}

func TestC3SearchSourceMoreBranches(t *testing.T) {
	src := NewSearchSource().
		Query(NewMatchAllQuery()).
		PostFilter(NewTermQuery("status", "published")).
		From(0).
		Size(10).
		Timeout("10s").
		TerminateAfter(1000).
		MinScore(1.0).
		Version(true).
		Explain(true).
		Profile(true).
		FetchSource(false).
		DocvalueFieldsWithFormat(DocvalueField{Field: "timestamp", Format: "epoch_millis"}).
		ScriptFields(NewScriptField("total", NewScript("doc['price'].value * doc['qty'].value"))).
		StoredField("title").
		StoredFields("title", "body").
		NoStoredFields().
		TrackScores(true).
		TrackTotalHits(true).
		SearchAfter(1234, "doc123").
		SortBy(NewFieldSort("date").Desc()).
		Sort("_score", false).
		SortWithInfo(SortInfo{Field: "title", Ascending: true}).
		Aggregation("avg_price", AvgAggregation{Field: "price"}).
		Highlight(NewHighlight().Field("title")).
		IndexBoost("idx1", 1.5).
		Stats("group1").
		InnerHit("my_inner", NewInnerHit().Name("hit1").Size(2)).
		Collapse(NewCollapseBuilder("user").InnerHit(NewInnerHit().Name("tweets").Size(2)))
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourceEmpty(t *testing.T) {
	src := NewSearchSource()
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourceCollapseNoInner(t *testing.T) {
	src := NewSearchSource().Collapse(NewCollapseBuilder("user"))
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourceRescore(t *testing.T) {
	src := NewSearchSource().
		Query(NewMatchAllQuery()).
		Rescorer(NewRescore().WindowSize(100).Rescorer(NewQueryRescorer(NewTermQuery("boost", "true"))))
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourceSuggest(t *testing.T) {
	src := NewSearchSource().
		Query(NewMatchAllQuery())
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3BoolQuerySingleClauseBranches(t *testing.T) {
	q := NewBoolQuery().
		Must(NewTermQuery("title", "test")).
		MustNot(NewTermQuery("draft", "true")).
		Filter(NewTermQuery("type", "article")).
		Should(NewTermQuery("author", "john"))
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bool"].(map[string]any)
	assert.NotNil(t, m["must"])
}

func TestC3BoolQueryCollectError(t *testing.T) {
	q := &BoolQuery{
		mustClauses: []Query{mockQueryError{}},
	}
	_, err := q.Source()
	assert.Error(t, err)

	q2 := &BoolQuery{
		mustNotClauses: []Query{mockQueryError{}},
	}
	_, err = q2.Source()
	assert.Error(t, err)

	q3 := &BoolQuery{
		filterClauses: []Query{mockQueryError{}},
	}
	_, err = q3.Source()
	assert.Error(t, err)

	q4 := &BoolQuery{
		shouldClauses: []Query{mockQueryError{}},
	}
	_, err = q4.Source()
	assert.Error(t, err)
}

func TestC3FuzzyCompletionSuggesterOptionsAll(t *testing.T) {
	opts := NewFuzzyCompletionSuggesterOptions().
		EditDistance("1").
		Transpositions(true).
		MinLength(3).
		PrefixLength(1).
		UnicodeAware(true).
		MaxDeterminizedStates(10000)
	src, err := opts.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "1", m["fuzziness"])
	assert.Equal(t, true, m["transpositions"])
	assert.Equal(t, 3, m["min_length"])
	assert.Equal(t, 1, m["prefix_length"])
	assert.Equal(t, true, m["unicode_aware"])
	assert.Equal(t, 10000, m["max_determinized_states"])
}

func TestC3RegexCompletionSuggesterOptionsAll(t *testing.T) {
	opts := NewRegexCompletionSuggesterOptions().
		Flags("ALL").
		MaxDeterminizedStates(10000)
	src, err := opts.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "ALL", m["flags"])
	assert.Equal(t, 10000, m["max_determinized_states"])
}

func TestC3SearchSourceWithDocvalueFields(t *testing.T) {
	src := NewSearchSource().
		Query(NewMatchAllQuery()).
		DocvalueFields("timestamp").
		DocvalueFieldsWithFormat(DocvalueField{Field: "date", Format: "epoch_millis"})
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourceFieldFields(t *testing.T) {
	src := NewSearchSource().
		Query(NewMatchAllQuery())
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3SearchSourcePointInTime(t *testing.T) {
	pit := &PointInTime{Id: "abc123", KeepAlive: "1m"}
	src := NewSearchSource().
		Query(NewMatchAllQuery()).
		PointInTime(pit)
	_, err := src.Source()
	require.NoError(t, err)
}

func TestC3RareTermsAggIncludeExcludeValues(t *testing.T) {
	a := NewRareTermsAggregation()
	a.Field = "genre"
	a.IncludeExclude = &TermsAggregationIncludeExclude{
		IncludeValues: []any{"rock", "jazz"},
		ExcludeValues: []any{"classical", "metal"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["rare_terms"].(map[string]any)
	assert.NotNil(t, m["include"])
	assert.NotNil(t, m["exclude"])
}

func TestC3RareTermsAggPartitions(t *testing.T) {
	a := NewRareTermsAggregation()
	a.Field = "genre"
	a.IncludeExclude = &TermsAggregationIncludeExclude{
		NumPartitions: 4,
		Partition:     1,
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["rare_terms"].(map[string]any)
	assert.NotNil(t, m["include"])
}

func TestC3TermsAggFullSource(t *testing.T) {
	size := int(10)
	shardSize := int(100)
	reqSize := int(20)
	minDoc := int(2)
	shardMinDoc := int(1)
	showErr := true
	a := NewTermsAggregation().
		WithField("genre").
		WithScript(NewScript("doc['genre'].value")).
		WithMissing("N/A").
		WithSize(size).
		WithRequiredSize(reqSize).
		WithShardSize(shardSize).
		WithMinDocCount(minDoc).
		WithShardMinDocCount(shardMinDoc).
		WithShowTermDocCountError(showErr).
		WithCollectionMode("breadth_first").
		WithValueType("string").
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByTermAsc().
		OrderByTermDesc().
		OrderByAggregation("avg_price", true).
		OrderByAggregationAndMetric("avg_price", "value", false).
		OrderBy("genre", true).
		WithIncludeExclude(&TermsAggregationIncludeExclude{
			Include: "rock.*",
			Exclude: "old.*",
		}).
		WithExecutionHint("map").
		WithSubAggregation("avg_price", AvgAggregation{Field: "price"}).
		WithMeta(map[string]any{"k": "v"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms"].(map[string]any)
	assert.Equal(t, "genre", m["field"])
}

func TestC3TermsAggIncludeExcludeMergeInto(t *testing.T) {
	ie := &TermsAggregationIncludeExclude{
		IncludeValues: []any{"rock", "jazz"},
	}
	m := map[string]any{}
	err := ie.MergeInto(m)
	require.NoError(t, err)
	assert.NotNil(t, m["include"])

	ie2 := &TermsAggregationIncludeExclude{
		ExcludeValues: []any{"classical"},
	}
	m2 := map[string]any{}
	err = ie2.MergeInto(m2)
	require.NoError(t, err)
	assert.NotNil(t, m2["exclude"])

	ie3 := &TermsAggregationIncludeExclude{
		NumPartitions: 3,
		Partition:     1,
	}
	m3 := map[string]any{}
	err = ie3.MergeInto(m3)
	require.NoError(t, err)
	inc := m3["include"].(map[string]any)
	assert.Equal(t, 1, inc["partition"])
	assert.Equal(t, 3, inc["num_partitions"])
}

func TestC3TermsAggIncludeExcludeSourceEmpty(t *testing.T) {
	ie := &TermsAggregationIncludeExclude{}
	src, err := ie.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Empty(t, m)
}

func TestC3WeightedAvgAggFull(t *testing.T) {
	a := NewWeightedAvgAggregation()
	a.Fields["price"] = &MultiValuesSourceFieldConfig{
		FieldName: "price",
		Missing:   0,
		Script:    NewScript("doc['price'].value"),
		TimeZone:  "UTC",
	}
	a.Format = "0.00"
	a.ValueType = "double"
	a.Value = &MultiValuesSourceFieldConfig{
		FieldName: "price",
		Missing:   1,
		Script:    NewScript("doc['price'].value"),
		TimeZone:  "UTC",
	}
	a.Weight = &MultiValuesSourceFieldConfig{
		FieldName: "weight",
		Missing:   1,
		Script:    NewScript("doc['weight'].value"),
		TimeZone:  "UTC",
	}
	a.SubAggs = map[string]Aggregation{"avg": AvgAggregation{Field: "price"}}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["weighted_avg"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Equal(t, "0.00", m["format"])
	assert.Equal(t, "double", m["value_type"])
	assert.Contains(t, m, "value")
	assert.Contains(t, m, "weight")
}

func TestC3MultiValuesSourceFieldConfigAll(t *testing.T) {
	f := MultiValuesSourceFieldConfig{
		FieldName: "price",
		Missing:   0,
		Script:    NewScript("doc['price'].value"),
		TimeZone:  "UTC",
	}
	src, err := f.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "price", m["field"])
	assert.Equal(t, 0, m["missing"])
	assert.Contains(t, m, "script")
	assert.Equal(t, "UTC", m["time_zone"])

	f2 := MultiValuesSourceFieldConfig{}
	src2, err := f2.Source()
	require.NoError(t, err)
	m2 := src2.(map[string]any)
	assert.Empty(t, m2)
}

func TestC3BucketSortAggFull(t *testing.T) {
	a := NewBucketSortAggregation()
	a.From = 5
	a.Size = 10
	a.GapPolicy = "skip"
	a.Sorters = []Sorter{NewFieldSort("date").Asc()}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bucket_sort"].(map[string]any)
	assert.Equal(t, 5, m["from"])
	assert.Equal(t, 10, m["size"])
	assert.Equal(t, "skip", m["gap_policy"])
	assert.NotNil(t, m["sort"])
}

func TestC3CollapseBuilderAll(t *testing.T) {
	cb := NewCollapseBuilder("user").
		InnerHit(NewInnerHit().Name("tweets").Size(3)).
		MaxConcurrentGroupRequests(4)
	src, err := cb.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "user", m["field"])
	assert.NotNil(t, m["inner_hits"])
	assert.Equal(t, 4, m["max_concurrent_group_searches"])
}

func TestC3BoolQueryAllClauses(t *testing.T) {
	b := 2.0
	q := NewBoolQuery().
		Must(NewTermQuery("title", "test"), NewTermQuery("body", "test")).
		MustNot(NewTermQuery("status", "draft")).
		Filter(NewTermQuery("type", "article"), NewTermQuery("year", 2024)).
		Should(NewTermQuery("author", "john"), NewTermQuery("author", "jane")).
		Boost(b).
		MinimumNumberShouldMatch(2).
		AdjustPureNegative(true).
		QueryName("my_bool")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bool"].(map[string]any)
	assert.Contains(t, m, "must")
	assert.Contains(t, m, "must_not")
	assert.Contains(t, m, "filter")
	assert.Contains(t, m, "should")
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "2", m["minimum_should_match"])
	assert.Equal(t, true, m["adjust_pure_negative"])
	assert.Equal(t, "my_bool", m["_name"])
}

func TestC3CombinedFieldsQueryAll(t *testing.T) {
	boost := 2.0
	agspq := true
	q := NewCombinedFieldsQuery("search text", "title", "body")
	q.FieldBoosts["title"] = &boost
	q.AutoGenerateSynonymsPhraseQuery = &agspq
	q.Operator = "and"
	q.MinimumShouldMatch = "1"
	q.ZeroTermsQuery = "all"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["combined_fields"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Equal(t, true, m["auto_generate_synonyms_phrase_query"])
	assert.Equal(t, "and", m["operator"])
	assert.Equal(t, "1", m["minimum_should_match"])
	assert.Equal(t, "all", m["zero_terms_query"])
}

func TestC3MoreLikeThisQueryFull(t *testing.T) {
	q := NewMoreLikeThisQuery().
		Field("title", "body").
		StopWord("the", "a").
		LikeText("hello world").
		LikeItems(NewMoreLikeThisQueryItem().Id("1").Index("idx").Type("doc").
			Doc(map[string]any{"key": "val"}).Fields("f1").Routing("r1").
			FetchSourceContext(NewFetchSourceContext(true).Include("title")).
			Version(int64(1)).VersionType("external")).
		IgnoreLikeText("ignore me").
		IgnoreLikeItems(NewMoreLikeThisQueryItem().Id("2")).
		Ids("3", "4").
		Include(true).
		MinimumShouldMatch("30%").
		MinTermFreq(2).
		MaxQueryTerms(25).
		MinDocFreq(5).
		MaxDocFreq(100).
		MinWordLength(3).
		MaxWordLength(50).
		BoostTerms(2.0).
		Analyzer("standard").
		Boost(1.5).
		FailOnUnsupportedField(true).
		QueryName("mlt_test")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["more_like_this"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Contains(t, m, "like")
}

func TestC3MoreLikeThisItemLikeText(t *testing.T) {
	item := NewMoreLikeThisQueryItem().LikeText("hello")
	src, err := item.Source()
	require.NoError(t, err)
	assert.Equal(t, "hello", src)
}

func TestC3MultiMatchQueryAll(t *testing.T) {
	boost := 2.0
	slop := int(2)
	prefixLen := int(1)
	maxExp := int(10)
	tie := 0.3
	lenient := true
	cutoff := 0.001
	q := NewMultiMatchQuery("test query", "title", "body")
	q.FieldBoosts["title"] = &boost
	q.Type = "best_fields"
	q.Operator = "and"
	q.Analyzer = "standard"
	q.Boost = &boost
	q.Slop = &slop
	q.Fuzziness = "AUTO"
	q.PrefixLength = &prefixLen
	q.MaxExpansions = &maxExp
	q.MinimumShouldMatch = "75%"
	q.Rewrite = "constant_score"
	q.FuzzyRewrite = "top_terms_boost_N"
	q.TieBreaker = &tie
	q.Lenient = &lenient
	q.CutoffFrequency = &cutoff
	q.ZeroTermsQuery = "all"
	q.QueryName = "mm_test"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["multi_match"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Equal(t, "best_fields", m["type"])
}

func TestC3MultiMatchQueryNoFields(t *testing.T) {
	q := NewMultiMatchQuery("test")
	q.Fields = nil
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["multi_match"].(map[string]any)
	fields := m["fields"].([]string)
	assert.Equal(t, 0, len(fields))
}

func TestC3IndexBoostsSource(t *testing.T) {
	ib := IndexBoosts{
		{Index: "idx1", Boost: 1.5},
		{Index: "idx2", Boost: 2.0},
	}
	src, err := ib.Source()
	require.NoError(t, err)
	boosts := src.([]any)
	assert.Equal(t, 2, len(boosts))
}

func TestC3SuggestFieldMarshalAllBranches(t *testing.T) {
	f := NewSuggestField("single_input").Weight(10)
	data, err := json.Marshal(f)
	require.NoError(t, err)
	m := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, "single_input", m["input"])
	assert.Equal(t, float64(10), m["weight"])

	f2 := NewSuggestField("input1", "input2").Weight(5)
	data2, err := json.Marshal(f2)
	require.NoError(t, err)
	m2 := map[string]any{}
	require.NoError(t, json.Unmarshal(data2, &m2))
	assert.NotNil(t, m2["input"])

	f3 := NewSuggestField().Input("hello")
	f3.ContextQuery(NewSuggesterCategoryQuery("cat", "val1"))
	data3, err := json.Marshal(f3)
	require.NoError(t, err)
	m3 := map[string]any{}
	require.NoError(t, json.Unmarshal(data3, &m3))
	assert.Contains(t, m3, "contexts")

	f4 := NewSuggestField().Input("hello")
	f4.ContextQuery(
		NewSuggesterCategoryQuery("cat1", "val1"),
		NewSuggesterCategoryQuery("cat2", "val2"),
	)
	data4, err := json.Marshal(f4)
	require.NoError(t, err)
	m4 := map[string]any{}
	require.NoError(t, json.Unmarshal(data4, &m4))
	assert.Contains(t, m4, "contexts")
}

type badCtxQuery struct{}

func (b *badCtxQuery) Source() (any, error) { return "not a map", nil }

func ptrInt(v int) *int       { return &v }
func ptrInt64(v int64) *int64 { return &v }

func TestC3SuggestFieldMarshalContextNotMap(t *testing.T) {
	f := NewSuggestField().Input("hello")
	f.ContextQuery(&badCtxQuery{}, &badCtxQuery{})
	_, err := json.Marshal(f)
	assert.Error(t, err)
}

func TestC3CompletionSuggesterSourceFull(t *testing.T) {
	fuzzyOpts := NewFuzzyCompletionSuggesterOptions().
		EditDistance("AUTO").
		Transpositions(true).
		MinLength(3).
		PrefixLength(1).
		UnicodeAware(true).
		MaxDeterminizedStates(10000)
	regexOpts := NewRegexCompletionSuggesterOptions().
		Flags("ALL").
		MaxDeterminizedStates(10000)

	s := NewCompletionSuggester("song-suggest").
		Text("nirvana").
		Field("suggest").
		Analyzer("standard").
		Size(5).
		ShardSize(10).
		SkipDuplicates(true).
		ContextQuery(NewSuggesterCategoryQuery("genre", "rock")).
		PrefixWithOptions("nir", fuzzyOpts).
		RegexWithOptions("n.*", regexOpts).
		FuzzyOptions(fuzzyOpts).
		RegexOptions(regexOpts).
		Fuzziness("AUTO")
	src, err := s.Source(true)
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "song-suggest")
}

func TestC3CompletionSuggesterNoName(t *testing.T) {
	s := NewCompletionSuggester("test").Text("hello").Field("suggest")
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3CompletionSuggesterMultipleCtx(t *testing.T) {
	s := NewCompletionSuggester("test").
		Text("hello").
		Field("suggest").
		ContextQueries(
			NewSuggesterCategoryQuery("cat1", "val1"),
			NewSuggesterCategoryQuery("cat2", "val2"),
		)
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3CompletionSuggesterCtxNotMap(t *testing.T) {
	s := NewCompletionSuggester("test").
		Text("hello").
		ContextQueries(&badCtxQuery{}, &badCtxQuery{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestC3ContextSuggesterSourceFull(t *testing.T) {
	s := NewContextSuggester("ctx-suggest").
		Prefix("hel").
		Field("suggest").
		Size(5).
		ContextQuery(NewSuggesterCategoryQuery("genre", "rock"))
	src, err := s.Source(true)
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "ctx-suggest")

	src2, err := s.Source(false)
	require.NoError(t, err)
	_ = src2
}

func TestC3ContextSuggesterMultipleCtx(t *testing.T) {
	s := NewContextSuggester("test").
		Prefix("hel").
		ContextQueries(
			NewSuggesterCategoryQuery("cat1", "val1"),
			NewSuggesterCategoryQuery("cat2", "val2"),
		)
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3ContextSuggesterCtxNotMap(t *testing.T) {
	s := NewContextSuggester("test").
		Prefix("hel").
		ContextQueries(&badCtxQuery{}, &badCtxQuery{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestC3PhraseSuggesterSourceFull(t *testing.T) {
	script := NewScript("return true").Lang("painless")
	s := NewPhraseSuggester("phrase-suggest").
		Text("noble prize").
		Field("title.trigram").
		Analyzer("standard").
		Size(5).
		ShardSize(10).
		GramSize(3).
		MaxErrors(0.5).
		Separator("|").
		RealWordErrorLikelihood(0.9).
		Confidence(1.0).
		CandidateGenerator(NewDirectCandidateGenerator("title").
			PreFilter("standard").
			PostFilter("reverse").
			SuggestMode("always").
			Accuracy(0.5).
			Size(5).
			Sort("score").
			StringDistance("levenshtein").
			MaxEdits(2).
			MaxInspections(5).
			MaxTermFreq(0.01).
			PrefixLength(1).
			MinWordLength(3).
			MinDocFreq(0.0)).
		SmoothingModel(NewStupidBackoffSmoothingModel(0.4)).
		ForceUnigrams(true).
		TokenLimit(10).
		Highlight("<em>", "</em>").
		CollateQuery(script).
		CollatePreference("_local").
		CollateParams(map[string]any{"query": "test"}).
		CollatePrune(true)
	src, err := s.Source(true)
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "phrase-suggest")
}

func TestC3PhraseSuggesterLaplaceSmoothing(t *testing.T) {
	s := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		SmoothingModel(NewLaplaceSmoothingModel(0.7))
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3PhraseSuggesterLinearInterpolationSmoothing(t *testing.T) {
	s := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		SmoothingModel(NewLinearInterpolationSmoothingModel(0.3, 0.4, 0.3))
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3PhraseSuggesterMultipleCtxErr(t *testing.T) {
	s := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		ContextQueries(&badCtxQuery{}, &badCtxQuery{})
	_, err := s.Source(false)
	require.NoError(t, err)

	s2 := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		ContextQueries(
			NewSuggesterCategoryQuery("c1", "v"),
			NewSuggesterCategoryQuery("c2", "v2"),
		)
	src, err := s2.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3PhraseSuggesterClearGenerator(t *testing.T) {
	s := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		CandidateGenerator(NewDirectCandidateGenerator("title")).
		ClearCandidateGenerator()
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3ScriptFieldIgnoreFailure(t *testing.T) {
	sf := NewScriptField("my_field", NewScript("doc['price'].value * 2")).IgnoreFailure(true)
	src, err := sf.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "script")
	assert.Equal(t, true, m["ignore_failure"])
}

func TestC3ScriptFieldNilScript(t *testing.T) {
	sf := NewScriptField("my_field", nil)
	_, err := sf.Source()
	assert.Error(t, err)
}

func TestC3ScriptSourceAllPaths(t *testing.T) {
	s := NewScript("return params.factor * _score").Lang("painless").Param("factor", 2)
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "source")
	assert.Equal(t, "painless", m["lang"])
	assert.Contains(t, m, "params")

	s2 := NewScriptStored("my_script_id")
	s2.Params(map[string]any{"key": "val"})
	src2, err := s2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)
	assert.Equal(t, "my_script_id", m["id"])

	s3 := &Script{script: "simple"}
	src3, err := s3.Source()
	require.NoError(t, err)
	assert.Equal(t, "simple", src3)
}

func TestC3SearchSourceBodyBranches(t *testing.T) {
	req := NewSearchRequest()
	body, err := req.Body()
	require.NoError(t, err)
	assert.NotNil(t, body)

	raw := json.RawMessage(`{"query":{"match_all":{}}}`)
	rawPtr := &raw
	str := `{"query":{"match_all":{}}}`
	strPtr := &str
	nilStr := (*string)(nil)

	tests := []struct {
		name   string
		source any
	}{
		{"default", nil},
		{"raw", raw},
		{"raw_ptr", rawPtr},
		{"str", str},
		{"str_ptr", strPtr},
		{"nil_str", nilStr},
		{"map", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}},
		{"search_source", NewSearchSource()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewSearchRequest().Source(tc.source)
			body, err := r.Body()
			require.NoError(t, err)
			assert.NotEmpty(t, body)
		})
	}
}

func TestC3ExistsQueryWithName(t *testing.T) {
	q := ExistsQuery{Field: "title", QueryName: "my_exists"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["exists"].(map[string]any)
	assert.Equal(t, "title", m["field"])
	assert.Equal(t, "my_exists", m["_name"])
}

func TestC3IdsQueryAll(t *testing.T) {
	boost := 2.0
	q := NewIdsQuery("1", "2", "3")
	q.Boost = &boost
	q.QueryName = "my_ids"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["ids"].(map[string]any)
	assert.Contains(t, m, "values")
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "my_ids", m["_name"])
}

func TestC3MatchAllQueryWithName(t *testing.T) {
	boost := 1.5
	q := MatchAllQuery{Boost: &boost, QueryName: "my_all"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_all"].(map[string]any)
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "my_all", m["_name"])
}

func TestC3MatchNoneQueryWithName(t *testing.T) {
	q := MatchNoneQuery{QueryName: "my_none"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_none"].(map[string]any)
	assert.Equal(t, "my_none", m["_name"])
}

func TestC3MatchQueryCompact(t *testing.T) {
	q := NewMatchQuery("title", "hello")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match"].(map[string]any)
	assert.NotNil(t, m["title"])
}

func TestC3MatchQueryFull(t *testing.T) {
	prefixLen := int(2)
	maxExp := int(25)
	cutoff := 0.001
	boost := 1.5
	lenient := true
	transitions := true
	q := NewMatchQuery("title", "hello")
	q.Analyzer = "standard"
	q.Operator = "and"
	q.Fuzziness = "AUTO"
	q.PrefixLength = &prefixLen
	q.MaxExpansions = &maxExp
	q.MinimumShouldMatch = "75%"
	q.FuzzyRewrite = "constant_score_boolean"
	q.Lenient = &lenient
	q.FuzzyTranspositions = &transitions
	q.ZeroTermsQuery = "all"
	q.CutoffFrequency = &cutoff
	q.Boost = &boost
	q.QueryName = "my_match"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match"].(map[string]any)
	assert.NotNil(t, m["title"])
}

func TestC3MatchPhraseQueryFull(t *testing.T) {
	slop := int(2)
	boost := 1.5
	q := NewMatchPhraseQuery("title", "hello world")
	q.Analyzer = "standard"
	q.Slop = &slop
	q.Boost = &boost
	q.QueryName = "my_phrase"
	q.ZeroTermsQuery = "all"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_phrase"].(map[string]any)
	assert.NotNil(t, m["title"])
}

func TestC3MatchPhrasePrefixQueryFull(t *testing.T) {
	slop := int(0)
	maxExp := int(50)
	boost := 1.0
	q := NewMatchPhrasePrefixQuery("title", "quick brown")
	q.Analyzer = "standard"
	q.Slop = &slop
	q.MaxExpansions = &maxExp
	q.Boost = &boost
	q.QueryName = "my_prefix_phrase"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_phrase_prefix"].(map[string]any)
	assert.NotNil(t, m["title"])
}

func TestC3MatchBoolPrefixQueryFull(t *testing.T) {
	prefixLen := int(1)
	maxExp := int(10)
	boost := 2.0
	transitions := true
	q := NewMatchBoolPrefixQuery("title", "quick bro")
	q.Analyzer = "standard"
	q.MinimumShouldMatch = "1"
	q.Operator = "and"
	q.Fuzziness = "AUTO"
	q.PrefixLength = &prefixLen
	q.MaxExpansions = &maxExp
	q.FuzzyTranspositions = &transitions
	q.FuzzyRewrite = "constant_score"
	q.Boost = &boost
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_bool_prefix"].(map[string]any)
	assert.NotNil(t, m["title"])
}

func TestC3NestedQueryFull(t *testing.T) {
	boost := 2.0
	iu := true
	q := NewNestedQuery("comments", NewTermQuery("comments.author", "john"))
	q.ScoreMode = "avg"
	q.Boost = &boost
	q.QueryName = "my_nested"
	q.IgnoreUnmapped = &iu
	q.InnerHit = NewInnerHit().Name("inner_comments").Size(3)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["nested"].(map[string]any)
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, true, m["ignore_unmapped"])
}

func TestC3ParentIdQueryFull(t *testing.T) {
	boost := 1.5
	iu := true
	q := NewParentIdQuery("answer", "parent123")
	q.Boost = &boost
	q.QueryName = "my_parent_id"
	q.IgnoreUnmapped = &iu
	q.InnerHit = NewInnerHit().Name("inner_hit").Size(2)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["parent_id"].(map[string]any)
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, true, m["ignore_unmapped"])
}

func TestC3PinnedQueryFull(t *testing.T) {
	q := NewPinnedQuery()
	q.IDs = []string{"1", "2"}
	q.Organic = NewTermQuery("type", "article")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["pinned"].(map[string]any)
	assert.Contains(t, m, "ids")
	assert.Contains(t, m, "organic")
}

func TestC3PrefixQueryFull(t *testing.T) {
	boost := 2.0
	caseIn := true
	q := NewPrefixQuery("title", "pre")
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.QueryName = "my_prefix"
	q.CaseInsensitive = &caseIn
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["prefix"].(map[string]any)
	mm := m["title"].(map[string]any)
	assert.NotNil(t, mm)
}

func TestC3RankFeatureQueryFull(t *testing.T) {
	boost := 2.0
	q := RankFeatureQuery{
		Field:         "pagerank",
		Boost:         &boost,
		QueryName:     "my_rank",
		ScoreFunction: RankFeatureLogScoreFunction{ScalingFactor: 10.0},
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["rank_feature"].(map[string]any)
	assert.Contains(t, m, "log")
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "my_rank", m["_name"])

	q2 := RankFeatureQuery{Field: "f", ScoreFunction: NewRankFeatureSaturationScoreFunction()}
	src2, err := q2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)["rank_feature"].(map[string]any)
	assert.Contains(t, m, "saturation")

	q3 := RankFeatureQuery{Field: "f", ScoreFunction: NewRankFeatureSigmoidScoreFunction(1.0, 2.0)}
	src3, err := q3.Source()
	require.NoError(t, err)
	m = src3.(map[string]any)["rank_feature"].(map[string]any)
	assert.Contains(t, m, "sigmoid")

	q4 := RankFeatureQuery{Field: "f", ScoreFunction: NewRankFeatureLinearScoreFunction()}
	src4, err := q4.Source()
	require.NoError(t, err)
	m = src4.(map[string]any)["rank_feature"].(map[string]any)
	assert.Contains(t, m, "linear")
}

func TestC3RegexpQueryFull(t *testing.T) {
	boost := 2.0
	mds := int(10000)
	caseIn := true
	q := NewRegexpQuery("title", "[a-z]+")
	q.Flags = "ALL"
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.CaseInsensitive = &caseIn
	q.MaxDeterminizedStates = &mds
	q.QueryName = "my_regexp"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["regexp"].(map[string]any)
	mm := m["title"].(map[string]any)
	assert.NotNil(t, mm)
}

func TestC3ScriptQueryFull(t *testing.T) {
	q := ScriptQuery{Script: NewScript("doc['likes'].value > 10"), QueryName: "my_script"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["script"].(map[string]any)
	assert.Contains(t, m, "script")
	assert.Equal(t, "my_script", m["_name"])

	q2 := ScriptQuery{}
	src2, err := q2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)["script"].(map[string]any)
	assert.NotContains(t, m, "script")
}

func TestC3ScriptScoreQueryFull(t *testing.T) {
	boost := 1.5
	minScore := 2.0
	q := NewScriptScoreQuery(NewMatchAllQuery(), NewScript("_score * doc['likes'].value"))
	q.MinScore = &minScore
	q.Boost = &boost
	q.QueryName = "my_script_score"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["script_score"].(map[string]any)
	assert.Contains(t, m, "query")
	assert.Contains(t, m, "script")
}

func TestC3SimpleQueryStringQueryFull(t *testing.T) {
	boost := 2.0
	lower := true
	lenient := true
	anWc := true
	transitions := true
	agspq := true
	q := NewSimpleQueryStringQuery("\"fried eggs\" +potato")
	q.Fields = []string{"title", "body"}
	q.FieldBoosts["title"] = func() *float64 { b := 3.0; return &b }()
	q.Analyzer = "standard"
	q.QuoteFieldSuffix = ".exact"
	q.DefaultOperator = "AND"
	q.MinimumShouldMatch = "2"
	q.Flags = "PREFIX|PHRASE"
	q.Boost = &boost
	q.LowercaseExpandedTerms = &lower
	q.Lenient = &lenient
	q.AnalyzeWildcard = &anWc
	q.Locale = "en-US"
	q.QueryName = "my_sqs"
	q.AutoGenerateSynonymsPhraseQuery = &agspq
	q.FuzzyPrefixLength = 2
	q.FuzzyMaxExpansions = 50
	q.FuzzyTranspositions = &transitions
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["simple_query_string"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Equal(t, "standard", m["analyzer"])
}

func TestC3SimpleQueryStringQueryNoFields(t *testing.T) {
	q := NewSimpleQueryStringQuery("hello world")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["simple_query_string"].(map[string]any)
	assert.NotContains(t, m, "fields")
}

func TestC3SpanFirstQueryFull(t *testing.T) {
	boost := 2.0
	q := SpanFirstQuery{
		Match:     NewTermQuery("body", "quick"),
		End:       3,
		Boost:     &boost,
		QueryName: "my_span_first",
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_first"].(map[string]any)
	assert.Contains(t, m, "match")
	assert.Equal(t, 2.0, m["boost"])
}

func TestC3SpanNearQueryFull(t *testing.T) {
	slop := int(2)
	inOrder := true
	boost := 2.0
	q := SpanNearQuery{
		Clauses:   []Query{NewTermQuery("body", "quick"), NewTermQuery("body", "fox")},
		Slop:      &slop,
		InOrder:   &inOrder,
		Boost:     &boost,
		QueryName: "my_span_near",
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_near"].(map[string]any)
	assert.Contains(t, m, "clauses")
}

func TestC3SpanTermQueryFull(t *testing.T) {
	boost := 2.0
	q := SpanTermQuery{
		Field:         "body",
		spanTermInner: spanTermInner{Value: "quick", Boost: &boost, QueryName: "my_span_term"},
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_term"].(map[string]any)
	mm := m["body"].(map[string]any)
	assert.Equal(t, "quick", mm["value"])
}

func TestC3TermsQueryLookup(t *testing.T) {
	q := TermsQuery{
		Field:       "user",
		TermsLookup: NewTermsLookup().Index("users").Id("2").Path("followers"),
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms"].(map[string]any)
	assert.Contains(t, m, "user")
}

func TestC3TermsQueryFull(t *testing.T) {
	boost := 2.0
	q := NewTermsQuery("user", "john", "jane")
	q.Boost = &boost
	q.QueryName = "my_terms"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms"].(map[string]any)
	assert.NotNil(t, m["user"])
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "my_terms", m["_name"])
}

func TestC3TermsSetQueryFull(t *testing.T) {
	boost := 2.0
	q := NewTermsSetQuery("languages", "java", "python")
	q.MinimumShouldMatchField = "required_matches"
	q.MinimumShouldMatchScript = NewScript("Math.min(params.num_terms, doc['required_matches'].value)")
	q.Boost = &boost
	q.QueryName = "my_terms_set"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms_set"].(map[string]any)
	mm := m["languages"].(map[string]any)
	assert.Contains(t, mm, "minimum_should_match_field")
	assert.Contains(t, mm, "minimum_should_match_script")
}

func TestC3TypeQuerySource(t *testing.T) {
	q := NewTypeQuery("my_type")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["type"].(map[string]any)
	assert.Equal(t, "my_type", m["value"])
}

func TestC3WildcardQueryFull(t *testing.T) {
	boost := 2.0
	caseIn := true
	q := NewWildcardQuery("name", "ki*y")
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.QueryName = "my_wildcard"
	q.CaseInsensitive = &caseIn
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["wildcard"].(map[string]any)
	mm := m["name"].(map[string]any)
	assert.NotNil(t, mm)
}

func TestC3FieldSortNested(t *testing.T) {
	ns := NewNestedSort("comments").
		Filter(NewTermQuery("comments.lang", "en")).
		NestedSort(NewNestedSort("comments.likes"))
	s := NewFieldSort("comments.likes.count").Nested(ns)
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m)
}

func TestC3GeoDistanceSortFull(t *testing.T) {
	iu := true
	dt := "arc"
	mode := "min"
	s := NewGeoDistanceSort("location").
		Point(40.7128, -74.0060).
		GeoHashes("drm3btev3e86").
		Unit("km").
		IgnoreUnmapped(iu).
		DistanceType(dt).
		SortMode(mode).
		Order(true).
		NestedFilter(NewTermQuery("type", "geo")).
		NestedPath("location").
		NestedSort(NewNestedSort("location").Filter(NewMatchAllQuery()))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m)
}

func TestC3ScriptSortFull(t *testing.T) {
	mode := "min"
	s := NewScriptSort(NewScript("doc['likes'].value"), "number").
		Order(true).
		SortMode(mode).
		NestedFilter(NewTermQuery("type", "article")).
		NestedPath("comments").
		NestedSort(NewNestedSort("comments"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m)
}

func TestC3ScriptSortNilScript(t *testing.T) {
	s := &ScriptSort{typ: "number"}
	_, err := s.Source()
	assert.Error(t, err)
}

func TestC3SortInfoFull(t *testing.T) {
	iu := true
	info := SortInfo{
		Field:          "title",
		Ascending:      false,
		Missing:        "_last",
		IgnoreUnmapped: &iu,
		UnmappedType:   "long",
		SortMode:       "min",
		Filter:         NewTermQuery("status", "active"),
		Path:           "comments",
		Nested:         NewNestedSort("comments"),
	}
	src, err := info.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m)
}

func TestC3SortInfoDeprecated(t *testing.T) {
	info := SortInfo{
		Field:        "title",
		Ascending:    true,
		NestedFilter: NewTermQuery("status", "active"),
		NestedPath:   "comments",
	}
	src, err := info.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m)
}

func TestC3SignificantTextAggFull(t *testing.T) {
	filterDup := true
	heuristic := NewJLHScoreSignificanceHeuristic()
	a := SignificantTextAggregation{
		FieldVal:            "body",
		SourceFieldNames:    []string{"body"},
		FilterDuplicateText: &filterDup,
		Filter:              NewTermQuery("type", "article"),
		Heuristic:           heuristic,
		BucketCountThresholds: &BucketCountThresholds{
			RequiredSize:     ptrInt(10),
			ShardSize:        ptrInt(100),
			MinDocCount:      ptrInt64(5),
			ShardMinDocCount: ptrInt64(2),
		},
		IncludeExclude: &TermsAggregationIncludeExclude{Include: "word.*", Exclude: "stop.*"},
		Meta:           map[string]any{"k": "v"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["significant_text"].(map[string]any)
	assert.Contains(t, m, "background_filter")
	assert.Contains(t, m, "include")
	assert.Contains(t, m, "exclude")
	assert.Equal(t, 10, m["size"])
	assert.Equal(t, 100, m["shard_size"])
	assert.Equal(t, int64(5), m["min_doc_count"])
	assert.Equal(t, int64(2), m["shard_min_doc_count"])
}

func TestC3AdjacencyMatrixAggFull(t *testing.T) {
	a := NewAdjacencyMatrixAggregation()
	a.Filters = map[string]Query{
		"grpA": NewTermQuery("type", "A"),
		"grpB": NewTermQuery("type", "B"),
	}
	a.SubAggs = map[string]Aggregation{"avg": AvgAggregation{Field: "score"}}
	a.Meta = map[string]any{"k": "v"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["adjacency_matrix"].(map[string]any)
	assert.Contains(t, m, "filters")
}

func TestC3PhraseSuggesterMultipleCtxNonErr(t *testing.T) {
	s := NewPhraseSuggester("test").
		Text("hello").
		Field("body").
		ContextQueries(
			NewSuggesterCategoryQuery("c1", "v"),
			NewSuggesterCategoryQuery("c2", "v2"),
		)
	src, err := s.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestC3SearchRequestSource(t *testing.T) {
	src := NewSearchSource().Query(NewMatchAllQuery())
	data, err := src.Source()
	require.NoError(t, err)
	_ = data
}

func TestC3NestedSortErr(t *testing.T) {
	ns := NewNestedSort("comments").
		Filter(mockQueryError{}).
		NestedSort(&NestedSort{path: "inner"})
	_, err := ns.Source()
	assert.Error(t, err)

	ns2 := NewNestedSort("comments").
		NestedSort(&NestedSort{path: "inner"})
	src, err := ns2.Source()
	require.NoError(t, err)
	assert.Contains(t, src.(map[string]any), "nested")
}

func TestC3HighlightErrors(t *testing.T) {
	f1 := NewHighlighterField("body").HighlightQuery(mockQueryError{})
	hl := NewHighlight().Fields(f1).UseExplicitFieldOrder(true)
	_, err := hl.Source()
	assert.Error(t, err)

	f2 := NewHighlighterField("body").HighlightQuery(mockQueryError{})
	hl2 := NewHighlight().Fields(f2)
	_, err = hl2.Source()
	assert.Error(t, err)

	hl3 := NewHighlight().HighlightQuery(mockQueryError{})
	_, err = hl3.Source()
	assert.Error(t, err)
}

func TestC3AggErrorPaths(t *testing.T) {
	_, err := AdjacencyMatrixAggregation{Filters: map[string]Query{"bad": mockQueryError{}}}.Source()
	assert.Error(t, err)

	_, err = CompositeAggregation{ValuesSources: []CompositeAggregationValuesSource{mockQueryError{}}}.Source()
	assert.Error(t, err)
}

func TestC3FiltersAggErrorPath(t *testing.T) {
	_, err := FiltersAggregation{NamedFilters: map[string]Query{"bad": mockQueryError{}}}.Source()
	assert.Error(t, err)

	_, err = FiltersAggregation{UnnamedFilters: []Query{mockQueryError{}}}.Source()
	assert.Error(t, err)
}

func TestC3SignificantTermsErrors(t *testing.T) {
	_, err := SignificantTermsAggregation{Filter: mockQueryError{}}.Source()
	assert.Error(t, err)

	_, err = SignificantTermsAggregation{Heuristic: &mockSignificanceHeuristicError{}}.Source()
	assert.Error(t, err)
}

func TestC3SignificantTextErrors(t *testing.T) {
	_, err := SignificantTextAggregation{Filter: mockQueryError{}}.Source()
	assert.Error(t, err)

	_, err = SignificantTextAggregation{Heuristic: &mockSignificanceHeuristicError{}}.Source()
	assert.Error(t, err)
}

func TestC3WeightedAvgErrors(t *testing.T) {
	_, err := WeightedAvgAggregation{
		Fields: map[string]*MultiValuesSourceFieldConfig{"f": {FieldName: ""}},
	}.Source()
	require.NoError(t, err)

	_, err = WeightedAvgAggregation{Value: &MultiValuesSourceFieldConfig{FieldName: "v"}}.Source()
	require.NoError(t, err)

	_, err = WeightedAvgAggregation{Weight: &MultiValuesSourceFieldConfig{FieldName: "w"}}.Source()
	require.NoError(t, err)
}

func TestC3MoreLikeThisErrors(t *testing.T) {
	q := &MoreLikeThisQuery{docs: []*MoreLikeThisQueryItem{{id: "1"}}}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["more_like_this"].(map[string]any)
	assert.Contains(t, m, "like")

	q2 := &MoreLikeThisQuery{
		docs:       []*MoreLikeThisQueryItem{{id: "1"}},
		unlikeDocs: []*MoreLikeThisQueryItem{{id: "2"}},
	}
	src2, err := q2.Source()
	require.NoError(t, err)
	m = src2.(map[string]any)["more_like_this"].(map[string]any)
	assert.Contains(t, m, "unlike")
}

func TestC3FSQFilterErr(t *testing.T) {
	q := NewFunctionScoreQuery()
	q.Add(mockQueryError{}, NewRandomFunction())
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC3IntervalFilterErrors(t *testing.T) {
	er := mockIntervalRuleError{}
	for _, f := range []*IntervalQueryFilter{
		{before: er},
		{after: er},
		{containedBy: er},
		{containing: er},
		{overlapping: er},
		{notContainedBy: er},
		{notContaining: er},
		{notOverlapping: er},
	} {
		_, err := f.Source()
		assert.Error(t, err)
	}
}

func TestC3IntervalAllOfErrors(t *testing.T) {
	er := mockIntervalRuleError{}
	_, err := NewIntervalQueryRuleAllOf(er).Source()
	assert.Error(t, err)

	src, err := NewIntervalQueryRuleAllOf().Filter(NewIntervalQueryFilter()).Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestC3IntervalAnyOfErrors(t *testing.T) {
	er := mockIntervalRuleError{}
	_, err := NewIntervalQueryRuleAnyOf(er).Source()
	assert.Error(t, err)

	src, err := NewIntervalQueryRuleAnyOf().Filter(NewIntervalQueryFilter()).Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestC3SearchSourceErrors(t *testing.T) {
	_, err := NewSearchSource().Query(mockQueryError{}).Source()
	assert.Error(t, err)

	_, err = NewSearchSource().PostFilter(mockQueryError{}).Source()
	assert.Error(t, err)
}

func TestC3SearchSourceProfileSeqNo(t *testing.T) {
	v := true
	src := &SearchSource{profile: true, seqNoAndPrimaryTerm: &v, sliceQuery: NewMatchAllQuery()}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, true, m["profile"])
	assert.Equal(t, true, m["seq_no_primary_term"])
}

func TestC3SearchSourceSlice(t *testing.T) {
	src := &SearchSource{sliceQuery: NewTermQuery("t", "v")}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "slice")
}

func TestC3SearchSourceCollapseErr(t *testing.T) {
	src := NewSearchSource().Collapse(NewCollapseBuilder("user").InnerHit(NewInnerHit()))
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "collapse")
}

func TestC3SearchSourceSuggesters(t *testing.T) {
	src := NewSearchSource().
		GlobalSuggestText("global").
		Suggester(NewCompletionSuggester("s1").Text("hello").Field("suggest")).
		Suggester(NewTermSuggester("s2").Text("world").Field("body")).
		Suggester(NewPhraseSuggester("s3").Text("foo").Field("body"))
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "suggest")
}

func TestC3SearchSourceInnerHits(t *testing.T) {
	src := NewSearchSource().InnerHit("my_hit", NewInnerHit().Name("inner"))
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "inner_hits")
}

func TestC3SearchSourceRescoreDefault(t *testing.T) {
	dw := 50
	src := &SearchSource{
		defaultRescoreWindowSize: &dw,
		rescores:                 []*Rescore{NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))},
	}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "rescore")
}

func TestC3SearchSourceIndicesBoost(t *testing.T) {
	src := NewSearchSource().IndexBoosts(IndexBoost{Index: "idx", Boost: 1.5})
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "indices_boost")
}

func TestC3SearchSourceStats(t *testing.T) {
	src := NewSearchSource().Stats("grp1", "grp2")
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "stats")
}

func TestC3SearchSourceStoredFields(t *testing.T) {
	src := NewSearchSource().StoredField("title").StoredFields("body")
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "stored_fields")
}

func TestC3SearchSourceNoStoredFields(t *testing.T) {
	src := NewSearchSource().NoStoredFields()
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "stored_fields")
}

func TestC3SearchSourceDocvalueFields(t *testing.T) {
	src := NewSearchSource().
		DocvalueFields("timestamp").
		DocvalueFieldWithFormat(DocvalueField{Field: "date", Format: "epoch_millis"}).
		DocvalueField("name")
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "docvalue_fields")
}

func TestC3SearchSourceSearchAfter(t *testing.T) {
	src := NewSearchSource().SearchAfter(1234, "doc123")
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "search_after")
}

func TestC3SearchSourceSearchAfterEmpty(t *testing.T) {
	src := NewSearchSource().SearchAfter()
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.NotContains(t, m, "search_after")
}

func TestC3SearchSourcePit(t *testing.T) {
	pit := &PointInTime{Id: "abc", KeepAlive: "1m"}
	src := NewSearchSource().PointInTime(pit)
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "pit")
}

func TestC3SearchSourceScriptFields(t *testing.T) {
	src := NewSearchSource().ScriptField(NewScriptField("f", NewScript("1")))
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "script_fields")
}

func TestC3SearchSourceMinScore(t *testing.T) {
	ms := 1.5
	src := &SearchSource{minScore: &ms}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, 1.5, m["min_score"])
}

func TestC3SearchSourceTimeoutTerminate(t *testing.T) {
	ta := 100
	src := &SearchSource{timeout: "5s", terminateAfter: &ta}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, "5s", m["timeout"])
	assert.Equal(t, 100, m["terminate_after"])
}

func TestC3SearchSourceFetchSource(t *testing.T) {
	fsc := NewFetchSourceContext(true).Include("title").Exclude("secret")
	src := NewSearchSource().FetchSourceContext(fsc)
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "_source")
}

func TestC3SearchSourceNoFetch(t *testing.T) {
	src := NewSearchSource().FetchSource(false)
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Contains(t, m, "_source")
}

func TestC3SearchSourceTrackScores(t *testing.T) {
	v := true
	src := &SearchSource{trackScores: &v}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, true, m["track_scores"])
}

func TestC3SearchSourceTrackTotalHits(t *testing.T) {
	src := &SearchSource{trackTotalHits: 1000}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, 1000, m["track_total_hits"])
}

func TestC3SearchSourceTrackTotalHitsBool(t *testing.T) {
	src := &SearchSource{trackTotalHits: false}
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, false, m["track_total_hits"])
}

func TestC3SearchSourceFF(t *testing.T) {
	src := NewSearchSource().
		FieldWithFormat(FieldField{Field: "ts", Format: "epoch_millis"}).
		FieldsWithFormat(FieldField{Field: "name"})
	out, err := src.Source()
	require.NoError(t, err)
	m := out.(map[string]any)
	_ = m
}

func TestC3FieldSortPathError(t *testing.T) {
	path := "nested_path"
	s := &FieldSort{fieldName: "f", path: &path, filter: mockQueryError{}}
	_, err := s.Source()
	assert.Error(t, err)
}

func TestC3GeoDistanceSortFilterErr(t *testing.T) {
	s := NewGeoDistanceSort("loc").NestedFilter(mockQueryError{})
	_, err := s.Source()
	assert.Error(t, err)
}

func TestC3ScriptSortFilterErr(t *testing.T) {
	s := NewScriptSort(NewScript("1"), "number").NestedFilter(mockQueryError{})
	_, err := s.Source()
	assert.Error(t, err)

	s2 := NewScriptSort(NewScript("1"), "number").NestedSort(NewNestedSort("p").Filter(mockQueryError{}))
	_, err = s2.Source()
	assert.Error(t, err)
}

func TestC3IntervalMatchErr(t *testing.T) {
	r := NewIntervalQueryRuleMatch("hello").Filter(&IntervalQueryFilter{before: mockIntervalRuleError{}})
	_, err := r.Source()
	assert.Error(t, err)
}

func TestC3SearchSourceHighlightErr(t *testing.T) {
	src := NewSearchSource().Highlight(NewHighlight().HighlightQuery(mockQueryError{}))
	_, err := src.Source()
	assert.Error(t, err)
}
