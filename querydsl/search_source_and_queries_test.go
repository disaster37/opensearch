// search_source_and_queries_test.go covers SearchRequest and SearchSource,
// the two top-level query orchestration types, plus additional query
// builders that are composites (BoostingQuery, CommonTermsQuery,
// ConstantScoreQuery, DisMaxQuery) and helper serialization functions
// (marshalStruct, sourcePipeline, sourceAgg).
package querydsl

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestC2NewBoostingQuery(t *testing.T) {
	q := NewBoostingQuery()
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["boosting"].(map[string]any)
	assert.NotNil(t, m)
	q.Positive = NewMatchAllQuery()
	q.Negative = NewMatchAllQuery()
	nb := 0.3
	q.NegativeBoost = &nb
	b := 1.2
	q.Boost = &b
	src, err = q.Source()
	require.NoError(t, err)
	m = src.(map[string]any)["boosting"].(map[string]any)
	assert.Contains(t, m, "positive")
	assert.Contains(t, m, "negative")
}

func TestC2BoostingQuery_Error(t *testing.T) {
	q := BoostingQuery{Positive: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
	q2 := BoostingQuery{Negative: mockQueryError{}}
	_, err = q2.Source()
	assert.Error(t, err)
}

func TestC2NewCommonTermsQuery(t *testing.T) {
	q := NewCommonTermsQuery("message", "hello")
	assert.Equal(t, "message", q.Field)
	assert.Equal(t, "hello", q.Query)
	cutoff := 0.001
	q.CutoffFrequency = &cutoff
	highFreq := 0.9
	q.HighFreq = &highFreq
	q.HighFreqOperator = "and"
	q.HighFreqMinimumShouldMatch = "75%"
	lowFreq := 0.5
	q.LowFreq = &lowFreq
	q.LowFreqOperator = "or"
	q.LowFreqMinimumShouldMatch = "2"
	q.Analyzer = "standard"
	boost := 1.5
	q.Boost = &boost
	q.QueryName = "ctq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["common"].(map[string]any)["message"].(map[string]any)
	assert.Equal(t, "hello", m["query"])
	assert.Equal(t, 0.001, m["cutoff_frequency"])
	assert.Equal(t, "and", m["high_freq_operator"])
	assert.Contains(t, m, "minimum_should_match")
	assert.Equal(t, "standard", m["analyzer"])
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "ctq", m["_name"])
	mm := m["minimum_should_match"].(map[string]any)
	assert.Equal(t, "75%", mm["high_freq"])
	assert.Equal(t, "2", mm["low_freq"])
}

func TestC2CommonTermsQuery_OnlyLowFreqMSM(t *testing.T) {
	q := NewCommonTermsQuery("f", "v")
	q.LowFreqMinimumShouldMatch = "1"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["common"].(map[string]any)["f"].(map[string]any)
	mm := m["minimum_should_match"].(map[string]any)
	assert.Equal(t, "1", mm["low_freq"])
	_, hasHigh := mm["high_freq"]
	assert.False(t, hasHigh)
}

func TestC2NewConstantScoreQuery(t *testing.T) {
	q := NewConstantScoreQuery(NewTermQuery("status", "active"))
	assert.NotNil(t, q.Filter)
	boost := 2.5
	q.Boost = &boost
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["constant_score"].(map[string]any)
	assert.Contains(t, m, "filter")
	assert.Equal(t, 2.5, m["boost"])
}

func TestC2ConstantScoreQuery_Error(t *testing.T) {
	q := ConstantScoreQuery{Filter: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2NewDisMaxQuery(t *testing.T) {
	q := NewDisMaxQuery()
	assert.Empty(t, q.Queries)
	q.Queries = []Query{NewMatchAllQuery(), NewMatchAllQuery()}
	tb := 0.7
	q.TieBreaker = &tb
	boost := 1.5
	q.Boost = &boost
	q.QueryName = "dmq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["dis_max"].(map[string]any)
	assert.Equal(t, 0.7, m["tie_breaker"])
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "dmq", m["_name"])
	assert.Len(t, m["queries"], 2)
}

func TestC2DisMaxQuery_Error(t *testing.T) {
	q := DisMaxQuery{Queries: []Query{mockQueryError{}}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2GaussDecayGetWeight(t *testing.T) {
	fn := NewGaussDecayFunction()
	assert.Nil(t, fn.GetWeight())
	fn.Weight(0.5)
	w := fn.GetWeight()
	require.NotNil(t, w)
	assert.Equal(t, 0.5, *w)
}

func TestC2ScriptFunctionGetWeight(t *testing.T) {
	fn := NewScriptFunction(nil)
	assert.Nil(t, fn.GetWeight())
	fn.Weight(2.0)
	w := fn.GetWeight()
	require.NotNil(t, w)
	assert.Equal(t, 2.0, *w)
}

func TestC2FieldValueFactorGetWeight(t *testing.T) {
	fn := NewFieldValueFactorFunction()
	assert.Nil(t, fn.GetWeight())
	fn.Weight(3.0)
	w := fn.GetWeight()
	require.NotNil(t, w)
	assert.Equal(t, 3.0, *w)
}

func TestC2MarshalStruct_ErrorPaths(t *testing.T) {
	t.Run("marshal_error", func(t *testing.T) {
		_, err := marshalStruct(make(chan int))
		assert.Error(t, err)
	})
	t.Run("unmarshal_not_object", func(t *testing.T) {
		_, err := marshalStruct(42)
		assert.Error(t, err)
	})
	t.Run("unmarshal_array", func(t *testing.T) {
		_, err := marshalStruct([]int{1, 2})
		assert.Error(t, err)
	})
	t.Run("unmarshal_string", func(t *testing.T) {
		_, err := marshalStruct("hello")
		assert.Error(t, err)
	})
}

func TestC2SourcePipeline_ScriptError(t *testing.T) {
	badScript := &Script{}
	badScript.script = ""
	badScript.typ = "badtype"
	body := map[string]any{"buckets_path": "agg1"}
	_, err := sourcePipeline("test_agg", body, nil, badScript)
	if err != nil {
		assert.Error(t, err)
	}
}

func TestC2SourceAgg_SubAggError(t *testing.T) {
	body := map[string]any{"field": "x"}
	subs := map[string]Aggregation{"bad": mockAggregationError{}}
	_, err := sourceAgg("test_agg", body, subs, nil, nil)
	assert.Error(t, err)
}

func TestC2SearchRequest_SearchSource_Nil(t *testing.T) {
	req := NewSearchRequest()
	req.SearchSource(nil)
	assert.NotNil(t, req.searchSource)
}

func TestC2SearchRequest_SearchSource_Custom(t *testing.T) {
	req := NewSearchRequest()
	custom := NewSearchSource().Size(42)
	req.SearchSource(custom)
	body, err := req.Body()
	require.NoError(t, err)
	assert.Contains(t, body, "42")
}

func TestC2SearchSource_Source_AllFields(t *testing.T) {
	ss := NewSearchSource()
	ss.From(0).
		Size(10).
		Timeout("1s").
		TerminateAfter(100).
		Query(NewMatchAllQuery()).
		PostFilter(NewTermQuery("status", "active")).
		MinScore(0.5).
		Version(true).
		Explain(true).
		Profile(true).
		FetchSource(true).
		FetchSourceIncludeExclude([]string{"obj.*"}, []string{"*.secret"}).
		StoredField("sf1").
		StoredFields("sf2", "sf3").
		DocvalueField("dv1").
		DocvalueFieldWithFormat(DocvalueField{Field: "dv2", Format: "epoch_millis"}).
		DocvalueFields("dv3").
		DocvalueFieldsWithFormat(DocvalueField{Field: "dv4"}).
		Field("f1").
		FieldWithFormat(FieldField{Field: "f2", Format: "fmt"}).
		Fields("f3", "f4").
		FieldsWithFormat(FieldField{Field: "f5"}).
		ScriptField(NewScriptField("sf", NewScriptInline("1"))).
		ScriptFields(NewScriptField("sf2", NewScriptInline("2"))).
		Sort("ts", true).
		SortWithInfo(SortInfo{Field: "x", Ascending: false}).
		SortBy(NewFieldSort("ts2")).
		TrackScores(true).
		TrackTotalHits(true).
		SearchAfter(123, "abc").
		Slice(NewSliceQuery()).
		IndexBoost("idx1", 1.5).
		IndexBoosts(IndexBoost{Index: "idx2", Boost: 2.0}).
		Aggregation("terms", NewTermsAggregation().WithField("genre")).
		Highlight(NewHighlight().Field("title")).
		GlobalSuggestText("hello").
		Suggester(NewTermSuggester("s1").Field("title").Text("hello")).
		Rescorer(NewRescore().WindowSize(10).Rescorer(NewQueryRescorer(NewMatchAllQuery()))).
		Stats("group1").
		Collapse(NewCollapseBuilder("user")).
		SeqNoAndPrimaryTerm(true).
		PointInTime(NewPointInTime("abc123"))

	ss.InnerHit("hit1", NewInnerHit().Path("comments").Name("ch"))
	ss.InnerHit("hit2", NewInnerHit().Type("answer").Name("ch2"))
	ss.InnerHit("hit3", NewInnerHit().Name("no_path"))

	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 0, m["from"])
	assert.Equal(t, 10, m["size"])
	assert.Equal(t, "1s", m["timeout"])
	assert.Equal(t, 100, m["terminate_after"])
	assert.Contains(t, m, "query")
	assert.Contains(t, m, "post_filter")
	assert.Equal(t, 0.5, m["min_score"])
	assert.Equal(t, true, m["version"])
	assert.Equal(t, true, m["explain"])
	assert.Equal(t, true, m["profile"])
	assert.Contains(t, m, "_source")
	assert.Contains(t, m, "stored_fields")
	assert.Contains(t, m, "docvalue_fields")
	assert.Contains(t, m, "fields")
	assert.Contains(t, m, "script_fields")
	assert.Contains(t, m, "sort")
	assert.Equal(t, true, m["track_scores"])
	assert.Equal(t, true, m["track_total_hits"])
	assert.Contains(t, m, "search_after")
	assert.Contains(t, m, "slice")
	assert.Contains(t, m, "indices_boost")
	assert.Contains(t, m, "aggregations")
	assert.Contains(t, m, "highlight")
	assert.Contains(t, m, "suggest")
	assert.Contains(t, m, "rescore")
	assert.Equal(t, []string{"group1"}, m["stats"])
	assert.Contains(t, m, "collapse")
	assert.Equal(t, true, m["seq_no_primary_term"])
	assert.Contains(t, m, "inner_hits")
	assert.Contains(t, m, "pit")
}

func TestC2SearchSource_MarshalJSON_Nil(t *testing.T) {
	var ss *SearchSource
	data, err := ss.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestC2SearchSource_MarshalJSON_Valid(t *testing.T) {
	ss := NewSearchSource().Size(5)
	data, err := ss.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), "size")
}

func TestC2Script_Source_AllBranches(t *testing.T) {
	t.Run("empty_script_plain", func(t *testing.T) {
		s := &Script{}
		src, err := s.Source()
		require.NoError(t, err)
		assert.Equal(t, "", src)
	})
	t.Run("inline_with_lang_params", func(t *testing.T) {
		s := NewScript("doc['x'].value").Lang("painless").Param("factor", 1.5)
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "painless", m["lang"])
		assert.Contains(t, m, "params")
		assert.Contains(t, m, "source")
	})
	t.Run("stored", func(t *testing.T) {
		s := NewScriptStored("calc").Lang("painless").Param("a", 1)
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "calc", m["id"])
	})
	t.Run("json_prefix_script", func(t *testing.T) {
		s := NewScript(`{"inline":"x"}`)
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "source")
	})
	t.Run("quoted_prefix_script", func(t *testing.T) {
		s := NewScript(`"return 1"`)
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "source")
	})
}

func TestC2Script_Param_NilMap(t *testing.T) {
	s := &Script{}
	s.Param("k", "v")
	assert.Equal(t, "v", s.params["k"])
}

func TestC2InnerHit_Source_AllBranches(t *testing.T) {
	t.Run("with_name", func(t *testing.T) {
		ih := NewInnerHit().Name("my_inner").Size(5).From(0)
		src, err := ih.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "my_inner", m["name"])
		assert.Equal(t, 5, m["size"])
		assert.Equal(t, 0, m["from"])
	})
	t.Run("no_name", func(t *testing.T) {
		ih := NewInnerHit().Path("comments")
		src, err := ih.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, hasName := m["name"]
		assert.False(t, hasName)
	})
	t.Run("many_options", func(t *testing.T) {
		ih := NewInnerHit().
			Path("comments").
			Type("answer").
			Name("ih1").
			Query(NewMatchAllQuery()).
			Collapse(NewCollapseBuilder("user")).
			From(0).
			Size(5).
			TrackScores(true).
			Explain(true).
			Version(true).
			StoredField("f1").
			StoredFields("f2").
			NoStoredFields().
			FetchSource(true).
			FetchSourceContext(NewFetchSourceContext(true).Include("a", "b").Exclude("c")).
			DocvalueFields("dv1").
			DocvalueFieldsWithFormat(DocvalueField{Field: "dv2"}).
			DocvalueField("dv3").
			DocvalueFieldWithFormat(DocvalueField{Field: "dv4"}).
			ScriptFields(NewScriptField("sf", NewScriptInline("1"))).
			ScriptField(NewScriptField("sf2", NewScriptInline("2"))).
			Sort("price", true).
			SortWithInfo(SortInfo{Field: "x", Ascending: false}).
			SortBy(NewFieldSort("ts")).
			Highlight(NewHighlight().Field("title"))
		ih.Highlighter()
		src, err := ih.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "ih1", m["name"])
	})
}

func TestC2SortInfo_Source_AllBranches(t *testing.T) {
	t.Run("ascending_with_all", func(t *testing.T) {
		iu := true
		info := SortInfo{
			Field:          "price",
			Ascending:      true,
			Missing:        "_last",
			IgnoreUnmapped: &iu,
			UnmappedType:   "date",
			SortMode:       "min",
			Filter:         NewMatchAllQuery(),
			Path:           "nested",
			Nested:         NewNestedSort("nested"),
		}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["price"].(map[string]any)
		assert.Equal(t, "asc", m["order"])
		assert.Equal(t, "_last", m["missing"])
		assert.Equal(t, true, m["ignore_unmapped"])
		assert.Equal(t, "date", m["unmapped_type"])
		assert.Equal(t, "min", m["mode"])
		assert.Contains(t, m, "filter")
		assert.Equal(t, "nested", m["path"])
		assert.Contains(t, m, "nested")
	})
	t.Run("descending_deprecated", func(t *testing.T) {
		info := SortInfo{
			Field:        "price",
			Ascending:    false,
			NestedFilter: NewMatchAllQuery(),
			NestedPath:   "nested",
			NestedSort:   NewNestedSort("nested"),
		}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["price"].(map[string]any)
		assert.Equal(t, "desc", m["order"])
		assert.Contains(t, m, "nested_filter")
		assert.Equal(t, "nested", m["nested_path"])
		assert.Contains(t, m, "nested")
	})
}

func TestC2FuzzyQuery_NoError(t *testing.T) {
	q := NewFuzzyQuery("field", "value")
	_, err := q.Source()
	assert.NoError(t, err)
}

func TestC2MatchQuery_CompactVsFull(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		q := NewMatchQuery("message", "hello")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["match"].(map[string]any)
		assert.Contains(t, m, "message")
	})
	t.Run("with_options", func(t *testing.T) {
		q := NewMatchQuery("message", "hello")
		q.Operator = "and"
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["match"].(map[string]any)
		inner := m["message"].(map[string]any)
		assert.Equal(t, "and", inner["operator"])
	})
}

func TestC2MatchAllQuery_WithName(t *testing.T) {
	q := NewMatchAllQuery()
	q.QueryName = "ma"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_all"].(map[string]any)
	assert.Equal(t, "ma", m["_name"])
}

func TestC2MatchNoneQuery_WithName(t *testing.T) {
	q := NewMatchNoneQuery()
	q.QueryName = "mn"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_none"].(map[string]any)
	assert.Equal(t, "mn", m["_name"])
}

func TestC2IdsQuery_AllFields(t *testing.T) {
	q := NewIdsQuery("1", "2")
	boost := 1.5
	q.Boost = &boost
	q.QueryName = "ids_q"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["ids"].(map[string]any)
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "ids_q", m["_name"])
}

func TestC2RegexpQuery_AllFields(t *testing.T) {
	q := NewRegexpQuery("name", "s.*")
	boost := 2.0
	ci := true
	maxDet := 10000
	q.Boost = &boost
	q.Flags = "ALL"
	q.Rewrite = "constant_score"
	q.CaseInsensitive = &ci
	q.MaxDeterminizedStates = &maxDet
	q.QueryName = "rq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["regexp"].(map[string]any)
	inner := m["name"].(map[string]any)
	assert.Equal(t, "s.*", inner["value"])
	assert.Equal(t, "ALL", inner["flags"])
	assert.Equal(t, "constant_score", inner["rewrite"])
	assert.Equal(t, true, inner["case_insensitive"])
	assert.Equal(t, float64(10000), inner["max_determinized_states"])
	assert.Equal(t, "rq", inner["name"])
}

func TestC2SpanTermQuery_AllFields(t *testing.T) {
	q := NewSpanTermQuery("user", "kimchy")
	boost := 2.0
	q.Boost = &boost
	q.QueryName = "st"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_term"].(map[string]any)
	inner := m["user"].(map[string]any)
	assert.Equal(t, "kimchy", inner["value"])
	assert.Equal(t, 2.0, inner["boost"])
	assert.Equal(t, "st", inner["query_name"])
}

func TestC2SpanTermQuery_NoValue(t *testing.T) {
	q := NewSpanTermQuery("user")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_term"].(map[string]any)
	assert.Contains(t, m, "user")
}

func TestC2TypeQuery_Source(t *testing.T) {
	q := NewTypeQuery("_doc")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["type"].(map[string]any)
	assert.Equal(t, "_doc", m["value"])
}

func TestC2WildcardQuery_AllFields(t *testing.T) {
	q := NewWildcardQuery("name", "ki*y")
	boost := 2.0
	ci := true
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.QueryName = "wq"
	q.CaseInsensitive = &ci
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["wildcard"].(map[string]any)
	inner := m["name"].(map[string]any)
	assert.Equal(t, "ki*y", inner["value"])
	assert.Equal(t, "constant_score", inner["rewrite"])
	assert.Equal(t, "wq", inner["_name"])
	assert.Equal(t, true, inner["case_insensitive"])
}

func TestC2ExistsQuery_WithName(t *testing.T) {
	q := NewExistsQuery("email")
	q.QueryName = "eq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["exists"].(map[string]any)
	assert.Equal(t, "email", m["field"])
	assert.Equal(t, "eq", m["_name"])
}

func TestC2FilterAggregation_Error(t *testing.T) {
	a := FilterAggregation{Filter: mockQueryError{}}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestC2TopHitsAggregation_AllBranches(t *testing.T) {
	a := NewTopHitsAggregation()
	a.Size(5).
		From(0).
		Sort("date", false).
		SortWithInfo(SortInfo{Field: "x", Ascending: true}).
		SortBy(NewFieldSort("y")).
		TrackScores(true).
		Explain(true).
		Version(true).
		NoStoredFields().
		FetchSource(true).
		FetchSourceContext(NewFetchSourceContext(true)).
		DocvalueFields("dv").
		DocvalueFieldsWithFormat(DocvalueField{Field: "dv2"}).
		DocvalueField("dv3").
		DocvalueFieldWithFormat(DocvalueField{Field: "dv4"}).
		ScriptFields(NewScriptField("sf", NewScriptInline("1"))).
		ScriptField(NewScriptField("sf2", NewScriptInline("2"))).
		Highlight(NewHighlight().Field("title"))
	a.Highlighter()
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["top_hits"].(map[string]any)
	assert.NotNil(t, m)
}

func TestC2ScriptedMetricAggregation_AllScripts(t *testing.T) {
	a := NewScriptedMetricAggregation()
	a.InitScript = NewScriptInline("state.transactions = []")
	a.MapScript = NewScriptInline("state.transactions.add(doc.type.value)")
	a.CombineScript = NewScriptInline("return state.transactions")
	a.ReduceScript = NewScriptInline("return states")
	a.Params = map[string]any{"currency": "USD"}
	a.Meta = map[string]any{"k": "v"}
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

func TestC2ScriptedMetricAggregation_NoScripts(t *testing.T) {
	a := NewScriptedMetricAggregation()
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["scripted_metric"].(map[string]any)
	assert.Empty(t, m)
}

func TestC2BoolQuery_CollectError(t *testing.T) {
	q := NewBoolQuery().Must(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)

	q2 := NewBoolQuery().MustNot(mockQueryError{})
	_, err = q2.Source()
	assert.Error(t, err)

	q3 := NewBoolQuery().Filter(mockQueryError{})
	_, err = q3.Source()
	assert.Error(t, err)

	q4 := NewBoolQuery().Should(mockQueryError{})
	_, err = q4.Source()
	assert.Error(t, err)
}

func TestC2BoolQuery_AllOptions(t *testing.T) {
	q := NewBoolQuery().
		Must(NewMatchAllQuery(), NewMatchAllQuery()).
		MustNot(NewTermQuery("status", "draft"), NewTermQuery("status", "archived")).
		Filter(NewRangeQuery("age").Gte(18), NewExistsQuery("email")).
		Should(NewMatchQuery("title", "hello"), NewMatchQuery("title", "world")).
		Boost(1.5).
		MinimumShouldMatch("1").
		AdjustPureNegative(true).
		QueryName("my-bool")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bool"].(map[string]any)
	assert.Len(t, m["must"].([]any), 2)
	assert.Len(t, m["must_not"].([]any), 2)
	assert.Len(t, m["filter"].([]any), 2)
	assert.Len(t, m["should"].([]any), 2)
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "1", m["minimum_should_match"])
	assert.Equal(t, true, m["adjust_pure_negative"])
	assert.Equal(t, "my-bool", m["_name"])
}

func TestC2BoolQuery_SingleClauses(t *testing.T) {
	q := NewBoolQuery().
		Must(NewMatchAllQuery()).
		MustNot(NewTermQuery("status", "draft")).
		Filter(NewRangeQuery("age").Gte(18)).
		Should(NewMatchQuery("title", "hello"))
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["bool"].(map[string]any)
	_, isMap := m["must"].(map[string]any)
	assert.True(t, isMap)
}

func TestC2Collect_ZeroOneMultiple(t *testing.T) {
	_, err := collect(nil)
	require.NoError(t, err)

	c1, err := collect([]Query{NewMatchAllQuery()})
	require.NoError(t, err)
	assert.Len(t, c1, 1)

	c2, err := collect([]Query{NewMatchAllQuery(), NewMatchAllQuery()})
	require.NoError(t, err)
	assert.Len(t, c2, 2)
}

func TestC2SignificanceHeuristics_SourceWithFields(t *testing.T) {
	t.Run("chi_square_full", func(t *testing.T) {
		h := NewChiSquareSignificanceHeuristic()
		t1 := true
		f1 := false
		h.BackgroundIsSupersetVal = &t1
		h.IncludeNegativesVal = &f1
		src, err := h.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["background_is_superset"])
		assert.Equal(t, false, m["include_negatives"])
	})
	t.Run("gnd_full", func(t *testing.T) {
		h := NewGNDSignificanceHeuristic()
		t1 := true
		h.BackgroundIsSupersetVal = &t1
		src, err := h.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["background_is_superset"])
	})
	t.Run("mutual_information_full", func(t *testing.T) {
		h := NewMutualInformationSignificanceHeuristic()
		t1 := true
		f1 := false
		h.BackgroundIsSupersetVal = &t1
		h.IncludeNegativesVal = &f1
		src, err := h.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["background_is_superset"])
		assert.Equal(t, false, m["include_negatives"])
	})
	t.Run("jlh", func(t *testing.T) {
		h := NewJLHScoreSignificanceHeuristic()
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("percentage", func(t *testing.T) {
		h := NewPercentageScoreSignificanceHeuristic()
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("script_full", func(t *testing.T) {
		h := NewScriptSignificanceHeuristic()
		h.ScriptVal = NewScriptInline("_score * 2")
		src, err := h.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "script")
	})
}

func TestC2TermsAggregationIncludeExclude_MergeInto(t *testing.T) {
	t.Run("regexp_merge", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Include: "foo.*", Exclude: "bar.*"}
		target := map[string]any{}
		err := ie.MergeInto(target)
		require.NoError(t, err)
		assert.Equal(t, "foo.*", target["include"])
		assert.Equal(t, "bar.*", target["exclude"])
	})
	t.Run("values_merge", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{
			IncludeValues: []any{"a", "b"},
			ExcludeValues: []any{"x"},
		}
		target := map[string]any{}
		err := ie.MergeInto(target)
		require.NoError(t, err)
		assert.NotNil(t, target["include"])
		assert.NotNil(t, target["exclude"])
	})
	t.Run("partitions_merge", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{Partition: 0, NumPartitions: 5}
		target := map[string]any{}
		err := ie.MergeInto(target)
		require.NoError(t, err)
		inc := target["include"].(map[string]any)
		assert.Equal(t, 0, inc["partition"])
		assert.Equal(t, 5, inc["num_partitions"])
	})
	t.Run("empty_merge", func(t *testing.T) {
		ie := &TermsAggregationIncludeExclude{}
		target := map[string]any{}
		err := ie.MergeInto(target)
		require.NoError(t, err)
		_, hasInc := target["include"]
		assert.False(t, hasInc)
	})
}

func TestC2SearchSource_InnerHit_Path(t *testing.T) {
	ss := NewSearchSource()
	ih := NewInnerHit().Path("comments")
	ss.InnerHit("my_hit", ih)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	ihm := m["inner_hits"].(map[string]any)
	hit := ihm["my_hit"].(map[string]any)
	assert.Contains(t, hit, "path")
}

func TestC2SearchSource_InnerHit_Type(t *testing.T) {
	ss := NewSearchSource()
	ih := NewInnerHit().Type("answer")
	ss.InnerHit("my_hit", ih)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	ihm := m["inner_hits"].(map[string]any)
	hit := ihm["my_hit"].(map[string]any)
	assert.Contains(t, hit, "type")
}

func TestC2SearchSource_InnerHit_NoPathType(t *testing.T) {
	ss := NewSearchSource()
	ss.InnerHit("hit", NewInnerHit())
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "inner_hits")
}

func TestC2SearchSource_Source_WithRescores(t *testing.T) {
	ss := NewSearchSource().
		DefaultRescoreWindowSize(100).
		Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))).
		Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())))
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "rescore")
}

func TestC2SearchSource_Source_EmptyRescore(t *testing.T) {
	ss := NewSearchSource().
		Rescorer(NewRescore()).
		Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())))
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "rescore")
}

func TestC2SearchSource_Source_SuggestNoGlobalText(t *testing.T) {
	ss := NewSearchSource().Suggester(NewTermSuggester("s1").Field("title").Text("hello"))
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	sug := m["suggest"].(map[string]any)
	_, hasText := sug["text"]
	assert.False(t, hasText)
}

func TestC2SearchSource_Source_SuggestGlobalText(t *testing.T) {
	ss := NewSearchSource().
		GlobalSuggestText("hello").
		Suggester(NewTermSuggester("s1").Field("title").Text("hello"))
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	sug := m["suggest"].(map[string]any)
	assert.Equal(t, "hello", sug["text"])
}

func TestC2IndexBoosts_Source(t *testing.T) {
	t.Run("multiple", func(t *testing.T) {
		boosts := IndexBoosts{
			{Index: "a", Boost: 1.0},
			{Index: "b", Boost: 2.0},
		}
		src, err := boosts.Source()
		require.NoError(t, err)
		arr := src.([]any)
		assert.Len(t, arr, 2)
	})
	t.Run("single", func(t *testing.T) {
		boosts := IndexBoosts{{Index: "a", Boost: 1.0}}
		src, err := boosts.Source()
		require.NoError(t, err)
		arr := src.([]any)
		assert.Len(t, arr, 1)
	})
}

func TestC2SearchRequest_Source_Overloads(t *testing.T) {
	t.Run("nil_source_default", func(t *testing.T) {
		req := NewSearchRequest()
		body, err := req.Body()
		require.NoError(t, err)
		assert.NotEmpty(t, body)
	})
	t.Run("search_source_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		ss := NewSearchSource().Size(5)
		req.Source(ss)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})
	t.Run("json_raw_message", func(t *testing.T) {
		req := NewSearchRequest()
		raw := json.RawMessage(`{"query":{"match_all":{}}}`)
		req.Source(raw)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})
	t.Run("json_raw_message_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		raw := json.RawMessage(`{"size":10}`)
		req.Source(&raw)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})
	t.Run("string", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source(`{"query":{"match_all":{}}}`)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})
	t.Run("string_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		s := `{"query":{"match_all":{}}}`
		req.Source(&s)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})
	t.Run("nil_string_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source((*string)(nil))
		body, err := req.Body()
		require.NoError(t, err)
		assert.Equal(t, "{}", body)
	})
	t.Run("other_type", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source(map[string]any{"size": 10})
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})
}

func TestC2SearchRequest_SourceAsMap_AllBranches(t *testing.T) {
	t.Run("nil_source", func(t *testing.T) {
		req := NewSearchRequest()
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("search_source_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source(NewSearchSource().Size(5))
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("json_raw_message", func(t *testing.T) {
		req := NewSearchRequest()
		raw := json.RawMessage(`{"size":10}`)
		req.Source(raw)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("json_raw_message_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		raw := json.RawMessage(`{"size":10}`)
		req.Source(&raw)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("string", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source(`{"size":10}`)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("string_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		s := `{"size":10}`
		req.Source(&s)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("nil_string_ptr", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source((*string)(nil))
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
	t.Run("other_type", func(t *testing.T) {
		req := NewSearchRequest()
		req.Source(map[string]any{"size": 10})
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestC2ScriptFunction_Source_NilScript(t *testing.T) {
	fn := NewScriptFunction(nil)
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	_, hasScript := m["script"]
	assert.False(t, hasScript)
}

func TestC2FieldValueFactorFunction_Source_AllFields(t *testing.T) {
	fn := NewFieldValueFactorFunction().
		Field("likes").
		Factor(1.2).
		Modifier("LOG").
		Missing(1.0)
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "likes", m["field"])
	assert.Equal(t, 1.2, m["factor"])
	assert.Equal(t, "log", m["modifier"])
	assert.Equal(t, 1.0, m["missing"])
}

func TestC2FieldValueFactorFunction_Source_Empty(t *testing.T) {
	fn := NewFieldValueFactorFunction()
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Empty(t, m)
}

func TestC2DecayFunctions_Source_AllOptions(t *testing.T) {
	t.Run("gauss_all", func(t *testing.T) {
		fn := NewGaussDecayFunction().
			FieldName("date").
			Origin("now").
			Scale("10d").
			Decay(0.5).
			Offset("2d").
			Weight(1.0).
			MultiValueMode("min")
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		params := m["date"].(map[string]any)
		assert.Equal(t, "now", params["origin"])
		assert.Equal(t, "10d", params["scale"])
		assert.Equal(t, 0.5, params["decay"])
		assert.Equal(t, "2d", params["offset"])
		assert.Equal(t, "min", m["multi_value_mode"])
	})
	t.Run("gauss_no_decay", func(t *testing.T) {
		fn := NewGaussDecayFunction().FieldName("date").Scale("10d")
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		params := m["date"].(map[string]any)
		_, hasDecay := params["decay"]
		assert.False(t, hasDecay)
	})
	t.Run("exponential_all", func(t *testing.T) {
		fn := NewExponentialDecayFunction().
			FieldName("price").
			Origin(100).
			Scale(50).
			Decay(0.5).
			Offset(10).
			Weight(2.0).
			MultiValueMode("max")
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "price")
	})
	t.Run("linear_all", func(t *testing.T) {
		fn := NewLinearDecayFunction().
			FieldName("ts").
			Origin("now").
			Scale("7d").
			Decay(0.3).
			Offset("1d").
			Weight(1.0).
			MultiValueMode("sum")
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "ts")
		assert.Equal(t, "sum", m["multi_value_mode"])
	})
}

func TestC2Highlight_Source_AllFields(t *testing.T) {
	hl := NewHighlight().
		TagsSchema("styled").
		HighlightFilter(true).
		FragmentSize(150).
		NumOfFragments(3).
		PreTags("<em>", "<b>").
		PostTags("</em>", "</b>").
		Order("score").
		Encoder("html").
		RequireFieldMatch(false).
		MaxAnalyzedOffset(1000).
		BoundaryMaxScan(20).
		BoundaryChars(".,!?").
		BoundaryScannerType("word").
		BoundaryScannerLocale("en").
		HighlighterType("unified").
		Fragmenter("span").
		HighlightQuery(NewMatchAllQuery()).
		NoMatchSize(100).
		Options(map[string]any{"key": "val"}).
		ForceSource(true).
		UseExplicitFieldOrder(true).
		Field("title").
		Field("body")
	src, err := hl.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "styled", m["tags_schema"])
	assert.Equal(t, true, m["highlight_filter"])
	assert.Equal(t, 150, m["fragment_size"])
	assert.Equal(t, 3, m["number_of_fragments"])
	assert.Len(t, m["pre_tags"].([]string), 2)
	assert.Len(t, m["post_tags"].([]string), 2)
	assert.Equal(t, "score", m["order"])
	assert.Equal(t, "html", m["encoder"])
	assert.Equal(t, false, m["require_field_match"])
	assert.Equal(t, 1000, m["max_analyzed_offset"])
	assert.Equal(t, 20, m["boundary_max_scan"])
	assert.Equal(t, ".,!?", m["boundary_chars"])
	assert.Equal(t, "word", m["boundary_scanner"])
	assert.Equal(t, "en", m["boundary_scanner_locale"])
	assert.Equal(t, "unified", m["type"])
	assert.Equal(t, "span", m["fragmenter"])
	assert.Contains(t, m, "highlight_query")
	assert.Equal(t, 100, m["no_match_size"])
	assert.Contains(t, m, "options")
	assert.Equal(t, true, m["force_source"])
	assert.Contains(t, m, "fields")
}

func TestC2HighlightSource_PhraseLimit(t *testing.T) {
	hl := NewHighlight()
	hl.phraseLimit = helperPtrInt(10)
	src, err := hl.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 10, m["phrase_limit"])
}

func TestC2HighlighterField_AllFields(t *testing.T) {
	f := NewHighlighterField("body").
		PreTags("<em>").
		PostTags("</em>").
		FragmentSize(100).
		FragmentOffset(10).
		NumOfFragments(2).
		HighlightFilter(true).
		Order("score").
		RequireFieldMatch(false).
		BoundaryMaxScan(20).
		BoundaryChars('.', '!').
		HighlighterType("fvh").
		Fragmenter("simple").
		HighlightQuery(NewMatchAllQuery()).
		NoMatchSize(50).
		MatchedFields("body", "body.ngram").
		PhraseLimit(10).
		Options(map[string]any{"foo": "bar"}).
		ForceSource(false)
	src, err := f.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, []string{"<em>"}, m["pre_tags"])
	assert.Equal(t, []string{"</em>"}, m["post_tags"])
	assert.Equal(t, 100, m["fragment_size"])
	assert.Equal(t, 2, m["number_of_fragments"])
	assert.Equal(t, 10, m["fragment_offset"])
	assert.Equal(t, true, m["highlight_filter"])
	assert.Equal(t, "score", m["order"])
	assert.Equal(t, false, m["require_field_match"])
	assert.Equal(t, 20, m["boundary_max_scan"])
	assert.Equal(t, "fvh", m["type"])
	assert.Equal(t, "simple", m["fragmenter"])
	assert.Contains(t, m, "highlight_query")
	assert.Equal(t, 50, m["no_match_size"])
	assert.Equal(t, []string{"body", "body.ngram"}, m["matched_fields"])
	assert.Equal(t, 10, m["phrase_limit"])
	assert.Contains(t, m, "options")
	assert.Equal(t, false, m["force_source"])
}

func TestC2FieldSort_AllFields(t *testing.T) {
	s := NewFieldSort("timestamp").
		Desc().
		Missing("_last").
		UnmappedType("date").
		SortMode("max").
		Filter(NewMatchAllQuery()).
		Path("nested.obj").
		Nested(NewNestedSort("nested.obj").Filter(NewMatchAllQuery()).NestedSort(NewNestedSort("nested.inner")))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["timestamp"].(map[string]any)
	assert.Equal(t, "desc", m["order"])
	assert.Equal(t, "_last", m["missing"])
	assert.Equal(t, "date", m["unmapped_type"])
	assert.Equal(t, "max", m["mode"])
	assert.Contains(t, m, "filter")
	assert.Equal(t, "nested.obj", m["path"])
	assert.Contains(t, m, "nested")
}

func TestC2GeoDistanceSort_AllFields(t *testing.T) {
	s := NewGeoDistanceSort("location").
		FieldName("location").
		Point(40.0, -74.0).
		Points(GeoPointFromLatLon(35.0, -110.0)).
		GeoHashes("drm3btev3e86").
		Unit("km").
		IgnoreUnmapped(true).
		GeoDistance("arc").
		DistanceType("plane").
		SortMode("min").
		Desc().
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested").
		NestedSort(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_geo_distance"].(map[string]any)
	assert.Equal(t, "km", m["unit"])
	assert.Equal(t, true, m["ignore_unmapped"])
	assert.Equal(t, "plane", m["distance_type"])
	assert.Equal(t, "min", m["mode"])
	assert.Equal(t, "desc", m["order"])
	assert.Contains(t, m, "nested_filter")
	assert.Equal(t, "nested", m["nested_path"])
	assert.Contains(t, m, "nested")
}

func TestC2ScriptSort_AllFields(t *testing.T) {
	s := NewScriptSort(NewScriptInline("doc['x'].value"), "number").
		Type("number").
		Desc().
		SortMode("min").
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested").
		NestedSort(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_script"].(map[string]any)
	assert.Equal(t, "number", m["type"])
	assert.Equal(t, "desc", m["order"])
	assert.Equal(t, "min", m["mode"])
	assert.Contains(t, m, "nested_filter")
	assert.Equal(t, "nested", m["nested_path"])
}

func TestC2ScriptSort_Asc(t *testing.T) {
	s := NewScriptSort(NewScriptInline("doc['x'].value"), "number").Asc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_script"].(map[string]any)
	assert.Equal(t, "asc", m["order"])
}

func TestC2NestedSort_AllFields(t *testing.T) {
	ns := NewNestedSort("comments").
		Filter(NewMatchAllQuery()).
		NestedSort(NewNestedSort("reply").Filter(NewMatchAllQuery()))
	src, err := ns.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "comments", m["path"])
	assert.Contains(t, m, "filter")
	assert.Contains(t, m, "nested")
}

func TestC2NestedSort_Empty(t *testing.T) {
	ns := NewNestedSort("")
	src, err := ns.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Empty(t, m)
}

func TestC2FieldSort_Deprecated(t *testing.T) {
	s := NewFieldSort("ts").
		FieldName("ts2").
		Order(true).
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested").
		NestedSort(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "ts2")
}

func TestC2ScoreSort_Order(t *testing.T) {
	s := NewScoreSort().Order(true).Desc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_score"].(map[string]any)
	assert.Equal(t, "desc", m["order"])
}

func TestC2FetchSourceContext_AllBranches(t *testing.T) {
	t.Run("false", func(t *testing.T) {
		fsc := NewFetchSourceContext(false)
		assert.False(t, fsc.FetchSource())
		src, err := fsc.Source()
		require.NoError(t, err)
		assert.Equal(t, false, src)
	})
	t.Run("true_no_filters", func(t *testing.T) {
		fsc := NewFetchSourceContext(true)
		src, err := fsc.Source()
		require.NoError(t, err)
		assert.Equal(t, true, src)
	})
	t.Run("with_includes_excludes", func(t *testing.T) {
		fsc := NewFetchSourceContext(true).Include("a", "b").Exclude("c")
		src, err := fsc.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "includes")
		assert.Contains(t, m, "excludes")
	})
	t.Run("query_false", func(t *testing.T) {
		fsc := NewFetchSourceContext(false)
		q := fsc.Query()
		assert.Equal(t, "false", q.Get("_source"))
	})
	t.Run("query_include_exclude", func(t *testing.T) {
		fsc := NewFetchSourceContext(true).Include("a").Exclude("b")
		q := fsc.Query()
		assert.Contains(t, q.Get("_source_includes"), "a")
		assert.Contains(t, q.Get("_source_excludes"), "b")
	})
}

func TestC2SearchSource_FetchSource_Lazy(t *testing.T) {
	ss := NewSearchSource()
	ss.FetchSource(true)
	ss.FetchSource(false)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, false, m["_source"])
}

func TestC2SearchSource_FetchSource_LazyInclude(t *testing.T) {
	ss := NewSearchSource()
	ss.FetchSource(true)
	ss.FetchSource(true)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, true, m["_source"])
}

func TestC2PointInTime_KeepAlive(t *testing.T) {
	pit := NewPointInTimeWithKeepAlive("abc", "5m")
	src, err := pit.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "abc", m["id"])
	assert.Equal(t, "5m", m["keep_alive"])
}

func TestC2PointInTime_Nil(t *testing.T) {
	var pit *PointInTime
	src, err := pit.Source()
	require.NoError(t, err)
	assert.Nil(t, src)
}

func TestC2CollapseBuilder_AllFields(t *testing.T) {
	cb := NewCollapseBuilder("user").
		Field("user").
		InnerHit(NewInnerHit().Name("last_tweets").Size(5)).
		MaxConcurrentGroupRequests(4)
	src, err := cb.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "user", m["field"])
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, 4, m["max_concurrent_group_searches"])
}

func TestC2CollapseBuilder_NoInnerHits(t *testing.T) {
	cb := NewCollapseBuilder("user")
	src, err := cb.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	_, hasIH := m["inner_hits"]
	assert.False(t, hasIH)
}

func TestC2Rescore_Source(t *testing.T) {
	t.Run("with_window", func(t *testing.T) {
		r := NewRescore().WindowSize(100).Rescorer(NewQueryRescorer(NewMatchAllQuery()))
		src, err := r.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 100, m["window_size"])
	})
	t.Run("default_window", func(t *testing.T) {
		r := NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))
		r.defaultRescoreWindowSize = helperPtrInt(50)
		src, err := r.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 50, m["window_size"])
	})
}

func TestC2QueryRescorer_AllFields(t *testing.T) {
	r := NewQueryRescorer(NewMatchAllQuery()).
		RescoreQueryWeight(2.0).
		QueryWeight(1.0).
		ScoreMode("total")
	assert.Equal(t, "query", r.Name())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "rescore_query")
	assert.Equal(t, 2.0, m["rescore_query_weight"])
	assert.Equal(t, 1.0, m["query_weight"])
	assert.Equal(t, "total", m["score_mode"])
}

func TestC2DocvalueFields_Source_Nil(t *testing.T) {
	var d DocvalueFields
	src, err := d.Source()
	require.NoError(t, err)
	assert.Nil(t, src)
}

func TestC2DocvalueFields_Source_Multiple(t *testing.T) {
	d := DocvalueFields{{Field: "a"}, {Field: "b", Format: "x"}}
	src, err := d.Source()
	require.NoError(t, err)
	arr := src.([]any)
	assert.Len(t, arr, 2)
}

func TestC2FieldFields_Source_Nil(t *testing.T) {
	var f FieldFields
	src, err := f.Source()
	require.NoError(t, err)
	assert.Nil(t, src)
}

func TestC2FieldFields_Source_Multiple(t *testing.T) {
	f := FieldFields{{Field: "a"}, {Field: "b", Format: "x"}}
	src, err := f.Source()
	require.NoError(t, err)
	arr := src.([]any)
	assert.Len(t, arr, 2)
}

func TestC2FunctionScoreQuery_WithFunctions(t *testing.T) {
	q := NewFunctionScoreQuery().
		Query(NewMatchAllQuery()).
		Filter(NewMatchAllQuery()).
		AddScoreFunc(NewWeightFactorFunction(2.0)).
		Add(NewMatchAllQuery(), NewRandomFunction().Field("_seq_no").Seed(42).Weight(1.5)).
		ScoreMode("sum").
		BoostMode("replace").
		MaxBoost(10.0).
		Boost(1.0).
		MinScore(0.5)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	assert.Contains(t, m, "query")
	assert.Contains(t, m, "filter")
	assert.Contains(t, m, "functions")
	assert.Equal(t, "sum", m["score_mode"])
	assert.Equal(t, "replace", m["boost_mode"])
	assert.Equal(t, 10.0, m["max_boost"])
	assert.Equal(t, 1.0, m["boost"])
	assert.Equal(t, 0.5, m["min_score"])
}

func TestC2FunctionScoreQuery_NoFunc(t *testing.T) {
	q := NewFunctionScoreQuery().Query(NewMatchAllQuery())
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	assert.Contains(t, m, "query")
}

func TestC2FunctionScoreQuery_AddNilFunc(t *testing.T) {
	q := NewFunctionScoreQuery()
	q.Add(NewMatchAllQuery(), NewWeightFactorFunction(1.0))
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	assert.Contains(t, m, "functions")
}

func TestC2SearchSource_StoredField_Single(t *testing.T) {
	ss := NewSearchSource().StoredField("f1")
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "f1", m["stored_fields"])
}

func TestC2SearchSource_TrackTotalHits_Int(t *testing.T) {
	ss := NewSearchSource().TrackTotalHits(5000)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, 5000, m["track_total_hits"])
}

func TestC2SearchSource_TrackTotalHits_Bool(t *testing.T) {
	ss := NewSearchSource().TrackTotalHits(true)
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, true, m["track_total_hits"])
}

func TestC2SearchRequest_Header_AllBranches(t *testing.T) {
	req := NewSearchRequest()
	req.SearchType("dfs_query_then_fetch")
	req.Index("idx1", "idx2")
	req.Type("type1", "type2")
	routing := "user_1"
	req.Routing(routing)
	pref := "_local"
	req.Preference(pref)
	req.RequestCache(true)
	req.IgnoreUnavailable(true)
	req.AllowNoIndices(true)
	req.ExpandWildcards("open")
	req.AllowPartialSearchResults(true)
	req.Scroll("2m")
	h := req.header().(map[string]any)
	assert.Equal(t, "dfs_query_then_fetch", h["search_type"])
	assert.Equal(t, []string{"idx1", "idx2"}, h["indices"])
	assert.Equal(t, []string{"type1", "type2"}, h["types"])
	assert.Equal(t, "user_1", h["routing"])
	assert.Equal(t, "_local", h["preference"])
	assert.Equal(t, true, h["request_cache"])
}

func TestC2SearchRequest_Header_SingleIndexType(t *testing.T) {
	req := NewSearchRequest()
	req.Index("single")
	req.Type("single_type")
	h := req.header().(map[string]any)
	assert.Equal(t, "single", h["index"])
	assert.Equal(t, "single_type", h["type"])
}

func TestC2SearchRequest_Header_EmptyRouting(t *testing.T) {
	req := NewSearchRequest()
	empty := ""
	req.routing = &empty
	h := req.header().(map[string]any)
	_, ok := h["routing"]
	assert.False(t, ok)
}

func TestC2SearchRequest_Header_EmptyPreference(t *testing.T) {
	req := NewSearchRequest()
	empty := ""
	req.preference = &empty
	h := req.header().(map[string]any)
	_, ok := h["preference"]
	assert.False(t, ok)
}

func TestC2SearchRequest_Methods(t *testing.T) {
	req := NewSearchRequest()
	req.SearchTypeDfsQueryThenFetch()
	req.SearchTypeQueryThenFetch()
	assert.False(t, req.HasIndices())
	req.Index("test")
	assert.True(t, req.HasIndices())
	req.Routings("r1", "r2")
	req.Routings()
	req.BatchedReduceSize(30)
	req.MaxConcurrentShardRequests(5)
	req.PreFilterShardSize(128)
}

func TestC2FunctionScoreQuery_QueryError(t *testing.T) {
	q := NewFunctionScoreQuery().Query(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2FunctionScoreQuery_FilterError(t *testing.T) {
	q := NewFunctionScoreQuery().Filter(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2FunctionScoreQuery_ScoreFuncError(t *testing.T) {
	q := NewFunctionScoreQuery()
	q.Add(NewMatchAllQuery(), mockScoreFunctionError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_QueryError(t *testing.T) {
	ss := NewSearchSource().Query(mockQueryError{})
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_PostFilterError(t *testing.T) {
	ss := NewSearchSource().PostFilter(mockQueryError{})
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_SliceError(t *testing.T) {
	ss := NewSearchSource().Slice(mockQueryError{})
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_AggError(t *testing.T) {
	ss := NewSearchSource()
	ss.aggregations["bad"] = mockAggregationError{}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchRequest_Body_MarshalError(t *testing.T) {
	req := NewSearchRequest()
	req.Source(make(chan int))
	_, err := req.Body()
	assert.Error(t, err)
}

func TestC2SearchRequest_Body_SearchSourceError(t *testing.T) {
	req := NewSearchRequest()
	ss := NewSearchSource().Query(mockQueryError{})
	req.Source(ss)
	_, err := req.Body()
	assert.Error(t, err)
}

func TestC2SearchRequest_Body_DefaultSourceError(t *testing.T) {
	req := NewSearchRequest()
	req.searchSource = NewSearchSource().Query(mockQueryError{})
	_, err := req.Body()
	assert.Error(t, err)
}

func TestC2SearchRequest_SourceAsMap_SearchSourceError(t *testing.T) {
	req := NewSearchRequest()
	ss := NewSearchSource().Query(mockQueryError{})
	req.Source(ss)
	_, err := req.sourceAsMap()
	assert.Error(t, err)
}

func TestC2SearchRequest_SourceAsMap_MarshalError(t *testing.T) {
	req := NewSearchRequest()
	req.Source(make(chan int))
	_, err := req.sourceAsMap()
	assert.Error(t, err)
}

func TestC2TopHitsAggregation_Source_Error(t *testing.T) {
	a := NewTopHitsAggregation()
	a.SearchSource = NewSearchSource().Query(mockQueryError{})
	_, err := a.Source()
	assert.Error(t, err)
}

func TestC2FilterAggregation_SubAggError(t *testing.T) {
	a := FilterAggregation{
		Filter:  NewMatchAllQuery(),
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_SuggesterError(t *testing.T) {
	ss := NewSearchSource()
	ss.suggesters = []Suggester{mockSuggesterError{}}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_HighlightError(t *testing.T) {
	ss := NewSearchSource()
	ss.Highlight(NewHighlight().HighlightQuery(mockQueryError{}))
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_RescorerError(t *testing.T) {
	ss := NewSearchSource()
	r := NewRescore().Rescorer(NewQueryRescorer(mockQueryError{})).WindowSize(10)
	ss.Rescorer(r)
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_CollapseError(t *testing.T) {
	ss := NewSearchSource()
	ih := NewInnerHit().Name("x").Query(mockQueryError{})
	cb := NewCollapseBuilder("user").InnerHit(ih)
	ss.Collapse(cb)
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_SorterError(t *testing.T) {
	ss := NewSearchSource()
	ss.sorters = []Sorter{mockSorterError{}}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_ScriptFieldError(t *testing.T) {
	ss := NewSearchSource()
	sf := NewScriptField("x", nil)
	ss.scriptFields = []*ScriptField{sf}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_DocvalueFieldsError(t *testing.T) {
	ss := NewSearchSource()
	ss.docvalueFields = DocvalueFields{{Field: "f"}}
	src, err := ss.Source()
	require.NoError(t, err)
	assert.Contains(t, src.(map[string]any), "docvalue_fields")
}

func TestC2SearchSource_FieldsError(t *testing.T) {
	ss := NewSearchSource()
	ss.fields = FieldFields{{Field: "f"}}
	src, err := ss.Source()
	require.NoError(t, err)
	assert.Contains(t, src.(map[string]any), "fields")
}

func TestC2SearchSource_IndexBoostsError(t *testing.T) {
	ss := NewSearchSource()
	ss.indexBoosts = IndexBoosts{{Index: "a", Boost: 1.0}}
	src, err := ss.Source()
	require.NoError(t, err)
	assert.Contains(t, src.(map[string]any), "indices_boost")
}

func TestC2SearchSource_FetchSourceError(t *testing.T) {
	ss := NewSearchSource()
	ss.fetchSourceContext = NewFetchSourceContext(true).Include("a", "b").Exclude("c")
	src, err := ss.Source()
	require.NoError(t, err)
	assert.Contains(t, src.(map[string]any), "_source")
}

func TestC2SearchSource_InnerHitPathError(t *testing.T) {
	ss := NewSearchSource()
	ih := NewInnerHit().Path("comments").Query(mockQueryError{})
	ss.InnerHit("hit", ih)
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_InnerHitTypeError(t *testing.T) {
	ss := NewSearchSource()
	ih := NewInnerHit().Type("answer").Query(mockQueryError{})
	ss.InnerHit("hit2", ih)
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2SearchSource_MarshalJSON_Error(t *testing.T) {
	ss := NewSearchSource().Query(mockQueryError{})
	_, err := ss.MarshalJSON()
	assert.Error(t, err)
}

func TestC2FilterAggregation_FilterMarshalError(t *testing.T) {
	ss := NewSearchSource().Query(mockQueryError{})
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestC2BoolQuery_ErrorInMustSingle(t *testing.T) {
	q := NewBoolQuery().Must(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2BoolQuery_ErrorInMustNotSingle(t *testing.T) {
	q := NewBoolQuery().MustNot(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2BoolQuery_ErrorInFilterSingle(t *testing.T) {
	q := NewBoolQuery().Filter(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestC2BoolQuery_ErrorInShouldSingle(t *testing.T) {
	q := NewBoolQuery().Should(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}
