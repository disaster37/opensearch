package querydsl

import (
	"testing"

	json "github.com/goccy/go-json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItoa(t *testing.T) {
	assert.Equal(t, "42", itoa(42))
	assert.Equal(t, "0", itoa(0))
	assert.Equal(t, "-1", itoa(-1))
}

func TestPipelineBucketsPath(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, pipelineBucketsPath(nil))
		assert.Nil(t, pipelineBucketsPath([]string{}))
	})
	t.Run("single", func(t *testing.T) {
		assert.Equal(t, "my_agg", pipelineBucketsPath([]string{"my_agg"}))
	})
	t.Run("multiple", func(t *testing.T) {
		r := pipelineBucketsPath([]string{"a", "b", "c"})
		assert.Equal(t, []string{"a", "b", "c"}, r)
	})
}

func TestMarshalStruct(t *testing.T) {
	type testStruct struct {
		Foo string `json:"foo,omitempty"`
		Bar int    `json:"bar,omitempty"`
	}

	r, err := marshalStruct(testStruct{Foo: "hello", Bar: 42})
	require.NoError(t, err)
	m := r.(map[string]any)
	assert.Equal(t, "hello", m["foo"])
	assert.Equal(t, float64(42), m["bar"])
}

func TestMatchAllQuery_Source(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		q := NewMatchAllQuery()
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["match_all"]
		assert.True(t, ok)
	})

	t.Run("with boost", func(t *testing.T) {
		boost := 1.5
		q := MatchAllQuery{Boost: &boost}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["match_all"].(map[string]any)
		assert.Equal(t, 1.5, inner["boost"])
	})

	t.Run("with query name", func(t *testing.T) {
		q := MatchAllQuery{QueryName: "my-query"}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["match_all"].(map[string]any)
		assert.Equal(t, "my-query", inner["_name"])
	})
}

func TestMatchNoneQuery_Source(t *testing.T) {
	q := NewMatchNoneQuery()
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	_, ok := m["match_none"]
	assert.True(t, ok)
}

func TestMatchQuery_Source(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		q := NewMatchQuery("title", "opensearch")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "match")
	})

	t.Run("with operator", func(t *testing.T) {
		q := NewMatchQuery("title", "opensearch search")
		q.Operator = "and"
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["match"].(map[string]any)
		titleMap := inner["title"].(map[string]any)
		assert.Equal(t, "and", titleMap["operator"])
	})
}

func TestTermQuery_Source(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		q := NewTermQuery("status", "published")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["term"].(map[string]any)
		assert.Equal(t, "published", inner["status"])
	})

	t.Run("with boost", func(t *testing.T) {
		boost := 1.5
		q := TermQuery{Field: "status", Value: "published", Boost: &boost}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["term"].(map[string]any)
		statusMap := inner["status"].(map[string]any)
		assert.Equal(t, "published", statusMap["value"])
		assert.Equal(t, 1.5, statusMap["boost"])
	})

	t.Run("with case_insensitive", func(t *testing.T) {
		ci := true
		q := TermQuery{Field: "status", Value: "published", CaseInsensitive: &ci}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["term"].(map[string]any)
		statusMap := inner["status"].(map[string]any)
		assert.Equal(t, true, statusMap["case_insensitive"])
	})
}

func TestTermsQuery_Source(t *testing.T) {
	t.Run("with values", func(t *testing.T) {
		q := NewTermsQuery("status", "published", "draft")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Contains(t, inner, "status")
	})

	t.Run("from strings", func(t *testing.T) {
		q := NewTermsQueryFromStrings("status", "published", "draft")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Contains(t, inner, "status")
	})

	t.Run("with boost", func(t *testing.T) {
		boost := 1.5
		q := TermsQuery{Field: "status", Values: []any{"a"}, Boost: &boost}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Equal(t, 1.5, inner["boost"])
	})

	t.Run("with query name", func(t *testing.T) {
		q := TermsQuery{Field: "status", Values: []any{"a"}, QueryName: "my-q"}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Equal(t, "my-q", inner["_name"])
	})
}

func TestRangeQuery_Source(t *testing.T) {
	t.Run("gte and lte", func(t *testing.T) {
		q := NewRangeQuery("age").Gte(18).Lte(65)
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["range"].(map[string]any)
		ageMap := inner["age"].(map[string]any)
		assert.Equal(t, 18, ageMap["from"])
		assert.Equal(t, 65, ageMap["to"])
		assert.Equal(t, true, ageMap["include_lower"])
		assert.Equal(t, true, ageMap["include_upper"])
	})

	t.Run("gt and lt", func(t *testing.T) {
		q := NewRangeQuery("price").Gt(0.0).Lt(100.0)
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["range"].(map[string]any)
		priceMap := inner["price"].(map[string]any)
		assert.Equal(t, 0.0, priceMap["from"])
		assert.Equal(t, 100.0, priceMap["to"])
		assert.Equal(t, false, priceMap["include_lower"])
		assert.Equal(t, false, priceMap["include_upper"])
	})

	t.Run("with time_zone and boost", func(t *testing.T) {
		boost := 2.0
		q := RangeQuery{Field: "date", From: "2024-01-01", To: "2024-12-31", TimeZone: "UTC", Boost: &boost}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["range"].(map[string]any)
		dateMap := inner["date"].(map[string]any)
		assert.Equal(t, "UTC", dateMap["time_zone"])
		assert.Equal(t, 2.0, dateMap["boost"])
	})
}

func TestExistsQuery_Source(t *testing.T) {
	q := NewExistsQuery("email")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["exists"].(map[string]any)
	assert.Equal(t, "email", inner["field"])
}

func TestIdsQuery_Source(t *testing.T) {
	q := NewIdsQuery("1", "2", "3")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["ids"].(map[string]any)
	vals := inner["values"].([]any)
	assert.Contains(t, vals, "1")
	assert.Contains(t, vals, "2")
	assert.Contains(t, vals, "3")
}

func TestPrefixQuery_Source(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		q := NewPrefixQuery("name", "joh")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["prefix"].(map[string]any)
		assert.Equal(t, "joh", inner["name"])
	})

	t.Run("with boost", func(t *testing.T) {
		boost := 2.0
		q := PrefixQuery{Field: "name", prefixInner: prefixInner{Value: "joh", Boost: &boost}}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["prefix"].(map[string]any)
		nameMap := inner["name"].(map[string]any)
		assert.Equal(t, 2.0, nameMap["boost"])
	})
}

func TestWildcardQuery_Source(t *testing.T) {
	q := NewWildcardQuery("name", "joh*")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["wildcard"].(map[string]any)
	nameMap := inner["name"].(map[string]any)
	assert.Equal(t, "joh*", nameMap["value"])
}

func TestFuzzyQuery_Source(t *testing.T) {
	q := NewFuzzyQuery("name", "kibana")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["fuzzy"].(map[string]any)
	nameMap := inner["name"].(map[string]any)
	assert.Equal(t, "kibana", nameMap["value"])
}

func TestMatchPhraseQuery_Source(t *testing.T) {
	q := NewMatchPhraseQuery("title", "the quick brown fox")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["match_phrase"].(map[string]any)
	assert.Contains(t, inner, "title")
}

func TestBoolQuery_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		q := NewBoolQuery()
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["bool"]
		assert.True(t, ok)
	})

	t.Run("must only single", func(t *testing.T) {
		q := NewBoolQuery().Must(NewMatchAllQuery())
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		bq := m["bool"].(map[string]any)
		_, ok := bq["must"]
		assert.True(t, ok)
		mustVal := bq["must"]
		_, isMap := mustVal.(map[string]any)
		assert.True(t, isMap)
	})

	t.Run("must multiple", func(t *testing.T) {
		q := NewBoolQuery().Must(NewMatchAllQuery(), NewMatchAllQuery())
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		bq := m["bool"].(map[string]any)
		mustVal := bq["must"].([]any)
		assert.Len(t, mustVal, 2)
	})

	t.Run("must_not filter should", func(t *testing.T) {
		q := NewBoolQuery().
			Should(NewMatchQuery("title", "hello")).
			MustNot(NewTermQuery("status", "draft")).
			Filter(NewRangeQuery("age").Gte(18))
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		bq := m["bool"].(map[string]any)
		assert.Contains(t, bq, "should")
		assert.Contains(t, bq, "must_not")
		assert.Contains(t, bq, "filter")
	})

	t.Run("with all options", func(t *testing.T) {
		q := NewBoolQuery().
			Boost(1.5).
			MinimumShouldMatch("1").
			AdjustPureNegative(true).
			QueryName("my-bool")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		bq := m["bool"].(map[string]any)
		assert.Equal(t, 1.5, bq["boost"])
		assert.Equal(t, "1", bq["minimum_should_match"])
		assert.Equal(t, true, bq["adjust_pure_negative"])
		assert.Equal(t, "my-bool", bq["_name"])
	})

	t.Run("minimum number should match", func(t *testing.T) {
		q := NewBoolQuery().MinimumNumberShouldMatch(2)
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		bq := m["bool"].(map[string]any)
		assert.Equal(t, "2", bq["minimum_should_match"])
	})
}

func TestNestedQuery_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		q := NewNestedQuery("comments", NewMatchQuery("comments.text", "opensearch"))
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		nested := m["nested"].(map[string]any)
		assert.Equal(t, "comments", nested["path"])
		assert.Contains(t, nested, "query")
	})

	t.Run("with options", func(t *testing.T) {
		boost := 2.0
		ignoreUnmapped := true
		q := NestedQuery{
			Path:           "comments",
			Query:          NewMatchAllQuery(),
			ScoreMode:      "max",
			Boost:          &boost,
			QueryName:      "nested-q",
			IgnoreUnmapped: &ignoreUnmapped,
		}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		nested := m["nested"].(map[string]any)
		assert.Equal(t, "max", nested["score_mode"])
		assert.Equal(t, 2.0, nested["boost"])
		assert.Equal(t, "nested-q", nested["_name"])
		assert.Equal(t, true, nested["ignore_unmapped"])
	})
}

func TestSearchSource_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ss := NewSearchSource()
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Empty(t, m)
	})

	t.Run("with query and size", func(t *testing.T) {
		ss := NewSearchSource().
			Query(NewMatchAllQuery()).
			Size(10).
			From(0)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 10, m["size"])
		assert.Equal(t, 0, m["from"])
		assert.Contains(t, m, "query")
	})

	t.Run("with sort", func(t *testing.T) {
		ss := NewSearchSource().
			Query(NewMatchAllQuery()).
			SortBy(NewFieldSort("timestamp").Desc())
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		sortArr := m["sort"].([]any)
		assert.Len(t, sortArr, 1)
	})

	t.Run("with aggregation", func(t *testing.T) {
		ss := NewSearchSource().
			Size(0).
			Aggregation("genres", NewTermsAggregation().WithField("genre"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "aggregations")
	})

	t.Run("with post_filter", func(t *testing.T) {
		ss := NewSearchSource().
			Query(NewMatchAllQuery()).
			PostFilter(NewTermQuery("status", "published"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "post_filter")
	})

	t.Run("with explain version profile", func(t *testing.T) {
		ss := NewSearchSource().
			Query(NewMatchAllQuery()).
			Explain(true).
			Version(true).
			Profile(true)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["explain"])
		assert.Equal(t, true, m["version"])
		assert.Equal(t, true, m["profile"])
	})

	t.Run("with timeout and terminate_after", func(t *testing.T) {
		ss := NewSearchSource().
			Timeout("1s").
			TerminateAfter(1000)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "1s", m["timeout"])
		assert.Equal(t, 1000, m["terminate_after"])
	})

	t.Run("timeout in millis", func(t *testing.T) {
		ss := NewSearchSource().TimeoutInMillis(500)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "500ms", m["timeout"])
	})

	t.Run("with min_score", func(t *testing.T) {
		ss := NewSearchSource().
			MinScore(0.5).
			Query(NewMatchAllQuery())
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 0.5, m["min_score"])
	})

	t.Run("with track_scores and track_total_hits", func(t *testing.T) {
		ss := NewSearchSource().TrackScores(true).TrackTotalHits(true)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["track_scores"])
		assert.Equal(t, true, m["track_total_hits"])
	})

	t.Run("with search_after", func(t *testing.T) {
		ss := NewSearchSource().
			Query(NewMatchAllQuery()).
			SearchAfter(1234, "doc-1")
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, []any{1234, "doc-1"}, m["search_after"])
	})

	t.Run("with seq_no_primary_term", func(t *testing.T) {
		ss := NewSearchSource().SeqNoAndPrimaryTerm(true)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, true, m["seq_no_primary_term"])
	})

	t.Run("with fetch_source context", func(t *testing.T) {
		ss := NewSearchSource().FetchSource(true)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "_source")
	})

	t.Run("with stored fields", func(t *testing.T) {
		ss := NewSearchSource().StoredFields("field1", "field2")
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "stored_fields")
	})

	t.Run("with stats", func(t *testing.T) {
		ss := NewSearchSource().Stats("group1")
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, []string{"group1"}, m["stats"])
	})

	t.Run("with index boost", func(t *testing.T) {
		ss := NewSearchSource().IndexBoost("my-index", 1.2)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "indices_boost")
	})
}

func TestFieldSort_Source(t *testing.T) {
	t.Run("ascending", func(t *testing.T) {
		s := NewFieldSort("timestamp").Asc()
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		ts := m["timestamp"].(map[string]any)
		assert.Equal(t, "asc", ts["order"])
	})

	t.Run("desc with missing", func(t *testing.T) {
		s := NewFieldSort("timestamp").Desc().Missing("_last")
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		ts := m["timestamp"].(map[string]any)
		assert.Equal(t, "desc", ts["order"])
		assert.Equal(t, "_last", ts["missing"])
	})

	t.Run("with unmapped type", func(t *testing.T) {
		s := NewFieldSort("ts").UnmappedType("date")
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		ts := m["ts"].(map[string]any)
		assert.Equal(t, "date", ts["unmapped_type"])
	})

	t.Run("with sort mode", func(t *testing.T) {
		s := NewFieldSort("prices").SortMode("min")
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		prices := m["prices"].(map[string]any)
		assert.Equal(t, "min", prices["mode"])
	})
}

func TestScoreSort_Source(t *testing.T) {
	t.Run("descending (default)", func(t *testing.T) {
		s := NewScoreSort()
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		score := m["_score"].(map[string]any)
		assert.Equal(t, "desc", score["order"])
	})

	t.Run("ascending", func(t *testing.T) {
		s := NewScoreSort().Asc()
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		score := m["_score"].(map[string]any)
		assert.Equal(t, "asc", score["order"])
	})
}

func TestSortByDoc_Source(t *testing.T) {
	s := SortByDoc{}
	src, err := s.Source()
	require.NoError(t, err)
	assert.Equal(t, "_doc", src)
}

func TestGeoDistanceSort_Source(t *testing.T) {
	s := NewGeoDistanceSort("location").
		Point(40.7128, -74.0060).
		Unit("km").
		Asc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	_, ok := m["_geo_distance"]
	assert.True(t, ok)
}

func TestScriptSort_Source(t *testing.T) {
	t.Run("nil script errors", func(t *testing.T) {
		s := NewScriptSort(nil, "number")
		_, err := s.Source()
		assert.Error(t, err)
	})

	t.Run("valid", func(t *testing.T) {
		s := NewScriptSort(NewScriptInline("doc['price'].value * 2"), "number").Desc()
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["_script"]
		assert.True(t, ok)
	})
}

func TestGeoPoint_Source(t *testing.T) {
	gp := GeoPointFromLatLon(40.7, -74.0)
	src := gp.Source()
	assert.Equal(t, 40.7, src["lat"])
	assert.Equal(t, -74.0, src["lon"])
}

func TestGeoPointFromString(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		gp, err := GeoPointFromString("40.7,-74.0")
		require.NoError(t, err)
		assert.Equal(t, 40.7, gp.Lat)
		assert.Equal(t, -74.0, gp.Lon)
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := GeoPointFromString("invalid")
		assert.Error(t, err)
	})

	t.Run("not numbers", func(t *testing.T) {
		_, err := GeoPointFromString("abc,def")
		assert.Error(t, err)
	})
}

func TestTermsAggregation_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		a := NewTermsAggregation().WithField("genre")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Equal(t, "genre", inner["field"])
	})

	t.Run("with size and min doc count", func(t *testing.T) {
		a := NewTermsAggregation().WithField("genre").WithSize(10).WithMinDocCount(1)
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Equal(t, 10, inner["size"])
		assert.Equal(t, 1, inner["min_doc_count"])
	})

	t.Run("with order", func(t *testing.T) {
		a := NewTermsAggregation().WithField("genre").OrderByCountAsc()
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["terms"].(map[string]any)
		assert.Contains(t, inner, "order")
	})
}

func TestAvgAggregation_Source(t *testing.T) {
	src, err := AvgAggregation{Field: "price"}.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["avg"].(map[string]any)
	assert.Equal(t, "price", inner["field"])
}

func TestNestedSort_Source(t *testing.T) {
	ns := NewNestedSort("comments")
	src, err := ns.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "comments", m["path"])
}

func TestSortInfo_Source(t *testing.T) {
	info := SortInfo{Field: "price", Ascending: true, Missing: "_last"}
	src, err := info.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	price := m["price"].(map[string]any)
	assert.Equal(t, "asc", price["order"])
	assert.Equal(t, "_last", price["missing"])
}

func TestIndexBoosts_Source(t *testing.T) {
	boosts := IndexBoosts{
		{Index: "my-index", Boost: 1.5},
		{Index: "other-index", Boost: 2.0},
	}
	src, err := boosts.Source()
	require.NoError(t, err)
	arr := src.([]any)
	assert.Len(t, arr, 2)
}

func TestHighlight_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		hl := NewHighlight()
		src, err := hl.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Empty(t, m)
	})

	t.Run("all_setters", func(t *testing.T) {
		hl := NewHighlight().
			Field("title").
			Field("body").
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
			UseExplicitFieldOrder(true)
		src, err := hl.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "styled", m["tags_schema"])
		assert.Contains(t, m, "pre_tags")
		assert.Contains(t, m, "post_tags")
		assert.Contains(t, m, "fields")
		assert.Equal(t, true, m["force_source"])
	})

	t.Run("field_map_order", func(t *testing.T) {
		hl := NewHighlight().Field("title").Field("body")
		src, err := hl.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["fields"]
		assert.True(t, ok)
	})
}

func TestHighlighterField_Source(t *testing.T) {
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
	assert.Contains(t, m, "pre_tags")
	assert.Contains(t, m, "post_tags")
	assert.Equal(t, 100, m["fragment_size"])
}

func TestScript_Source(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		s := NewScript("doc['x'].value").Lang("painless").Param("a", 1).Params(map[string]any{"b": 2})
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "source")
		assert.Equal(t, "painless", m["lang"])
		assert.Contains(t, m, "params")
	})

	t.Run("stored", func(t *testing.T) {
		s := NewScriptStored("calculate_score")
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "id")
	})

	t.Run("plain_string", func(t *testing.T) {
		s := &Script{}
		s.script = "ctx._source.likes++"
		src, err := s.Source()
		require.NoError(t, err)
		assert.Equal(t, "ctx._source.likes++", src)
	})

	t.Run("json_prefix", func(t *testing.T) {
		s := NewScript(`{"source": "x"}`).Lang("")
		src, err := s.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestScriptField_Source(t *testing.T) {
	t.Run("nil_script", func(t *testing.T) {
		sf := NewScriptField("my_field", nil)
		_, err := sf.Source()
		assert.Error(t, err)
	})

	t.Run("valid", func(t *testing.T) {
		sf := NewScriptField("my_field", NewScriptInline("doc['x'].value")).IgnoreFailure(true)
		src, err := sf.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "script")
		assert.Equal(t, true, m["ignore_failure"])
	})
}

func TestSearchSource_Extended(t *testing.T) {
	t.Run("stored_field_single", func(t *testing.T) {
		ss := NewSearchSource().StoredField("f1")
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "f1", m["stored_fields"])
	})

	t.Run("no_stored_fields", func(t *testing.T) {
		ss := NewSearchSource().NoStoredFields()
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["stored_fields"]
		assert.True(t, ok)
	})

	t.Run("docvalue_fields", func(t *testing.T) {
		ss := NewSearchSource().
			DocvalueField("f1").
			DocvalueFieldWithFormat(DocvalueField{Field: "f2", Format: "epoch_millis"}).
			DocvalueFields("f3", "f4").
			DocvalueFieldsWithFormat(DocvalueField{Field: "f5"})
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "docvalue_fields")
	})

	t.Run("fields", func(t *testing.T) {
		ss := NewSearchSource().
			Field("f1").
			FieldWithFormat(FieldField{Field: "f2", Format: "f"}).
			Fields("f3", "f4").
			FieldsWithFormat(FieldField{Field: "f5"})
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "fields")
	})

	t.Run("script_fields", func(t *testing.T) {
		ss := NewSearchSource().
			ScriptField(NewScriptField("x", NewScriptInline("1"))).
			ScriptFields(NewScriptField("y", NewScriptInline("2")))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "script_fields")
	})

	t.Run("highlight", func(t *testing.T) {
		ss := NewSearchSource().Highlight(NewHighlight().Field("title"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "highlight")
	})

	t.Run("highlighter_lazy", func(t *testing.T) {
		ss := NewSearchSource()
		hl := ss.Highlighter()
		assert.NotNil(t, hl)
	})

	t.Run("global_suggest_text", func(t *testing.T) {
		ss := NewSearchSource().
			GlobalSuggestText("opensearch").
			Suggester(NewTermSuggester("s1").Field("title"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		sug := m["suggest"].(map[string]any)
		assert.Equal(t, "opensearch", sug["text"])
	})

	t.Run("rescore_single", func(t *testing.T) {
		s := NewSearchSource().Rescorer(NewRescore().WindowSize(10).Rescorer(NewQueryRescorer(NewMatchAllQuery())))
		src, err := s.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "rescore")
	})

	t.Run("rescore_multi", func(t *testing.T) {
		ss := NewSearchSource().
			Rescorer(NewRescore().WindowSize(10).Rescorer(NewQueryRescorer(NewMatchAllQuery()))).
			Rescorer(NewRescore().WindowSize(20).Rescorer(NewQueryRescorer(NewMatchAllQuery())))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		arr := m["rescore"].([]any)
		assert.Len(t, arr, 2)
	})

	t.Run("clear_rescorers", func(t *testing.T) {
		ss := NewSearchSource().Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())))
		ss.ClearRescorers()
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		_, ok := m["rescore"]
		assert.False(t, ok)
	})

	t.Run("inner_hit_path", func(t *testing.T) {
		ss := NewSearchSource().InnerHit("my_hit", NewInnerHit().Path("comments"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "inner_hits")
	})

	t.Run("inner_hit_type", func(t *testing.T) {
		ss := NewSearchSource().InnerHit("my_hit", NewInnerHit().Type("answer"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "inner_hits")
	})

	t.Run("collapse", func(t *testing.T) {
		ss := NewSearchSource().Collapse(NewCollapseBuilder("user"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "collapse")
	})

	t.Run("pit", func(t *testing.T) {
		ss := NewSearchSource().PointInTime(NewPointInTime("abc123"))
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "pit")
	})

	t.Run("slice", func(t *testing.T) {
		ss := NewSearchSource().Slice(NewSliceQuery())
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "slice")
	})

	t.Run("fetch_source_include_exclude", func(t *testing.T) {
		ss := NewSearchSource().FetchSourceIncludeExclude([]string{"obj1.*"}, []string{"*.secret"})
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "_source")
	})

	t.Run("fetch_source_context", func(t *testing.T) {
		ctx := NewFetchSourceContext(true).Include("a").Exclude("b")
		ss := NewSearchSource().FetchSourceContext(ctx)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "_source")
	})

	t.Run("sort_simple", func(t *testing.T) {
		ss := NewSearchSource().Sort("price", true)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "sort")
	})

	t.Run("sort_with_info", func(t *testing.T) {
		ss := NewSearchSource().SortWithInfo(SortInfo{Field: "x", Ascending: false})
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "sort")
	})

	t.Run("track_total_hits_int", func(t *testing.T) {
		ss := NewSearchSource().TrackTotalHits(5000)
		src, err := ss.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 5000, m["track_total_hits"])
	})

	t.Run("default_rescore_window", func(t *testing.T) {
		ss := NewSearchSource().DefaultRescoreWindowSize(50).Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())))
		src, err := ss.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("index_boosts", func(t *testing.T) {
		ss := NewSearchSource().IndexBoosts(IndexBoost{Index: "a", Boost: 1.0})
		src, err := ss.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("marshal_json", func(t *testing.T) {
		ss := NewSearchSource().Size(5)
		data, err := ss.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "size")
	})

	t.Run("marshal_json_nil", func(t *testing.T) {
		var ss *SearchSource
		data, err := ss.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, "null", string(data))
	})
}

func TestFetchSourceContext(t *testing.T) {
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

	t.Run("include_exclude", func(t *testing.T) {
		fsc := NewFetchSourceContext(true).Include("a", "b").Exclude("c")
		src, err := fsc.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, []string{"a", "b"}, m["includes"])
		assert.Equal(t, []string{"c"}, m["excludes"])
	})

	t.Run("set_fetch_source", func(t *testing.T) {
		fsc := NewFetchSourceContext(true)
		fsc.SetFetchSource(false)
		assert.False(t, fsc.FetchSource())
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

func TestRescore_Source(t *testing.T) {
	t.Run("with_window", func(t *testing.T) {
		r := NewRescore().WindowSize(100).Rescorer(NewQueryRescorer(NewMatchAllQuery()))
		src, err := r.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 100, m["window_size"])
		assert.Contains(t, m, "query")
	})

	t.Run("default_window", func(t *testing.T) {
		r := NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))
		r.defaultRescoreWindowSize = ptr(50)
		src, err := r.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 50, m["window_size"])
	})

	t.Run("is_empty", func(t *testing.T) {
		assert.True(t, NewRescore().IsEmpty())
		assert.False(t, NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())).IsEmpty())
	})
}

func TestQueryRescorer_Source(t *testing.T) {
	q := NewQueryRescorer(NewMatchAllQuery()).
		RescoreQueryWeight(2.0).
		QueryWeight(1.0).
		ScoreMode("total")
	assert.Equal(t, "query", q.Name())
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "rescore_query")
	assert.Equal(t, 2.0, m["rescore_query_weight"])
	assert.Equal(t, 1.0, m["query_weight"])
	assert.Equal(t, "total", m["score_mode"])
}

func TestGeoPoint_MarshalJSON(t *testing.T) {
	gp := GeoPointFromLatLon(40.0, -74.0)
	gp.Source()
	data, err := gp.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), "lat")
}

func TestDocvalueField_Source(t *testing.T) {
	t.Run("no_format", func(t *testing.T) {
		d := DocvalueField{Field: "date"}
		src, err := d.Source()
		require.NoError(t, err)
		assert.Equal(t, "date", src)
	})

	t.Run("with_format", func(t *testing.T) {
		d := DocvalueField{Field: "date", Format: "epoch_millis"}
		src, err := d.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "epoch_millis", m["format"])
	})

	t.Run("nil_fields", func(t *testing.T) {
		var d DocvalueFields
		src, err := d.Source()
		require.NoError(t, err)
		assert.Nil(t, src)
	})

	t.Run("multiple", func(t *testing.T) {
		d := DocvalueFields{{Field: "a"}, {Field: "b", Format: "x"}}
		src, err := d.Source()
		require.NoError(t, err)
		arr := src.([]any)
		assert.Len(t, arr, 2)
	})
}

func TestFieldField_Source(t *testing.T) {
	t.Run("no_format", func(t *testing.T) {
		f := FieldField{Field: "x"}
		src, err := f.Source()
		require.NoError(t, err)
		assert.Equal(t, "x", src)
	})

	t.Run("with_format", func(t *testing.T) {
		f := FieldField{Field: "x", Format: "y"}
		src, err := f.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "y", m["format"])
	})

	t.Run("nil_fields", func(t *testing.T) {
		var f FieldFields
		src, err := f.Source()
		require.NoError(t, err)
		assert.Nil(t, src)
	})
}

func TestInnerHit_Source(t *testing.T) {
	ih := NewInnerHit().
		Path("nested").
		Type("answer").
		Name("my_inner").
		Query(NewMatchAllQuery()).
		Collapse(NewCollapseBuilder("f")).
		From(0).
		Size(5).
		TrackScores(true).
		Explain(true).
		Version(true).
		StoredField("f1").
		StoredFields("f2", "f3").
		NoStoredFields().
		FetchSource(true).
		FetchSourceContext(NewFetchSourceContext(true)).
		DocvalueFields("dv1").
		DocvalueFieldsWithFormat(DocvalueField{Field: "dv2"}).
		DocvalueField("dv3").
		DocvalueFieldWithFormat(DocvalueField{Field: "dv4"}).
		ScriptFields(NewScriptField("sf", NewScriptInline("1"))).
		ScriptField(NewScriptField("sf2", NewScriptInline("2"))).
		Sort("price", true).
		SortWithInfo(SortInfo{Field: "x", Ascending: true}).
		SortBy(NewFieldSort("ts")).
		Highlight(NewHighlight())
	ih.Highlighter()
	assert.NotNil(t, ih)
	src, err := ih.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "my_inner", m["name"])
}

func TestPointInTime_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		pit := NewPointInTime("abc")
		src, err := pit.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "abc", m["id"])
	})

	t.Run("keep_alive", func(t *testing.T) {
		pit := NewPointInTimeWithKeepAlive("abc", "5m")
		src, err := pit.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "5m", m["keep_alive"])
	})

	t.Run("nil", func(t *testing.T) {
		var pit *PointInTime
		src, err := pit.Source()
		require.NoError(t, err)
		assert.Nil(t, src)
	})
}

func TestCollapseBuilder_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cb := NewCollapseBuilder("user")
		src, err := cb.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "user", m["field"])
	})

	t.Run("with_inner_hits_and_max", func(t *testing.T) {
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
	})
}

func TestTermsLookup_Source(t *testing.T) {
	tl := NewTermsLookup().
		Index("my-index").
		Type("_doc").
		Id("1").
		Path("followers").
		Routing("user_1")
	src, err := tl.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "my-index", m["index"])
	assert.Equal(t, "followers", m["path"])
}

func TestSortInfo_Extended(t *testing.T) {
	t.Run("with_filter", func(t *testing.T) {
		info := SortInfo{Field: "price", Ascending: false, Filter: NewMatchAllQuery()}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		price := m["price"].(map[string]any)
		assert.Contains(t, price, "filter")
	})

	t.Run("with_nested_filter", func(t *testing.T) {
		info := SortInfo{Field: "price", Ascending: true, NestedFilter: NewMatchAllQuery()}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		price := m["price"].(map[string]any)
		assert.Contains(t, price, "nested_filter")
	})

	t.Run("with_path", func(t *testing.T) {
		info := SortInfo{Field: "price", Ascending: true, Path: "nested"}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		price := m["price"].(map[string]any)
		assert.Equal(t, "nested", price["path"])
	})

	t.Run("with_nested_path", func(t *testing.T) {
		info := SortInfo{Field: "price", Ascending: true, NestedPath: "nested"}
		src, err := info.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("with_nested_sort_struct", func(t *testing.T) {
		ns := NewNestedSort("comments")
		info := SortInfo{Field: "price", Ascending: true, Nested: ns}
		src, err := info.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		price := m["price"].(map[string]any)
		assert.Contains(t, price, "nested")
	})

	t.Run("with_nested_sort_deprecated", func(t *testing.T) {
		ns := NewNestedSort("comments")
		info := SortInfo{Field: "price", Ascending: true, NestedSort: ns}
		src, err := info.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("with_ignore_unmapped", func(t *testing.T) {
		iu := true
		info := SortInfo{Field: "price", Ascending: true, IgnoreUnmapped: &iu, UnmappedType: "date", SortMode: "min"}
		src, err := info.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestFieldSort_Extended(t *testing.T) {
	s := NewFieldSort("ts").
		FieldName("ts").
		Order(false).
		Filter(NewMatchAllQuery()).
		Path("nested").
		Nested(NewNestedSort("nested")).
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested2").
		NestedSort(NewNestedSort("n2"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "ts")
}

func TestGeoDistanceSort_Extended(t *testing.T) {
	s := NewGeoDistanceSort("location").
		FieldName("location").
		Order(false).
		Point(40.0, -74.0).
		Points(GeoPointFromLatLon(35.0, -110.0)).
		GeoHashes("drm3btev3e86").
		IgnoreUnmapped(true).
		GeoDistance("arc").
		DistanceType("plane").
		SortMode("min").
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested").
		NestedSort(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	geo := m["_geo_distance"].(map[string]any)
	assert.Equal(t, true, geo["ignore_unmapped"])
}

func TestScriptSort_Extended(t *testing.T) {
	s := NewScriptSort(NewScriptInline("doc['x'].value"), "number").
		Type("number").
		Order(true).
		SortMode("min").
		NestedFilter(NewMatchAllQuery()).
		NestedPath("nested").
		NestedSort(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "_script")
}

func TestNestedSort_Extended(t *testing.T) {
	ns := NewNestedSort("comments").
		Filter(NewMatchAllQuery()).
		NestedSort(NewNestedSort("reply"))
	src, err := ns.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "comments", m["path"])
	assert.Contains(t, m, "filter")
	assert.Contains(t, m, "nested")
}

func TestScoreSort_Order(t *testing.T) {
	s := NewScoreSort().Order(true).Desc()
	src, err := s.Source()
	require.NoError(t, err)
	_ = src
}

func TestBoostingQuery_Source(t *testing.T) {
	boost := 2.0
	nb := 0.5
	q := BoostingQuery{Positive: NewMatchAllQuery(), Negative: NewMatchAllQuery(), NegativeBoost: &nb, Boost: &boost}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	body := m["boosting"].(map[string]any)
	assert.Contains(t, body, "positive")
	assert.Contains(t, body, "negative")
	assert.Equal(t, 0.5, body["negative_boost"])
}

func TestCombinedFieldsQuery_Source(t *testing.T) {
	q := NewCombinedFieldsQuery("hello world", "title", "body")
	q.Operator = "and"
	q.MinimumShouldMatch = "1"
	q.ZeroTermsQuery = "none"
	autoGen := true
	q.AutoGenerateSynonymsPhraseQuery = &autoGen
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["combined_fields"].(map[string]any)
	assert.Contains(t, inner, "query")
	assert.Equal(t, "and", inner["operator"])
}

func TestCommonTermsQuery_Source(t *testing.T) {
	cutoff := 0.001
	boost := 1.5
	q := CommonTermsQuery{
		Field: "message", Query: "hello world", CutoffFrequency: &cutoff,
		HighFreq: &boost, HighFreqOperator: "and", HighFreqMinimumShouldMatch: "1",
		LowFreq: &boost, LowFreqOperator: "or", LowFreqMinimumShouldMatch: "2",
		Analyzer: "standard", Boost: &boost, QueryName: "test",
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["common"].(map[string]any)["message"].(map[string]any)
	assert.Contains(t, inner, "query")
	assert.Contains(t, inner, "minimum_should_match")
}

func TestConstantScoreQuery_Source(t *testing.T) {
	boost := 2.0
	q := ConstantScoreQuery{Filter: NewTermQuery("status", "published"), Boost: &boost}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["constant_score"].(map[string]any)
	assert.Contains(t, inner, "filter")
	assert.Equal(t, 2.0, inner["boost"])
}

func TestDisMaxQuery_Source(t *testing.T) {
	boost := 2.0
	tb := 0.7
	q := DisMaxQuery{Queries: []Query{NewMatchAllQuery(), NewMatchAllQuery()}, Boost: &boost, TieBreaker: &tb, QueryName: "dmq"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["dis_max"].(map[string]any)
	assert.Contains(t, inner, "queries")
}

func TestDistanceFeatureQuery_Source(t *testing.T) {
	t.Run("string_origin", func(t *testing.T) {
		q := NewDistanceFeatureQuery("date", "2024-01-01", "1d")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "distance_feature")
	})

	t.Run("geopoint_origin", func(t *testing.T) {
		q := NewDistanceFeatureQuery("location", GeoPointFromLatLon(40.0, -74.0), "1km")
		src, err := q.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("geopoint_value_origin", func(t *testing.T) {
		q := DistanceFeatureQuery{Field: "loc", Origin: *GeoPointFromLatLon(1, 2), Pivot: "1km"}
		src, err := q.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("bad_origin", func(t *testing.T) {
		q := DistanceFeatureQuery{Field: "f", Origin: 123, Pivot: "1d"}
		_, err := q.Source()
		assert.Error(t, err)
	})
}

func TestFunctionScoreQuery_Source(t *testing.T) {
	q := NewFunctionScoreQuery().
		Query(NewMatchAllQuery()).
		Filter(NewMatchAllQuery()).
		AddScoreFunc(NewWeightFactorFunction(2.0)).
		Add(NewMatchAllQuery(), NewRandomFunction().Field("seed_field").Seed(42).Weight(1.5)).
		ScoreMode("sum").
		BoostMode("replace").
		MaxBoost(10.0).
		Boost(1.0).
		MinScore(0.5)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["function_score"].(map[string]any)
	assert.Contains(t, inner, "query")
	assert.Contains(t, inner, "functions")
}

func TestDecayFunctions_Source(t *testing.T) {
	t.Run("exponential", func(t *testing.T) {
		fn := NewExponentialDecayFunction().FieldName("date").Origin("now").Scale("10d").Decay(0.5).Offset("2d").Weight(1.5).MultiValueMode("min")
		assert.Equal(t, "exp", fn.Name())
		src, err := fn.Source()
		require.NoError(t, err)
		_ = src
		assert.Equal(t, ptrF(1.5), fn.GetWeight())
	})

	t.Run("gauss", func(t *testing.T) {
		fn := NewGaussDecayFunction().FieldName("date").Origin("now").Scale("10d").Decay(0.5).Offset("2d").Weight(1.0).MultiValueMode("max")
		assert.Equal(t, "gauss", fn.Name())
		src, err := fn.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("linear", func(t *testing.T) {
		fn := NewLinearDecayFunction().FieldName("price").Origin(100).Scale(50).Decay(0.5).Offset(10).Weight(2.0).MultiValueMode("avg")
		assert.Equal(t, "linear", fn.Name())
		assert.Equal(t, "avg", fn.GetMultiValueMode())
		src, err := fn.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestScriptFunction_Source(t *testing.T) {
	fn := NewScriptFunction(NewScriptInline("_score * 2")).Weight(1.5)
	assert.Equal(t, "script_score", fn.Name())
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "script")
}

func TestFieldValueFactorFunction_Source(t *testing.T) {
	fn := NewFieldValueFactorFunction().Field("likes").Factor(1.2).Modifier("log").Missing(1.0).Weight(2.0)
	assert.Equal(t, "field_value_factor", fn.Name())
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Equal(t, "likes", m["field"])
}

func TestWeightFactorFunction_Source(t *testing.T) {
	fn := NewWeightFactorFunction(5.0)
	assert.Equal(t, "weight", fn.Name())
	w := fn.GetWeight()
	assert.NotNil(t, w)
	src, err := fn.Source()
	require.NoError(t, err)
	assert.Equal(t, 5.0, src)
}

func TestRandomFunction_Source(t *testing.T) {
	fn := NewRandomFunction().Field("_seq_no").Seed(42).Weight(1.0)
	assert.Equal(t, "random_score", fn.Name())
	src, err := fn.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "field")
}

func TestGeoBoundingBoxQuery_Source(t *testing.T) {
	iu := true
	q := NewGeoBoundingBoxQuery("location").
		TopLeftCoords(40.73, -74.1).
		BottomRightCoords(40.01, -71.12)
	q = GeoBoundingBoxQuery{Field: q.Field, TopLeft: q.TopLeft, BottomRight: q.BottomRight, Type: "indexed", ValidationMethod: "COERCE", IgnoreUnmapped: &iu, QueryName: "geo_q"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "geo_bounding_box")
}

func TestGeoDistanceQuery_Source(t *testing.T) {
	q := NewGeoDistanceQuery("location").
		Point(40.0, -74.0).
		FromGeoPoint(GeoPointFromLatLon(40.0, -74.0))
	q = GeoDistanceQuery{Field: q.Field, Lat: q.Lat, Lon: q.Lon, Distance: "10km", DistanceType: "arc", QueryName: "geo_q"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "geo_distance")

	q2 := GeoDistanceQuery{Field: "loc", GeoHash: "drm3btev3e86"}
	src2, err := q2.Source()
	require.NoError(t, err)
	_ = src2
}

func TestGeoPolygonQuery_Source(t *testing.T) {
	q := NewGeoPolygonQuery("location").AddPoint(40.0, -74.0).AddPoint(41.0, -73.0).AddPoint(39.0, -75.0)
	q = GeoPolygonQuery{Field: q.Field, Points: q.Points, QueryName: "geo_poly"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "geo_polygon")
}

func TestHasChildQuery_Source(t *testing.T) {
	boost := 2.0
	mc := 1
	xc := 10
	scc := 5
	q := HasChildQuery{Query: NewMatchAllQuery(), Type: "answer", Boost: &boost, ScoreMode: "max", MinChildren: &mc, MaxChildren: &xc, ShortCircuitCutoff: &scc, QueryName: "hc", InnerHit: NewInnerHit().Name("inner")}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["has_child"].(map[string]any)
	assert.Equal(t, "answer", inner["type"])
}

func TestHasParentQuery_Source(t *testing.T) {
	boost := 2.0
	score := true
	iu := true
	q := HasParentQuery{Query: NewMatchAllQuery(), ParentType: "question", Boost: &boost, Score: &score, QueryName: "hp", InnerHit: NewInnerHit().Name("inner"), IgnoreUnmapped: &iu}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["has_parent"].(map[string]any)
	assert.Equal(t, "question", inner["parent_type"])
}

func TestIntervalQuery_Source(t *testing.T) {
	match := NewIntervalQueryRuleMatch("hello world").MaxGaps(2).Ordered(true).Analyzer("standard").UseField("text").Filter(NewIntervalQueryFilter().Before(NewIntervalQueryRuleMatch("world")))
	q := NewIntervalQuery("body", match)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "intervals")
}

func TestIntervalQueryRules(t *testing.T) {
	t.Run("all_of", func(t *testing.T) {
		r := NewIntervalQueryRuleAllOf(NewIntervalQueryRuleMatch("a"), NewIntervalQueryRuleMatch("b")).
			MaxGaps(5).Ordered(true).Filter(NewIntervalQueryFilter())
		assert.True(t, r.isIntervalQueryRule())
		src, err := r.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "all_of")
	})

	t.Run("any_of", func(t *testing.T) {
		r := NewIntervalQueryRuleAnyOf(NewIntervalQueryRuleMatch("a")).Filter(NewIntervalQueryFilter())
		assert.True(t, r.isIntervalQueryRule())
		src, err := r.Source()
		require.NoError(t, err)
		assert.Contains(t, src.(map[string]any), "any_of")
	})

	t.Run("fuzzy", func(t *testing.T) {
		r := NewIntervalQueryRuleFuzzy("hello").PrefixLength(2).Fuzziness("auto").Transpositions(true).Analyzer("standard").UseField("body")
		assert.True(t, r.isIntervalQueryRule())
		src, err := r.Source()
		require.NoError(t, err)
		assert.Contains(t, src.(map[string]any), "fuzzy")
	})

	t.Run("prefix", func(t *testing.T) {
		r := NewIntervalQueryRulePrefix("hel").Analyzer("standard").UseField("body")
		assert.True(t, r.isIntervalQueryRule())
		src, err := r.Source()
		require.NoError(t, err)
		assert.Contains(t, src.(map[string]any), "prefix")
	})

	t.Run("wildcard", func(t *testing.T) {
		r := NewIntervalQueryRuleWildcard("h*lo").Analyzer("standard").UseField("body")
		assert.True(t, r.isIntervalQueryRule())
		src, err := r.Source()
		require.NoError(t, err)
		assert.Contains(t, src.(map[string]any), "wildcard")
	})
}

func TestIntervalQueryFilter_All(t *testing.T) {
	m1 := NewIntervalQueryRuleMatch("a")
	f := NewIntervalQueryFilter().
		After(m1).Before(m1).ContainedBy(m1).Containing(m1).
		Overlapping(m1).NotContainedBy(m1).NotContaining(m1).NotOverlapping(m1).
		Script(NewScriptInline("return true"))
	assert.True(t, f.isIntervalQueryRule())
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

func TestMatchBoolPrefixQuery_Source(t *testing.T) {
	q := NewMatchBoolPrefixQuery("message", "quick brown fox")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "match_bool_prefix")
}

func TestMatchPhrasePrefixQuery_Source(t *testing.T) {
	q := NewMatchPhrasePrefixQuery("message", "quick brown fo")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "match_phrase_prefix")
}

func TestMoreLikeThisQuery_Source(t *testing.T) {
	t.Run("error_no_docs", func(t *testing.T) {
		q := NewMoreLikeThisQuery()
		_, err := q.Source()
		assert.Error(t, err)
	})

	t.Run("with_likes", func(t *testing.T) {
		q := NewMoreLikeThisQuery().
			Field("title", "body").
			LikeText("hello world").
			LikeItems(NewMoreLikeThisQueryItem().Id("1").Index("idx").Type("_doc").Doc(map[string]string{"a": "b"}).Fields("f").Routing("r").FetchSourceContext(NewFetchSourceContext(true)).Version(1).VersionType("external")).
			IgnoreLikeText("exclude").
			IgnoreLikeItems(NewMoreLikeThisQueryItem().Id("2")).
			Ids("3").
			Include(true).
			MinimumShouldMatch("5").
			MinTermFreq(1).
			MaxQueryTerms(10).
			StopWord("the", "a").
			MinDocFreq(2).
			MaxDocFreq(100).
			MinWordLength(3).
			MaxWordLength(20).
			BoostTerms(1.5).
			Analyzer("standard").
			Boost(2.0).
			FailOnUnsupportedField(false).
			QueryName("mlt")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		inner := m["more_like_this"].(map[string]any)
		assert.Contains(t, inner, "like")
	})
}

func TestMultiMatchQuery_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		q := NewMultiMatchQuery("hello", "title", "body")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "multi_match")
	})

	t.Run("all_fields", func(t *testing.T) {
		slop := 2
		pl := 1
		me := 50
		lenient := true
		cutoff := 0.01
		tb := 0.3
		boost := 2.0
		q := MultiMatchQuery{
			Query: "hello", Fields: []string{"title", "body"}, FieldBoosts: map[string]*float64{},
			Type: "best_fields", Operator: "and", Analyzer: "standard", Boost: &boost,
			Slop: &slop, Fuzziness: "auto", PrefixLength: &pl, MaxExpansions: &me,
			MinimumShouldMatch: "1", Rewrite: "constant_score", FuzzyRewrite: "top_terms_N",
			TieBreaker: &tb, Lenient: &lenient, CutoffFrequency: &cutoff,
			ZeroTermsQuery: "all", QueryName: "mm",
		}
		src, err := q.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestParentIdQuery_Source(t *testing.T) {
	iu := true
	boost := 2.0
	q := ParentIdQuery{Type: "answer", ID: "1", IgnoreUnmapped: &iu, Boost: &boost, QueryName: "pid", InnerHit: NewInnerHit().Name("inner")}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "parent_id")
}

func TestPercolatorQuery_Source(t *testing.T) {
	ver := int64(1)
	q := PercolatorQuery{
		Field: "query", DocumentType: "doc", Name: "my_query",
		Documents:            []any{map[string]string{"text": "hello"}},
		IndexedDocumentIndex: "idx", IndexedDocumentType: "_doc", IndexedDocumentID: "1",
		IndexedDocumentRouting: "r", IndexedDocumentPreference: "primary", IndexedDocumentVersion: &ver,
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "percolate")
}

func TestPinnedQuery_Source(t *testing.T) {
	q := PinnedQuery{IDs: []string{"1", "2"}, Organic: NewMatchAllQuery()}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["pinned"].(map[string]any)
	assert.Contains(t, inner, "ids")
}

func TestQueryStringQuery_Source(t *testing.T) {
	q := NewQueryStringQuery("(new york city) OR (big apple)").
		WithDefaultField("content").
		WithDefaultOperator("AND").
		WithAnalyzer("standard").
		WithQuoteAnalyzer("whitespace").
		WithQuoteFieldSuffix(".exact").
		WithAllowLeadingWildcard(true).
		WithLowercaseExpandedTerms(true).
		WithEnablePositionIncrements(true).
		WithAnalyzeWildcard(true).
		WithLocale("en").
		WithFuzziness("AUTO").
		WithFuzzyRewrite("constant_score").
		WithRewrite("constant_score").
		WithMinimumShouldMatch("1").
		WithLenient(true).
		WithQueryName("q").
		WithTimeZone("UTC").
		WithEscape(true).
		WithType("best_fields")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "query_string")
}

func TestRankFeatureQuery_Source(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		f := NewRankFeatureLogScoreFunction(10.0)
		q := RankFeatureQuery{Field: "pagerank", ScoreFunction: f}
		src, err := q.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("saturation", func(t *testing.T) {
		p := 5.0
		f := RankFeatureSaturationScoreFunction{Pivot: &p}
		assert.Equal(t, "saturation", f.Name())
		src, err := f.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("sigmoid", func(t *testing.T) {
		f := NewRankFeatureSigmoidScoreFunction(5.0, 0.5)
		assert.Equal(t, "sigmoid", f.Name())
		src, err := f.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("linear", func(t *testing.T) {
		f := NewRankFeatureLinearScoreFunction()
		assert.Equal(t, "linear", f.Name())
		src, err := f.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestRawStringQuery_Source(t *testing.T) {
	q := NewRawStringQuery(`{"match_all":{}}`)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "match_all")

	q2 := NewRawStringQuery("not-json")
	_, err = q2.Source()
	assert.Error(t, err)
}

func TestRegexpQuery_Source(t *testing.T) {
	ci := true
	mds := 100
	boost := 2.0
	q := RegexpQuery{Field: "name", regexpInner: regexpInner{Value: "joh.*", Flags: "ALL", Boost: &boost, Rewrite: "constant_score", CaseInsensitive: &ci, MaxDeterminizedStates: &mds, QueryName: "re"}}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "regexp")
}

func TestScriptScoreQuery_Source(t *testing.T) {
	ms := 0.5
	boost := 2.0
	q := ScriptScoreQuery{Query: NewMatchAllQuery(), Script: NewScriptInline("_score * 2"), MinScore: &ms, Boost: &boost, QueryName: "ss"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "script_score")
}

func TestScriptQuery_Source(t *testing.T) {
	q := ScriptQuery{Script: NewScriptInline("doc['x'].value > 0"), QueryName: "sq"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "script")
}

func TestSimpleQueryStringQuery_Source(t *testing.T) {
	lenient := true
	fuzzyTrans := true
	q := NewSimpleQueryStringQuery(`"fried eggs" +(eggplant | potato) -frittata`)
	q = &SimpleQueryStringQuery{
		Query: q.Query, Analyzer: "standard", DefaultOperator: "AND",
		Fields: []string{"title", "body"}, FieldBoosts: map[string]*float64{},
		MinimumShouldMatch: "1", Flags: "ALL", Lenient: &lenient,
		AnalyzeWildcard: &lenient, Locale: "en", QueryName: "sqs",
		AutoGenerateSynonymsPhraseQuery: &lenient, FuzzyTranspositions: &fuzzyTrans,
		FuzzyPrefixLength: 2, FuzzyMaxExpansions: 50,
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "simple_query_string")
}

func TestSliceQuery_Source(t *testing.T) {
	id := 0
	max := 4
	q := SliceQuery{Field: "_id", ID: &id, Max: &max}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "field")
}

func TestSpanFirstQuery_Source(t *testing.T) {
	boost := 2.0
	q := SpanFirstQuery{Match: NewSpanTermQuery("user", "kimchy"), End: 3, Boost: &boost, QueryName: "sf"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "span_first")
}

func TestSpanNearQuery_Source(t *testing.T) {
	slop := 5
	inOrder := true
	boost := 2.0
	q := SpanNearQuery{Clauses: []Query{NewSpanTermQuery("field", "a"), NewSpanTermQuery("field", "b")}, Slop: &slop, InOrder: &inOrder, Boost: &boost, QueryName: "sn"}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "span_near")
}

func TestSpanTermQuery_Source(t *testing.T) {
	boost := 2.0
	q := SpanTermQuery{Field: "user", spanTermInner: spanTermInner{Value: "kimchy", Boost: &boost, QueryName: "st"}}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "span_term")

	q2 := NewSpanTermQuery("field", "value")
	src2, err := q2.Source()
	require.NoError(t, err)
	_ = src2
}

func TestTermsSetQuery_Source(t *testing.T) {
	boost := 2.0
	q := TermsSetQuery{
		Field: "required_tags", Values: []any{"go", "python"},
		MinimumShouldMatchField: "min_required", MinimumShouldMatchScript: NewScriptInline("2"),
		Boost: &boost, QueryName: "tsq",
	}
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "terms_set")
}

func TestTypeQuery_Source(t *testing.T) {
	q := NewTypeQuery("_doc")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "type")
}

func TestWrapperQuery_Source(t *testing.T) {
	q := NewWrapperQuery("eyJ0ZXJtIjp7InN0YXR1cyI6InB1Ymxpc2hlZCJ9fQ==")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "wrapper")
}

func TestCompletionSuggester_Source(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cs := NewCompletionSuggester("song-suggest").Field("suggest").Prefix("nir").Text("nirvana").Analyzer("simple").Size(5).ShardSize(10).SkipDuplicates(true)
		assert.Equal(t, "song-suggest", cs.Name())
		src, err := cs.Source(false)
		require.NoError(t, err)
		_ = src
	})

	t.Run("with_context", func(t *testing.T) {
		cs := NewCompletionSuggester("s").Field("suggest").Prefix("test").
			ContextQuery(NewSuggesterCategoryQuery("color", "red")).
			ContextQueries(NewSuggesterCategoryQuery("size", "large"))
		src, err := cs.Source(true)
		require.NoError(t, err)
		_ = src
	})

	t.Run("fuzzy", func(t *testing.T) {
		fops := NewFuzzyCompletionSuggesterOptions().EditDistance(2).Transpositions(true).MinLength(3).PrefixLength(1).UnicodeAware(true).MaxDeterminizedStates(10)
		cs := NewCompletionSuggester("s").Field("suggest").Prefix("t").
			PrefixWithEditDistance("t", 1).PrefixWithOptions("t", fops).FuzzyOptions(fops).Fuzziness(1)
		src, err := cs.Source(false)
		require.NoError(t, err)
		_ = src
	})

	t.Run("regex", func(t *testing.T) {
		rops := NewRegexCompletionSuggesterOptions().Flags("ALL").MaxDeterminizedStates(10)
		cs := NewCompletionSuggester("s").Field("suggest").Regex("n.*").RegexWithOptions("n.*", rops).RegexOptions(rops)
		src, err := cs.Source(false)
		require.NoError(t, err)
		_ = src
	})
}

func TestPhraseSuggester_Source(t *testing.T) {
	gen := NewDirectCandidateGenerator("title").Field("title").PreFilter("standard").PostFilter("standard").SuggestMode("always").Accuracy(0.5).Size(5).Sort("score").StringDistance("levenshtein").MaxEdits(2).MaxInspections(5).MaxTermFreq(0.01).PrefixLength(1).MinWordLength(4).MinDocFreq(0.001)

	ps := NewPhraseSuggester("my-suggest").
		Field("title.trigram").Text("noble prize").Analyzer("standard").Size(1).ShardSize(5).
		ContextQuery(NewSuggesterCategoryQuery("cat")).
		ContextQueries(NewSuggesterCategoryQuery("cat2")).
		GramSize(3).MaxErrors(1.0).Separator("|").
		RealWordErrorLikelihood(0.95).Confidence(1.0).
		CandidateGenerator(gen).CandidateGenerators(gen).ClearCandidateGenerator().
		ForceUnigrams(true).SmoothingModel(NewLaplaceSmoothingModel(0.7)).
		TokenLimit(10).Highlight("<em>", "</em>").
		CollateQuery(NewScriptInline("return true")).
		CollatePreference("_local").CollateParams(map[string]any{"a": 1}).CollatePrune(true)
	assert.Equal(t, "my-suggest", ps.Name())
	src, err := ps.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestSmoothingModels(t *testing.T) {
	t.Run("stupid_backoff", func(t *testing.T) {
		sm := NewStupidBackoffSmoothingModel(0.4)
		assert.Equal(t, "stupid_backoff", sm.Type())
		src, err := sm.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 0.4, m["discount"])
	})

	t.Run("laplace", func(t *testing.T) {
		sm := NewLaplaceSmoothingModel(0.7)
		assert.Equal(t, "laplace", sm.Type())
		src, err := sm.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 0.7, m["alpha"])
	})

	t.Run("linear_interp", func(t *testing.T) {
		sm := NewLinearInterpolationSmoothingModel(0.1, 0.2, 0.7)
		assert.Equal(t, "linear_interpolation", sm.Type())
		src, err := sm.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, 0.1, m["trigram_lambda"])
	})
}

func TestTermSuggester_Source(t *testing.T) {
	ts := NewTermSuggester("my-suggest").
		Field("title").Text("openseerch").Analyzer("standard").Size(3).ShardSize(10).
		ContextQuery(NewSuggesterCategoryQuery("cat")).
		ContextQueries(NewSuggesterCategoryQuery("cat2"), NewSuggesterCategoryQuery("cat3")).
		SuggestMode("always").Accuracy(0.5).Sort("score").StringDistance("levenshtein").
		MaxEdits(2).MaxInspections(5).MaxTermFreq(0.01).PrefixLength(1).
		MinWordLength(4).MinDocFreq(0.001)
	assert.Equal(t, "my-suggest", ts.Name())
	src, err := ts.Source(true)
	require.NoError(t, err)
	_ = src
}

func TestContextSuggester_Source(t *testing.T) {
	cs := NewContextSuggester("ctx-suggest").Prefix("test").Field("suggest").Size(5).
		ContextQuery(NewSuggesterCategoryQuery("cat")).
		ContextQueries(NewSuggesterCategoryQuery("cat2"))
	assert.Equal(t, "ctx-suggest", cs.Name())
	src, err := cs.Source(false)
	require.NoError(t, err)
	_ = src
}

func TestSuggesterCategoryMapping_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := NewSuggesterCategoryMapping("color")
		src, err := m.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("single_val", func(t *testing.T) {
		m := NewSuggesterCategoryMapping("color").DefaultValues("red").FieldName("color_field")
		src, err := m.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("multi_val", func(t *testing.T) {
		m := NewSuggesterCategoryMapping("color").DefaultValues("red", "blue")
		src, err := m.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestSuggesterCategoryQuery_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		q := NewSuggesterCategoryQuery("color")
		src, err := q.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("with_values_boost", func(t *testing.T) {
		q := NewSuggesterCategoryQuery("color", "red").Value("blue").ValueWithBoost("green", 2)
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "color")
	})
}

func TestSuggesterCategoryIndex_Source(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		i := NewSuggesterCategoryIndex("color")
		src, err := i.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("single", func(t *testing.T) {
		i := NewSuggesterCategoryIndex("color", "red")
		src, err := i.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "red", m["color"])
	})

	t.Run("multi", func(t *testing.T) {
		i := NewSuggesterCategoryIndex("color", "red", "blue")
		i.Values("green")
		src, err := i.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestSuggesterGeoMapping_Source(t *testing.T) {
	m := NewSuggesterGeoMapping("location").
		DefaultLocations(GeoPointFromLatLon(40.0, -74.0), GeoPointFromLatLon(35.0, -110.0)).
		Precision("10km", "5km").
		Neighbors(true).
		FieldName("geo_field")
	src, err := m.Source()
	require.NoError(t, err)
	_ = src

	m2 := NewSuggesterGeoMapping("location").DefaultLocations(GeoPointFromLatLon(40.0, -74.0))
	src2, err := m2.Source()
	require.NoError(t, err)
	_ = src2
}

func TestSuggesterGeoQuery_Source(t *testing.T) {
	q := NewSuggesterGeoQuery("location", GeoPointFromLatLon(40.0, -74.0)).
		Precision("10km").Neighbours("n1", "n2").Boost(2)
	src, err := q.Source()
	require.NoError(t, err)
	_ = src
}

func TestSuggesterGeoIndex_Source(t *testing.T) {
	i := NewSuggesterGeoIndex("location").Locations(GeoPointFromLatLon(40.0, -74.0), GeoPointFromLatLon(35.0, -110.0))
	src, err := i.Source()
	require.NoError(t, err)
	_ = src

	i2 := NewSuggesterGeoIndex("location").Locations(GeoPointFromLatLon(40.0, -74.0))
	src2, err := i2.Source()
	require.NoError(t, err)
	_ = src2

	i3 := NewSuggesterGeoIndex("location")
	src3, err := i3.Source()
	require.NoError(t, err)
	_ = src3
}

func TestSuggestField_MarshalJSON(t *testing.T) {
	t.Run("single_input", func(t *testing.T) {
		sf := NewSuggestField("opensearch").Input("opensearch", "search").Weight(10).ContextQuery(NewSuggesterCategoryQuery("cat"))
		data, err := sf.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "input")
	})

	t.Run("multi_context", func(t *testing.T) {
		sf := NewSuggestField("test").ContextQuery(NewSuggesterCategoryQuery("cat"), NewSuggesterCategoryQuery("size"))
		data, err := sf.MarshalJSON()
		require.NoError(t, err)
		_ = data
	})
}

func TestAllMetricAggregations(t *testing.T) {
	tests := []struct {
		name string
		agg  Aggregation
		key  string
	}{
		{"max", MaxAggregation{Field: "price"}, "max"},
		{"min", MinAggregation{Field: "price"}, "min"},
		{"sum", SumAggregation{Field: "price"}, "sum"},
		{"value_count", ValueCountAggregation{Field: "price"}, "value_count"},
		{"stats", StatsAggregation{Field: "grade"}, "stats"},
		{"extended_stats", ExtendedStatsAggregation{Field: "grade", Format: "0.00", Missing: 0}, "extended_stats"},
		{"cardinality", CardinalityAggregation{Field: "author", PrecisionThreshold: ptr(int64(100)), Missing: "N/A"}, "cardinality"},
		{"geo_bounds", GeoBoundsAggregation{Field: "location"}, "geo_bounds"},
		{"geo_centroid", GeoCentroidAggregation{Field: "location"}, "geo_centroid"},
		{"percentiles", PercentilesAggregation{Field: "load_time", Percentiles: []float64{25, 50, 75, 99}}, "percentiles"},
		{"percentile_ranks", PercentileRanksAggregation{Field: "load_time", Values: []float64{15, 30}, Compression: ptrF(200), Estimator: "auto"}, "percentile_ranks"},
		{"median_abs_dev", MedianAbsoluteDeviationAggregation{Field: "rating", Compression: ptrF(200), Format: "#.##", Missing: 0}, "median_absolute_deviation"},
		{"matrix_stats", MatrixStatsAggregation{Fields: []string{"poverty", "income"}}, "matrix_stats"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := tt.agg.Source()
			require.NoError(t, err)
			m := src.(map[string]any)
			assert.Contains(t, m, tt.key)
		})
	}
}

func TestCardinalityAggregation_AllFields(t *testing.T) {
	rehash := true
	a := CardinalityAggregation{Field: "author", PrecisionThreshold: ptr(int64(100)), Rehash: &rehash, Format: "#", Missing: "N/A"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["cardinality"].(map[string]any)
	assert.Equal(t, "author", inner["field"])
}

func TestExtendedStatsAggregation_Sigma(t *testing.T) {
	sigma := 2.0
	a := ExtendedStatsAggregation{Field: "grade", Sigma: &sigma}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["extended_stats"].(map[string]any)
	assert.Equal(t, 2.0, inner["sigma"])
}

func TestGeoBoundsAggregation_WrapLongitude(t *testing.T) {
	wl := true
	a := GeoBoundsAggregation{Field: "location", WrapLongitude: &wl}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["geo_bounds"].(map[string]any)
	assert.Equal(t, true, inner["wrap_longitude"])
}

func TestScriptedMetricAggregation_Source(t *testing.T) {
	a := ScriptedMetricAggregation{
		InitScript:    NewScriptInline("state.transactions = []"),
		MapScript:     NewScriptInline("state.transactions.add(doc.type.value)"),
		CombineScript: NewScriptInline("return state.transactions"),
		ReduceScript:  NewScriptInline("return states"),
		Params:        map[string]any{"factor": 2},
		Meta:          map[string]any{"x": "y"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "scripted_metric")
}

func TestTopHitsAggregation_Source(t *testing.T) {
	a := NewTopHitsAggregation().From(0).Size(5).TrackScores(true).Explain(true).Version(true).
		NoStoredFields().FetchSource(true).FetchSourceContext(NewFetchSourceContext(true)).
		DocvalueFields("f1").DocvalueFieldsWithFormat(DocvalueField{Field: "f2"}).
		DocvalueField("f3").DocvalueFieldWithFormat(DocvalueField{Field: "f4"}).
		ScriptFields(NewScriptField("s", NewScriptInline("1"))).
		ScriptField(NewScriptField("s2", NewScriptInline("2"))).
		Sort("date", true).SortWithInfo(SortInfo{}).SortBy(NewFieldSort("date")).
		Highlight(NewHighlight())
	a.Highlighter()
	a.SearchSourceBuilder(NewSearchSource())
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "top_hits")
}

func TestWeightedAvgAggregation_Source(t *testing.T) {
	a := WeightedAvgAggregation{
		Fields: map[string]*MultiValuesSourceFieldConfig{"grade": {FieldName: "grade"}},
		Value:  &MultiValuesSourceFieldConfig{FieldName: "grade"},
		Weight: &MultiValuesSourceFieldConfig{FieldName: "weight"},
		Format: "#.##", ValueType: "double",
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "weighted_avg")
}

func TestAllBucketAggregations(t *testing.T) {
	tests := []struct {
		name string
		agg  Aggregation
		key  string
	}{
		{"global", NewGlobalAggregation(), "global"},
		{"missing", NewMissingAggregation(), "missing"},
		{"nested", NestedAggregation{Path: "comments"}, "nested"},
		{"reverse_nested", NewReverseNestedAggregation(), "reverse_nested"},
		{"children", ChildrenAggregation{Type: "answer"}, "children"},
		{"filter", FilterAggregation{Filter: NewMatchAllQuery()}, "filter"},
		{"sampler", NewSamplerAggregation(), "sampler"},
		{"diversified_sampler", NewDiversifiedSamplerAggregation(), "diversified_sampler"},
		{"adjacency_matrix", NewAdjacencyMatrixAggregation(), "adjacency_matrix"},
		{"geo_hash_grid", GeoHashGridAggregation{GeoHashField: "location"}, "geohash_grid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := tt.agg.Source()
			require.NoError(t, err)
			m := src.(map[string]any)
			assert.Contains(t, m, tt.key)
		})
	}
}

func TestRangeAggregation_Source(t *testing.T) {
	keyed := true
	a := NewRangeAggregation().
		AddRange(0, 50).AddRangeWithKey("mid", 50, 100).
		AddUnboundedTo(0).AddUnboundedToWithKey("k", 10).
		AddUnboundedFrom(100).AddUnboundedFromWithKey("k", 50).
		Lt(10).LtWithKey("k", 20).
		Between(10, 20).BetweenWithKey("k", 30, 40).
		Gt(50).GtWithKey("k", 60)
	a = &RangeAggregation{FieldVal: a.FieldVal, Ranges: a.Ranges, Keyed: &keyed, Unmapped: &keyed, Missing: 0}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "range")
}

func TestDateRangeAggregation_Source(t *testing.T) {
	a := NewDateRangeAggregation().
		WithField("created").WithFormat("MM-yyyy").WithTimeZone("UTC").WithKeyed(true).WithUnmapped(true).
		AddRange("now-1M", "now").AddRangeWithKey("recent", "now-1M", "now").
		AddUnboundedTo("now-1M").AddUnboundedToWithKey("old", "now-1y").
		AddUnboundedFrom("now").AddUnboundedFromWithKey("new", "now").
		Lt("now").LtWithKey("k", "now").
		Between("2024-01-01", "2024-12-31").BetweenWithKey("k", "2024-01-01", "2024-12-31").
		Gt("2024-01-01").GtWithKey("k", "2024-01-01").
		WithSubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"}).
		WithScript(NewScriptInline("doc['date'].value"))
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "date_range")
}

func TestHistogramAggregation_Source(t *testing.T) {
	a := NewHistogramAggregation()
	a = a.Field_("price").Interval_(50).MinDocCount_(1).Offset_(5).
		ExtendedBounds(0, 500).ExtendedBoundsMin(0).ExtendedBoundsMax(500).
		OrderByCountAsc().OrderByCountDesc().OrderByKeyAsc().OrderByKeyDesc().
		OrderByAggregation("agg", true).OrderByAggregationAndMetric("agg", "metric", true).
		SubAggregation("avg_price", AvgAggregation{Field: "price"}).
		Meta_(map[string]any{"x": "y"}).Missing_(0)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "histogram")
}

func TestDateHistogramAggregation_Source(t *testing.T) {
	a := NewDateHistogramAggregation()
	a = a.Field_("timestamp").CalendarInterval_("month").FixedInterval_("1d").Interval_("1d").
		TimeZone_("UTC").Format_("yyyy-MM-dd").Offset_("+01:00").
		MinDocCount_(1).Keyed_(true).
		ExtendedBounds("2024-01-01", "2024-12-31").
		OrderByCountAsc().OrderByCountDesc().OrderByKeyAsc().OrderByKeyDesc().
		OrderByAggregation("agg", true).OrderByAggregationAndMetric("agg", "m", true).
		SubAggregation("sub", NewAvgAggregation()).Meta_(nil).Missing_("2024-01-01").
		Script_(nil)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "date_histogram")
}

func TestAutoDateHistogramAggregation_Source(t *testing.T) {
	a := NewAutoDateHistogramAggregation().
		WithBuckets(10).WithMinDocCount(1).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"})
	a = &AutoDateHistogramAggregation{Field: a.Field, Buckets: a.Buckets, MinDocCount: a.MinDocCount, SubAggs: a.SubAggs, Meta: a.Meta, Format: "yyyy-MM", MinimumInterval: "month", TimeZone: "UTC"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "auto_date_histogram")
}

func TestFiltersAggregation_Source(t *testing.T) {
	t.Run("named", func(t *testing.T) {
		a := NewFiltersAggregation()
		a.NamedFilters["errors"] = NewTermQuery("status", "error")
		a.NamedFilters["warnings"] = NewTermQuery("status", "warning")
		other := true
		a.OtherBucket = &other
		a.OtherBucketKey = "other"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "filters")
	})

	t.Run("unnamed", func(t *testing.T) {
		a := FiltersAggregation{UnnamedFilters: []Query{NewMatchAllQuery(), NewMatchAllQuery()}}
		src, err := a.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestGeoDistanceAggregation_Source(t *testing.T) {
	a := NewGeoDistanceAggregation().
		AddRange(0, 100).AddRangeWithKey("near", 0, 100).
		AddUnboundedTo(50.0).AddUnboundedToWithKey("far", 100.0).
		AddUnboundedFrom(100.0).AddUnboundedFromWithKey("close", 100.0).
		Between(50, 100).BetweenWithKey("mid", "50km", "100km").
		SubAggregation("sub", NewAvgAggregation())
	a = &GeoDistanceAggregation{Field: "location", Unit: "km", DistanceType: "arc", Origin: "40.0,-74.0", Ranges: a.Ranges, SubAggs: a.SubAggs}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "geo_distance")
}

func TestGeoHashGridAggregation_AllFields(t *testing.T) {
	size := 10
	ss := 5
	a := GeoHashGridAggregation{GeoHashField: "location", Precision: 3, Size: &size, ShardSize: &ss}
	a.SubAggregation("sub", NewAvgAggregation())
	src, err := a.Source()
	require.NoError(t, err)
	_ = src
}

func TestGeoTileGridAggregation_Source(t *testing.T) {
	t.Run("no_field_error", func(t *testing.T) {
		a := NewGeoTileGridAggregation()
		_, err := a.Source()
		assert.Error(t, err)
	})

	t.Run("valid", func(t *testing.T) {
		a := NewGeoTileGridAggregation().WithPrecision(14).WithSize(10).WithShardSize(100).WithBounds(BoundingBox{TopLeft: *GeoPointFromLatLon(90, -180), BottomRight: *GeoPointFromLatLon(-90, 180)}).WithMeta(nil)
		a = &GeoTileGridAggregation{Field: "location", Precision: a.Precision, Size: a.Size, ShardSize: a.ShardSize, Bounds: a.Bounds}
		a.SubAggregation("sub", NewAvgAggregation())
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "geotile_grid")
	})
}

func TestIPRangeAggregation_Source(t *testing.T) {
	a := NewIPRangeAggregation().WithField("ip_addr").WithKeyed(true).
		AddMaskRange("192.168.0.0/24").AddMaskRangeWithKey("k", "10.0.0.0/8").
		AddRange("192.168.0.0", "192.168.255.255").AddRangeWithKey("k", "10.0.0.0", "10.255.255.255").
		AddUnboundedTo("192.168.0.0").AddUnboundedToWithKey("k", "10.0.0.0").
		AddUnboundedFrom("192.168.255.255").AddUnboundedFromWithKey("k", "10.255.255.255").
		Lt("192.168.255.255").LtWithKey("k", "10.255.255.255").
		Between("192.168.0.0", "192.168.255.255").BetweenWithKey("k", "10.0.0.0", "10.255.255.255").
		Gt("192.168.0.0").GtWithKey("k", "10.0.0.0").
		WithSubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "ip_range")
}

func TestMultiTermsAggregation_Source(t *testing.T) {
	a := NewMultiTermsAggregation().
		Terms("genre", "product").MultiTerms(MultiTerm{Field: "extra"}).
		WithSize(10).WithShardSize(100).WithMinDocCount(1).WithShardMinDocCount(5).
		WithCollectionMode("breadth_first").WithShowTermDocCountError(true).
		OrderByCountAsc().OrderByCountDesc().OrderByKeyAsc().OrderByKeyDesc().
		OrderByAggregation("agg", true).OrderByAggregationAndMetric("agg", "m", true).
		Order("field", true).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "multi_terms")
}

func TestSignificantTermsAggregation_Source(t *testing.T) {
	a := NewSignificantTermsAggregation().
		Include("term.*").IncludeValues("v1").Exclude("stop.*").ExcludeValues("v2").
		Partition(0).NumPartitions(10).BackgroundFilter(NewMatchAllQuery()).
		SignificanceHeuristic(NewJLHScoreSignificanceHeuristic()).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"})
	a = &SignificantTermsAggregation{FieldVal: "text", RequiredSize: ptr(20), ShardSize: ptr(100), MinDocCount: ptr(5), ShardMinDocCount: ptr(10), ExecutionHint: "map", SubAggs: a.SubAggs, Meta: a.Meta, Filter: a.Filter, Heuristic: a.Heuristic, IncludeExclude: a.IncludeExclude}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "significant_terms")
}

func TestSignificanceHeuristics(t *testing.T) {
	t.Run("chi_square", func(t *testing.T) {
		bs := true
		inc := true
		h := ChiSquareSignificanceHeuristic{BackgroundIsSupersetVal: &bs, IncludeNegativesVal: &inc}
		assert.Equal(t, "chi_square", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("gnd", func(t *testing.T) {
		bs := true
		h := GNDSignificanceHeuristic{BackgroundIsSupersetVal: &bs}
		assert.Equal(t, "gnd", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("mutual_information", func(t *testing.T) {
		bs := true
		inc := true
		h := MutualInformationSignificanceHeuristic{BackgroundIsSupersetVal: &bs, IncludeNegativesVal: &inc}
		assert.Equal(t, "mutual_information", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("percentage", func(t *testing.T) {
		h := NewPercentageScoreSignificanceHeuristic()
		assert.Equal(t, "percentage", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		_ = src
	})

	t.Run("script", func(t *testing.T) {
		h := ScriptSignificanceHeuristic{ScriptVal: NewScriptInline("return true")}
		assert.Equal(t, "script_heuristic", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		_ = src
	})
}

func TestSignificantTextAggregation_Source(t *testing.T) {
	a := NewSignificantTextAggregation().
		Include("term.*").IncludeValues("v1").Exclude("stop.*").ExcludeValues("v2").
		Partition(0).NumPartitions(10).BackgroundFilter(NewMatchAllQuery()).
		SignificanceHeuristic(NewJLHScoreSignificanceHeuristic()).
		MinDocCount(5).ShardMinDocCount(10).WithSize(20).WithShardSize(100).
		SubAggregation("sub", NewAvgAggregation()).
		WithMeta(map[string]any{"x": "y"})
	a = &SignificantTextAggregation{FieldVal: "body", Filter: a.Filter, Heuristic: a.Heuristic, IncludeExclude: a.IncludeExclude, SubAggs: a.SubAggs, Meta: a.Meta, BucketCountThresholds: a.BucketCountThresholds}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "significant_text")
}

func TestRareTermsAggregation_Source(t *testing.T) {
	mc := 3
	prec := 0.001
	a := RareTermsAggregation{Field: "genre", MaxDocCount: &mc, Precision: &prec, Missing: "N/A", IncludeExclude: &TermsAggregationIncludeExclude{Include: "go.*"}}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "rare_terms")
}

func TestCompositeAggregation_Source(t *testing.T) {
	src := NewCompositeAggregation().
		WithSize(10).
		AggregateAfter(map[string]any{"product": "bag"}).
		Sources(
			NewCompositeAggregationTermsValuesSource("product").Field("product").Asc().Desc().
				OrderValue("asc").ValueTypeValue("string").MissingValue("N/A").MissingBucketValue(true).
				SetScript(NewScriptInline("doc['product'].value")),
			NewCompositeAggregationHistogramValuesSource("price", 50).Field("price").Asc().Desc().
				OrderValue("asc").ValueTypeValue("double").MissingValue(0).MissingBucketValue(true).
				SetScript(NewScriptInline("doc['price'].value")).IntervalValue(100),
			NewCompositeAggregationDateHistogramValuesSource("date").Field("timestamp").Asc().Desc().
				OrderValue("asc").ValueTypeValue("date").MissingValue("2024-01-01").MissingBucketValue(true).
				SetScript(NewScriptInline("doc['date'].value")).IntervalValue(1).
				FixedIntervalValue("1d").CalendarIntervalValue("month").FormatValue("yyyy-MM-dd").TimeZoneValue("UTC"),
		).
		SubAggregation("avg_price", AvgAggregation{Field: "price"}).
		WithMeta(map[string]any{"x": "y"})
	s, err := src.Source()
	require.NoError(t, err)
	m := s.(map[string]any)
	assert.Contains(t, m, "composite")
}

func TestDiversifiedSamplerAggregation_Source(t *testing.T) {
	a := DiversifiedSamplerAggregation{Field: "author", ShardSize: 100, MaxDocsPerValue: 5, ExecutionHint: "map"}
	a.SubAggs = nil
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "diversified_sampler")
}

func TestAllPipelineAggregations(t *testing.T) {
	tests := []struct {
		name string
		agg  Aggregation
		key  string
	}{
		{"avg_bucket", AvgBucketAggregation{BucketsPaths: []string{"agg>"}, Format: "#.##"}, "avg_bucket"},
		{"sum_bucket", SumBucketAggregation{BucketsPaths: []string{"agg>"}, GapPolicy: "skip"}, "sum_bucket"},
		{"max_bucket", MaxBucketAggregation{BucketsPaths: []string{"agg>"}}, "max_bucket"},
		{"min_bucket", MinBucketAggregation{BucketsPaths: []string{"agg>"}}, "min_bucket"},
		{"stats_bucket", StatsBucketAggregation{BucketsPaths: []string{"agg>"}}, "stats_bucket"},
		{"percentiles_bucket", PercentilesBucketAggregation{BucketsPaths: []string{"agg>"}, Percents: []float64{25, 50, 75}}, "percentiles_bucket"},
		{"derivative", DerivativeAggregation{BucketsPaths: []string{"agg>"}, Unit: "day"}, "derivative"},
		{"cumulative_sum", CumulativeSumAggregation{BucketsPaths: []string{"agg>"}, Format: "#.##"}, "cumulative_sum"},
		{"serial_diff", SerialDiffAggregation{BucketsPaths: []string{"agg>"}, Lag: ptr(1)}, "serial_diff"},
		{"bucket_script", BucketScriptAggregation{BucketsPathsMap: map[string]string{"total": "agg"}, Script: NewScriptInline("params.total / 100")}, "bucket_script"},
		{"bucket_selector", BucketSelectorAggregation{BucketsPathsMap: map[string]string{"total": "agg"}, Script: NewScriptInline("params.total > 200")}, "bucket_selector"},
		{"bucket_sort", BucketSortAggregation{Sorters: []Sorter{NewFieldSort("total_sales").Desc()}, Size: 3, From: 1}, "bucket_sort"},
		{"moving_fn", NewMovFnAggregation("agg>", NewScriptInline("MovingFunctions.unweightedAvg(values)"), 10), "moving_fn"},
		{"extended_stats_bucket", ExtendedStatsBucketAggregation{BucketsPaths: []string{"agg>"}}, "extended_stats_bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := tt.agg.Source()
			require.NoError(t, err)
			m := src.(map[string]any)
			assert.Contains(t, m, tt.key)
		})
	}
}

func TestMovAvgAggregation_Source(t *testing.T) {
	t.Run("ewma", func(t *testing.T) {
		alpha := 0.5
		m := EWMAMovAvgModel{Alpha: &alpha}
		assert.Equal(t, "ewma", m.Name())
		assert.NotNil(t, m.Settings())
	})

	t.Run("holt", func(t *testing.T) {
		a := 0.3
		b := 0.2
		m := HoltLinearMovAvgModel{Alpha: &a, Beta: &b}
		assert.Equal(t, "holt", m.Name())
		assert.NotNil(t, m.Settings())
	})

	t.Run("holt_winters", func(t *testing.T) {
		a := 0.5
		b := 0.4
		g := 0.3
		p := 4
		pad := true
		m := HoltWintersMovAvgModel{Alpha: &a, Beta: &b, Gamma: &g, Period: &p, SeasonalityType: "add", Pad: &pad}
		assert.Equal(t, "holt_winters", m.Name())
		assert.NotNil(t, m.Settings())
	})

	t.Run("linear", func(t *testing.T) {
		m := NewLinearMovAvgModel()
		assert.Equal(t, "linear", m.Name())
	})

	t.Run("simple", func(t *testing.T) {
		m := NewSimpleMovAvgModel()
		assert.Equal(t, "simple", m.Name())
	})

	w := 10
	pr := 5
	mn := true
	agg := MovAvgAggregation{Model: NewEWMAMovAvgModel(), Window: &w, Predict: &pr, Minimize: &mn, BucketsPaths: []string{"agg"}, Format: "#", GapPolicy: "skip"}
	src, err := agg.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "moving_avg")
}

func TestExtendedStatsBucketAggregation_Sigma(t *testing.T) {
	s := float32(2.0)
	a := ExtendedStatsBucketAggregation{BucketsPaths: []string{"agg"}, Format: "#.##", GapPolicy: "skip", Sigma: &s}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["extended_stats_bucket"].(map[string]any)
	assert.InDelta(t, 2.0, inner["sigma"], 0.01)
}

func TestPercentilesAggregation_AllOptions(t *testing.T) {
	comp := 200.0
	nsvd := 4
	a := PercentilesAggregation{Field: "load_time", Percentiles: []float64{25, 50, 75}, Method: "tdigest", Compression: &comp}
	src, err := a.Source()
	require.NoError(t, err)
	_ = src

	a2 := PercentilesAggregation{Field: "load_time", Method: "hdr", NumberOfSignificantValueDigits: &nsvd, Estimator: "auto", Format: "#", Missing: 0}
	src2, err := a2.Source()
	require.NoError(t, err)
	_ = src2
}

func TestAdjacencyMatrixAggregation_WithFilters(t *testing.T) {
	a := NewAdjacencyMatrixAggregation()
	a.Filters["grpA"] = NewTermQuery("status", "active")
	a.Filters["grpB"] = NewTermQuery("status", "inactive")
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	inner := m["adjacency_matrix"].(map[string]any)
	assert.Contains(t, inner, "filters")
}

func TestSearchSource_MarshalJSON_Errors(t *testing.T) {
	ss := NewSearchSource()
	data, err := ss.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestSearchSource_EmptyRescore(t *testing.T) {
	ss := NewSearchSource().Rescorer(NewRescore())
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	if r, ok := m["rescore"]; ok {
		assert.Nil(t, r)
	}
}

func TestHighlight_Fields(t *testing.T) {
	hl := NewHighlight().Fields(NewHighlighterField("title"), NewHighlighterField("body"))
	src, err := hl.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "fields")
}

func TestScript_ScriptSetter(t *testing.T) {
	s := &Script{}
	s.Script("hello").Type("inline").Lang("painless")
	src, err := s.Source()
	require.NoError(t, err)
	_ = src
}

func TestSearchRequest(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		r := NewSearchRequest().
			Index("idx1", "idx2").Type("_doc").
			Routing("r1").Routings("r1", "r2").
			Preference("_local").RequestCache(true).
			IgnoreUnavailable(true).AllowNoIndices(true).
			ExpandWildcards("open").Scroll("2m").
			SearchTypeDfsQueryThenFetch().SearchTypeQueryThenFetch().
			AllowPartialSearchResults(true).
			BatchedReduceSize(10).MaxConcurrentShardRequests(5).PreFilterShardSize(100).
			Query(NewMatchAllQuery()).PostFilter(NewMatchAllQuery()).
			MinScore(0.5).From(0).Size(10).Explain(true).Version(true).
			IndexBoost("idx1", 1.5).Stats("group1").
			FetchSource(true).FetchSourceIncludeExclude([]string{"a"}, []string{"b"}).
			FetchSourceContext(NewFetchSourceContext(true)).
			DocValueField("dv1").DocValueFieldWithFormat(DocvalueField{Field: "dv2"}).
			DocValueFields("dv3", "dv4").DocValueFieldsWithFormat(DocvalueField{Field: "dv5"}).
			StoredField("sf1").NoStoredFields().StoredFields("sf2", "sf3").
			ScriptField(NewScriptField("sc1", NewScriptInline("1"))).
			ScriptFields(NewScriptField("sc2", NewScriptInline("2"))).
			Sort("date", true).SortWithInfo(SortInfo{}).SortBy(NewFieldSort("ts")).
			SearchAfter(1234).Slice(NewSliceQuery()).
			TrackScores(true).TrackTotalHits(true).
			Aggregation("agg1", NewTermsAggregation().WithField("genre")).
			Highlight(NewHighlight().Field("body")).
			Suggester(NewTermSuggester("s1").Field("title")).
			Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))).
			ClearRescorers().Profile(true).
			Collapse(NewCollapseBuilder("user")).
			PointInTime(NewPointInTime("abc123")).
			Timeout("1s").TerminateAfter(1000)
		assert.True(t, r.HasIndices())
		body, err := r.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "query")
		hdr := r.header()
		assert.NotNil(t, hdr)
	})

	t.Run("source_types", func(t *testing.T) {
		r := NewSearchRequest().Source(map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
		body, err := r.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})

	t.Run("source_search_source", func(t *testing.T) {
		r := NewSearchRequest().Source(NewSearchSource().Size(5))
		body, err := r.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})

	t.Run("source_raw", func(t *testing.T) {
		raw := json.RawMessage(`{"query":{"match_all":{}}}`)
		r := NewSearchRequest().Source(raw)
		body, err := r.Body()
		require.NoError(t, err)
		assert.Equal(t, `{"query":{"match_all":{}}}`, body)
	})

	t.Run("source_raw_ptr", func(t *testing.T) {
		raw := json.RawMessage(`{"query":{"match_all":{}}}`)
		r := NewSearchRequest().Source(&raw)
		body, err := r.Body()
		require.NoError(t, err)
		assert.Equal(t, `{"query":{"match_all":{}}}`, body)
	})

	t.Run("source_string", func(t *testing.T) {
		r := NewSearchRequest().Source(`{"query":{"match_all":{}}}`)
		body, err := r.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})

	t.Run("source_string_ptr", func(t *testing.T) {
		s := `{"query":{"match_all":{}}}`
		r := NewSearchRequest().Source(&s)
		body, err := r.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})

	t.Run("source_nil_string_ptr", func(t *testing.T) {
		r := NewSearchRequest().Source((*string)(nil))
		body, err := r.Body()
		require.NoError(t, err)
		assert.Equal(t, "{}", body)
	})

	t.Run("source_as_map", func(t *testing.T) {
		r := NewSearchRequest()
		src, err := r.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("search_source_nil", func(t *testing.T) {
		r := NewSearchRequest().SearchSource(nil)
		assert.NotNil(t, r.searchSource)
	})

	t.Run("header_single_idx", func(t *testing.T) {
		r := NewSearchRequest().Index("idx1").Type("_doc").Routing("r1")
		h := r.header()
		m := h.(map[string]any)
		assert.Equal(t, "idx1", m["index"])
	})

	t.Run("routings_nil", func(t *testing.T) {
		r := NewSearchRequest().Routings()
		_ = r
	})
}

func TestAggregations_Accessors(t *testing.T) {
	raw := json.RawMessage(`{"value": 42.0}`)
	null := json.RawMessage(`null`)
	bad := json.RawMessage(`{invalid`)

	t.Run("min_found", func(t *testing.T) {
		aggs := Aggregations{"my_min": raw}
		v, ok := aggs.Min("my_min")
		assert.True(t, ok)
		assert.NotNil(t, v)
	})

	t.Run("min_nil", func(t *testing.T) {
		aggs := Aggregations{"my_min": null}
		v, ok := aggs.Min("my_min")
		assert.True(t, ok)
		assert.NotNil(t, v)
	})

	t.Run("min_not_found", func(t *testing.T) {
		aggs := Aggregations{}
		_, ok := aggs.Min("x")
		assert.False(t, ok)
	})

	t.Run("max", func(t *testing.T) {
		aggs := Aggregations{"my_max": raw}
		v, ok := aggs.Max("my_max")
		assert.True(t, ok)
		_ = v
	})

	t.Run("max_nil", func(t *testing.T) {
		aggs := Aggregations{"my_max": null}
		v, ok := aggs.Max("my_max")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sum", func(t *testing.T) {
		aggs := Aggregations{"my_sum": raw}
		v, ok := aggs.Sum("my_sum")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sum_nil", func(t *testing.T) {
		aggs := Aggregations{"my_sum": null}
		v, ok := aggs.Sum("my_sum")
		assert.True(t, ok)
		_ = v
	})

	t.Run("avg", func(t *testing.T) {
		aggs := Aggregations{"my_avg": raw}
		v, ok := aggs.Avg("my_avg")
		assert.True(t, ok)
		_ = v
	})

	t.Run("avg_nil", func(t *testing.T) {
		aggs := Aggregations{"my_avg": null}
		v, ok := aggs.Avg("my_avg")
		assert.True(t, ok)
		_ = v
	})

	t.Run("weighted_avg", func(t *testing.T) {
		aggs := Aggregations{"my_wavg": raw}
		v, ok := aggs.WeightedAvg("my_wavg")
		assert.True(t, ok)
		_ = v
	})

	t.Run("weighted_avg_nil", func(t *testing.T) {
		aggs := Aggregations{"my_wavg": null}
		v, ok := aggs.WeightedAvg("my_wavg")
		assert.True(t, ok)
		_ = v
	})

	t.Run("median_abs_dev", func(t *testing.T) {
		aggs := Aggregations{"my_mad": raw}
		v, ok := aggs.MedianAbsoluteDeviation("my_mad")
		assert.True(t, ok)
		_ = v
	})

	t.Run("median_abs_dev_nil", func(t *testing.T) {
		aggs := Aggregations{"my_mad": null}
		v, ok := aggs.MedianAbsoluteDeviation("my_mad")
		assert.True(t, ok)
		_ = v
	})

	t.Run("value_count", func(t *testing.T) {
		aggs := Aggregations{"my_vc": raw}
		v, ok := aggs.ValueCount("my_vc")
		assert.True(t, ok)
		_ = v
	})

	t.Run("value_count_nil", func(t *testing.T) {
		aggs := Aggregations{"my_vc": null}
		v, ok := aggs.ValueCount("my_vc")
		assert.True(t, ok)
		_ = v
	})

	t.Run("cardinality", func(t *testing.T) {
		aggs := Aggregations{"my_card": raw}
		v, ok := aggs.Cardinality("my_card")
		assert.True(t, ok)
		_ = v
	})

	t.Run("cardinality_nil", func(t *testing.T) {
		aggs := Aggregations{"my_card": null}
		v, ok := aggs.Cardinality("my_card")
		assert.True(t, ok)
		_ = v
	})

	t.Run("stats", func(t *testing.T) {
		sr := json.RawMessage(`{"count":10,"min":1.0,"max":100.0,"avg":50.5,"sum":505.0}`)
		aggs := Aggregations{"my_stats": sr}
		v, ok := aggs.Stats("my_stats")
		assert.True(t, ok)
		assert.NotNil(t, v)
	})

	t.Run("stats_nil", func(t *testing.T) {
		aggs := Aggregations{"my_stats": null}
		v, ok := aggs.Stats("my_stats")
		assert.True(t, ok)
		_ = v
	})

	t.Run("extended_stats", func(t *testing.T) {
		esr := json.RawMessage(`{"count":10,"min":1.0,"max":100.0,"avg":50.5,"sum":505.0,"sum_of_squares":100,"variance":2.0,"std_deviation":1.4}`)
		aggs := Aggregations{"my_es": esr}
		v, ok := aggs.ExtendedStats("my_es")
		assert.True(t, ok)
		_ = v
	})

	t.Run("extended_stats_nil", func(t *testing.T) {
		aggs := Aggregations{"my_es": null}
		v, ok := aggs.ExtendedStats("my_es")
		assert.True(t, ok)
		_ = v
	})

	t.Run("matrix_stats", func(t *testing.T) {
		msr := json.RawMessage(`{"fields":[{"name":"x","count":10}]}`)
		aggs := Aggregations{"my_ms": msr}
		v, ok := aggs.MatrixStats("my_ms")
		assert.True(t, ok)
		_ = v
	})

	t.Run("matrix_stats_nil", func(t *testing.T) {
		aggs := Aggregations{"my_ms": null}
		v, ok := aggs.MatrixStats("my_ms")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentiles", func(t *testing.T) {
		pr := json.RawMessage(`{"values":{"25.0":1.0,"50.0":2.0,"75.0":3.0}}`)
		aggs := Aggregations{"my_p": pr}
		v, ok := aggs.Percentiles("my_p")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentiles_nil", func(t *testing.T) {
		aggs := Aggregations{"my_p": null}
		v, ok := aggs.Percentiles("my_p")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentile_ranks", func(t *testing.T) {
		aggs := Aggregations{"my_pr": raw}
		v, ok := aggs.PercentileRanks("my_pr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentile_ranks_nil", func(t *testing.T) {
		aggs := Aggregations{"my_pr": null}
		v, ok := aggs.PercentileRanks("my_pr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("top_hits", func(t *testing.T) {
		thr := json.RawMessage(`{"hits":{"total":{"value":10,"relation":"eq"},"hits":[]}}`)
		aggs := Aggregations{"my_th": thr}
		v, ok := aggs.TopHits("my_th")
		assert.True(t, ok)
		_ = v
	})

	t.Run("top_hits_nil", func(t *testing.T) {
		aggs := Aggregations{"my_th": null}
		v, ok := aggs.TopHits("my_th")
		assert.True(t, ok)
		_ = v
	})

	t.Run("global", func(t *testing.T) {
		gr := json.RawMessage(`{"doc_count":100}`)
		aggs := Aggregations{"my_g": gr}
		v, ok := aggs.Global("my_g")
		assert.True(t, ok)
		_ = v
	})

	t.Run("global_nil", func(t *testing.T) {
		aggs := Aggregations{"my_g": null}
		v, ok := aggs.Global("my_g")
		assert.True(t, ok)
		_ = v
	})

	t.Run("filter", func(t *testing.T) {
		fr := json.RawMessage(`{"doc_count":50}`)
		aggs := Aggregations{"my_f": fr}
		v, ok := aggs.Filter("my_f")
		assert.True(t, ok)
		_ = v
	})

	t.Run("filter_nil", func(t *testing.T) {
		aggs := Aggregations{"my_f": null}
		v, ok := aggs.Filter("my_f")
		assert.True(t, ok)
		_ = v
	})

	t.Run("filters", func(t *testing.T) {
		flr := json.RawMessage(`{"buckets":[{}]}`)
		aggs := Aggregations{"my_flt": flr}
		v, ok := aggs.Filters("my_flt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("filters_nil", func(t *testing.T) {
		aggs := Aggregations{"my_flt": null}
		v, ok := aggs.Filters("my_flt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("adjacency_matrix", func(t *testing.T) {
		amr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_am": amr}
		v, ok := aggs.AdjacencyMatrix("my_am")
		assert.True(t, ok)
		_ = v
	})

	t.Run("adjacency_matrix_nil", func(t *testing.T) {
		aggs := Aggregations{"my_am": null}
		v, ok := aggs.AdjacencyMatrix("my_am")
		assert.True(t, ok)
		_ = v
	})

	t.Run("missing", func(t *testing.T) {
		mr := json.RawMessage(`{"doc_count":5}`)
		aggs := Aggregations{"my_m": mr}
		v, ok := aggs.Missing("my_m")
		assert.True(t, ok)
		_ = v
	})

	t.Run("missing_nil", func(t *testing.T) {
		aggs := Aggregations{"my_m": null}
		v, ok := aggs.Missing("my_m")
		assert.True(t, ok)
		_ = v
	})

	t.Run("nested", func(t *testing.T) {
		nr := json.RawMessage(`{"doc_count":5}`)
		aggs := Aggregations{"my_n": nr}
		v, ok := aggs.Nested("my_n")
		assert.True(t, ok)
		_ = v
	})

	t.Run("nested_nil", func(t *testing.T) {
		aggs := Aggregations{"my_n": null}
		v, ok := aggs.Nested("my_n")
		assert.True(t, ok)
		_ = v
	})

	t.Run("reverse_nested", func(t *testing.T) {
		rnr := json.RawMessage(`{"doc_count":5}`)
		aggs := Aggregations{"my_rn": rnr}
		v, ok := aggs.ReverseNested("my_rn")
		assert.True(t, ok)
		_ = v
	})

	t.Run("reverse_nested_nil", func(t *testing.T) {
		aggs := Aggregations{"my_rn": null}
		v, ok := aggs.ReverseNested("my_rn")
		assert.True(t, ok)
		_ = v
	})

	t.Run("children", func(t *testing.T) {
		cr := json.RawMessage(`{"doc_count":5}`)
		aggs := Aggregations{"my_c": cr}
		v, ok := aggs.Children("my_c")
		assert.True(t, ok)
		_ = v
	})

	t.Run("children_nil", func(t *testing.T) {
		aggs := Aggregations{"my_c": null}
		v, ok := aggs.Children("my_c")
		assert.True(t, ok)
		_ = v
	})

	t.Run("terms", func(t *testing.T) {
		tr := json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}`)
		aggs := Aggregations{"my_t": tr}
		v, ok := aggs.Terms("my_t")
		assert.True(t, ok)
		_ = v
	})

	t.Run("terms_nil", func(t *testing.T) {
		aggs := Aggregations{"my_t": null}
		v, ok := aggs.Terms("my_t")
		assert.True(t, ok)
		_ = v
	})

	t.Run("multi_terms", func(t *testing.T) {
		mtr := json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}`)
		aggs := Aggregations{"my_mt": mtr}
		v, ok := aggs.MultiTerms("my_mt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("multi_terms_nil", func(t *testing.T) {
		aggs := Aggregations{"my_mt": null}
		v, ok := aggs.MultiTerms("my_mt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("significant_terms", func(t *testing.T) {
		str := json.RawMessage(`{"doc_count":100,"buckets":[]}`)
		aggs := Aggregations{"my_st": str}
		v, ok := aggs.SignificantTerms("my_st")
		assert.True(t, ok)
		_ = v
	})

	t.Run("significant_terms_nil", func(t *testing.T) {
		aggs := Aggregations{"my_st": null}
		v, ok := aggs.SignificantTerms("my_st")
		assert.True(t, ok)
		_ = v
	})

	t.Run("rare_terms", func(t *testing.T) {
		rtr := json.RawMessage(`{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[]}`)
		aggs := Aggregations{"my_rt": rtr}
		v, ok := aggs.RareTerms("my_rt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("rare_terms_nil", func(t *testing.T) {
		aggs := Aggregations{"my_rt": null}
		v, ok := aggs.RareTerms("my_rt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sampler", func(t *testing.T) {
		sr := json.RawMessage(`{"doc_count":50}`)
		aggs := Aggregations{"my_s": sr}
		v, ok := aggs.Sampler("my_s")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sampler_nil", func(t *testing.T) {
		aggs := Aggregations{"my_s": null}
		v, ok := aggs.Sampler("my_s")
		assert.True(t, ok)
		_ = v
	})

	t.Run("diversified_sampler", func(t *testing.T) {
		dsr := json.RawMessage(`{"doc_count":50}`)
		aggs := Aggregations{"my_ds": dsr}
		v, ok := aggs.DiversifiedSampler("my_ds")
		assert.True(t, ok)
		_ = v
	})

	t.Run("diversified_sampler_nil", func(t *testing.T) {
		aggs := Aggregations{"my_ds": null}
		v, ok := aggs.DiversifiedSampler("my_ds")
		assert.True(t, ok)
		_ = v
	})

	t.Run("range", func(t *testing.T) {
		rr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_r": rr}
		v, ok := aggs.Range("my_r")
		assert.True(t, ok)
		_ = v
	})

	t.Run("range_nil", func(t *testing.T) {
		aggs := Aggregations{"my_r": null}
		v, ok := aggs.Range("my_r")
		assert.True(t, ok)
		_ = v
	})

	t.Run("keyed_range", func(t *testing.T) {
		krr := json.RawMessage(`{"buckets":{}}`)
		aggs := Aggregations{"my_kr": krr}
		v, ok := aggs.KeyedRange("my_kr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("keyed_range_nil", func(t *testing.T) {
		aggs := Aggregations{"my_kr": null}
		v, ok := aggs.KeyedRange("my_kr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("date_range", func(t *testing.T) {
		drr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_dr": drr}
		v, ok := aggs.DateRange("my_dr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("date_range_nil", func(t *testing.T) {
		aggs := Aggregations{"my_dr": null}
		v, ok := aggs.DateRange("my_dr")
		assert.True(t, ok)
		_ = v
	})

	t.Run("ip_range", func(t *testing.T) {
		ipr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_ip": ipr}
		v, ok := aggs.IPRange("my_ip")
		assert.True(t, ok)
		_ = v
	})

	t.Run("ip_range_nil", func(t *testing.T) {
		aggs := Aggregations{"my_ip": null}
		v, ok := aggs.IPRange("my_ip")
		assert.True(t, ok)
		_ = v
	})

	t.Run("histogram", func(t *testing.T) {
		hr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_h": hr}
		v, ok := aggs.Histogram("my_h")
		assert.True(t, ok)
		_ = v
	})

	t.Run("histogram_nil", func(t *testing.T) {
		aggs := Aggregations{"my_h": null}
		v, ok := aggs.Histogram("my_h")
		assert.True(t, ok)
		_ = v
	})

	t.Run("auto_date_histogram", func(t *testing.T) {
		adhr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_adh": adhr}
		v, ok := aggs.AutoDateHistogram("my_adh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("auto_date_histogram_nil", func(t *testing.T) {
		aggs := Aggregations{"my_adh": null}
		v, ok := aggs.AutoDateHistogram("my_adh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("date_histogram", func(t *testing.T) {
		dhr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_dh": dhr}
		v, ok := aggs.DateHistogram("my_dh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("date_histogram_nil", func(t *testing.T) {
		aggs := Aggregations{"my_dh": null}
		v, ok := aggs.DateHistogram("my_dh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("keyed_date_histogram", func(t *testing.T) {
		kdhr := json.RawMessage(`{"buckets":{}}`)
		aggs := Aggregations{"my_kdh": kdhr}
		v, ok := aggs.KeyedDateHistogram("my_kdh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("keyed_date_histogram_nil", func(t *testing.T) {
		aggs := Aggregations{"my_kdh": null}
		v, ok := aggs.KeyedDateHistogram("my_kdh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_bounds", func(t *testing.T) {
		gbr := json.RawMessage(`{"bounds":{"top_left":{"lat":90,"lon":-180},"bottom_right":{"lat":-90,"lon":180}}}`)
		aggs := Aggregations{"my_gb": gbr}
		v, ok := aggs.GeoBounds("my_gb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_bounds_nil", func(t *testing.T) {
		aggs := Aggregations{"my_gb": null}
		v, ok := aggs.GeoBounds("my_gb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_hash", func(t *testing.T) {
		ghr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_gh": ghr}
		v, ok := aggs.GeoHash("my_gh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_hash_nil", func(t *testing.T) {
		aggs := Aggregations{"my_gh": null}
		v, ok := aggs.GeoHash("my_gh")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_tile", func(t *testing.T) {
		gtr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_gt": gtr}
		v, ok := aggs.GeoTile("my_gt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_tile_nil", func(t *testing.T) {
		aggs := Aggregations{"my_gt": null}
		v, ok := aggs.GeoTile("my_gt")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_centroid", func(t *testing.T) {
		gcr := json.RawMessage(`{"location":{"lat":0,"lon":0},"count":10}`)
		aggs := Aggregations{"my_gc": gcr}
		v, ok := aggs.GeoCentroid("my_gc")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_centroid_nil", func(t *testing.T) {
		aggs := Aggregations{"my_gc": null}
		v, ok := aggs.GeoCentroid("my_gc")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_distance", func(t *testing.T) {
		gdr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_gd": gdr}
		v, ok := aggs.GeoDistance("my_gd")
		assert.True(t, ok)
		_ = v
	})

	t.Run("geo_distance_nil", func(t *testing.T) {
		aggs := Aggregations{"my_gd": null}
		v, ok := aggs.GeoDistance("my_gd")
		assert.True(t, ok)
		_ = v
	})

	t.Run("avg_bucket", func(t *testing.T) {
		abr := json.RawMessage(`{"value":1.5}`)
		aggs := Aggregations{"my_ab": abr}
		v, ok := aggs.AvgBucket("my_ab")
		assert.True(t, ok)
		_ = v
	})

	t.Run("avg_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_ab": null}
		v, ok := aggs.AvgBucket("my_ab")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sum_bucket", func(t *testing.T) {
		sbr := json.RawMessage(`{"value":100.0}`)
		aggs := Aggregations{"my_sb": sbr}
		v, ok := aggs.SumBucket("my_sb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("sum_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_sb": null}
		v, ok := aggs.SumBucket("my_sb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("stats_bucket", func(t *testing.T) {
		stbr := json.RawMessage(`{"count":10,"min":1.0,"max":100.0,"avg":50.5,"sum":505.0}`)
		aggs := Aggregations{"my_stb": stbr}
		v, ok := aggs.StatsBucket("my_stb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("stats_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_stb": null}
		v, ok := aggs.StatsBucket("my_stb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentiles_bucket", func(t *testing.T) {
		pbr := json.RawMessage(`{"values":{"25.0":1.0}}`)
		aggs := Aggregations{"my_pb": pbr}
		v, ok := aggs.PercentilesBucket("my_pb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("percentiles_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_pb": null}
		v, ok := aggs.PercentilesBucket("my_pb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("max_bucket", func(t *testing.T) {
		mbr := json.RawMessage(`{"keys":["a"],"value":10.0}`)
		aggs := Aggregations{"my_mb": mbr}
		v, ok := aggs.MaxBucket("my_mb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("max_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_mb": null}
		v, ok := aggs.MaxBucket("my_mb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("min_bucket", func(t *testing.T) {
		mnbr := json.RawMessage(`{"keys":["a"],"value":1.0}`)
		aggs := Aggregations{"my_mnb": mnbr}
		v, ok := aggs.MinBucket("my_mnb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("min_bucket_nil", func(t *testing.T) {
		aggs := Aggregations{"my_mnb": null}
		v, ok := aggs.MinBucket("my_mnb")
		assert.True(t, ok)
		_ = v
	})

	t.Run("mov_avg", func(t *testing.T) {
		mar := json.RawMessage(`{"value":1.5}`)
		aggs := Aggregations{"my_ma": mar}
		v, ok := aggs.MovAvg("my_ma")
		assert.True(t, ok)
		_ = v
	})

	t.Run("mov_avg_nil", func(t *testing.T) {
		aggs := Aggregations{"my_ma": null}
		v, ok := aggs.MovAvg("my_ma")
		assert.True(t, ok)
		_ = v
	})

	t.Run("mov_fn", func(t *testing.T) {
		mfr := json.RawMessage(`{"value":1.5}`)
		aggs := Aggregations{"my_mf": mfr}
		v, ok := aggs.MovFn("my_mf")
		assert.True(t, ok)
		_ = v
	})

	t.Run("mov_fn_nil", func(t *testing.T) {
		aggs := Aggregations{"my_mf": null}
		v, ok := aggs.MovFn("my_mf")
		assert.True(t, ok)
		_ = v
	})

	t.Run("derivative", func(t *testing.T) {
		dr := json.RawMessage(`{"value":1.0}`)
		aggs := Aggregations{"my_d": dr}
		v, ok := aggs.Derivative("my_d")
		assert.True(t, ok)
		_ = v
	})

	t.Run("derivative_nil", func(t *testing.T) {
		aggs := Aggregations{"my_d": null}
		v, ok := aggs.Derivative("my_d")
		assert.True(t, ok)
		_ = v
	})

	t.Run("cumulative_sum", func(t *testing.T) {
		csr := json.RawMessage(`{"value":10.0}`)
		aggs := Aggregations{"my_cs": csr}
		v, ok := aggs.CumulativeSum("my_cs")
		assert.True(t, ok)
		_ = v
	})

	t.Run("cumulative_sum_nil", func(t *testing.T) {
		aggs := Aggregations{"my_cs": null}
		v, ok := aggs.CumulativeSum("my_cs")
		assert.True(t, ok)
		_ = v
	})

	t.Run("bucket_script", func(t *testing.T) {
		bsr := json.RawMessage(`{"value":5.0}`)
		aggs := Aggregations{"my_bs": bsr}
		v, ok := aggs.BucketScript("my_bs")
		assert.True(t, ok)
		_ = v
	})

	t.Run("bucket_script_nil", func(t *testing.T) {
		aggs := Aggregations{"my_bs": null}
		v, ok := aggs.BucketScript("my_bs")
		assert.True(t, ok)
		_ = v
	})

	t.Run("serial_diff", func(t *testing.T) {
		sdr := json.RawMessage(`{"value":2.0}`)
		aggs := Aggregations{"my_sd": sdr}
		v, ok := aggs.SerialDiff("my_sd")
		assert.True(t, ok)
		_ = v
	})

	t.Run("serial_diff_nil", func(t *testing.T) {
		aggs := Aggregations{"my_sd": null}
		v, ok := aggs.SerialDiff("my_sd")
		assert.True(t, ok)
		_ = v
	})

	t.Run("composite", func(t *testing.T) {
		cr := json.RawMessage(`{"buckets":[]}`)
		aggs := Aggregations{"my_c": cr}
		v, ok := aggs.Composite("my_c")
		assert.True(t, ok)
		_ = v
	})

	t.Run("composite_nil", func(t *testing.T) {
		aggs := Aggregations{"my_c": null}
		v, ok := aggs.Composite("my_c")
		assert.True(t, ok)
		_ = v
	})

	t.Run("scripted_metric", func(t *testing.T) {
		smr := json.RawMessage(`{"value":100}`)
		aggs := Aggregations{"my_sm": smr}
		v, ok := aggs.ScriptedMetric("my_sm")
		assert.True(t, ok)
		_ = v
	})

	t.Run("scripted_metric_nil", func(t *testing.T) {
		aggs := Aggregations{"my_sm": null}
		v, ok := aggs.ScriptedMetric("my_sm")
		assert.True(t, ok)
		_ = v
	})

	t.Run("top_metrics", func(t *testing.T) {
		tmr := json.RawMessage(`{"top":[]}`)
		aggs := Aggregations{"my_tm": tmr}
		v, ok := aggs.TopMetrics("my_tm")
		assert.True(t, ok)
		_ = v
	})

	t.Run("top_metrics_nil", func(t *testing.T) {
		aggs := Aggregations{"my_tm": null}
		v, ok := aggs.TopMetrics("my_tm")
		assert.True(t, ok)
		_ = v
	})

	t.Run("marshal_errors", func(t *testing.T) {
		aggs := Aggregations{"bad": bad}
		_, ok := aggs.Min("bad")
		assert.False(t, ok)
		_, ok = aggs.Max("bad")
		assert.False(t, ok)
		_, ok = aggs.Sum("bad")
		assert.False(t, ok)
		_, ok = aggs.Avg("bad")
		assert.False(t, ok)
		_, ok = aggs.Stats("bad")
		assert.False(t, ok)
		_, ok = aggs.ExtendedStats("bad")
		assert.False(t, ok)
		_, ok = aggs.Terms("bad")
		assert.False(t, ok)
		_, ok = aggs.Range("bad")
		assert.False(t, ok)
	})
}

func ptr[T any](v T) *T       { return &v }
func ptrF(v float64) *float64 { return &v }
