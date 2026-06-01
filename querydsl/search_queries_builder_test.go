// search_queries_builder_test.go tests the Source() method of every query
// type in the querydsl package. Each test constructs a query with the
// constructor plus optional pointer fields, calls Source(), and verifies
// the returned map[string]any matches the expected OpenSearch JSON DSL.
//
// Tests are organized by query family:
//   - Leaf queries: HasChild, HasParent, ParentId, Percolator, etc.
//   - Full-text queries: Match, MatchPhrase, MatchBoolPrefix, etc.
//   - Term-level queries: Term, Terms, Range, Wildcard, Regexp, etc.
//   - Compound queries: Interval, FunctionScore (FSQ score functions)
package querydsl

import (
	"testing"

	json "github.com/goccy/go-json"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasChildQuery_Coverage(t *testing.T) {
	boost := 2.0
	mc := 1
	xc := 10
	scc := 5
	q := NewHasChildQuery("answer", NewMatchAllQuery())
	q.Boost = &boost
	q.ScoreMode = "max"
	q.MinChildren = &mc
	q.MaxChildren = &xc
	q.ShortCircuitCutoff = &scc
	q.QueryName = "hc"
	q.InnerHit = NewInnerHit().Name("inner")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["has_child"].(map[string]any)
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "max", m["score_mode"])
	assert.Equal(t, 1, m["min_children"])
	assert.Equal(t, 10, m["max_children"])
	assert.Equal(t, 5, m["short_circuit_cutoff"])
	assert.Equal(t, "hc", m["_name"])
	assert.Contains(t, m, "inner_hits")
}

func TestHasParentQuery_Coverage(t *testing.T) {
	boost := 1.5
	score := true
	iu := true
	q := NewHasParentQuery("question", NewMatchAllQuery())
	q.Boost = &boost
	q.Score = &score
	q.QueryName = "hp"
	q.InnerHit = NewInnerHit().Name("inner")
	q.IgnoreUnmapped = &iu
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["has_parent"].(map[string]any)
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, true, m["score"])
	assert.Equal(t, "hp", m["_name"])
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, true, m["ignore_unmapped"])
}

func TestParentIdQuery_Coverage(t *testing.T) {
	q := NewParentIdQuery("answer", "1")
	q.InnerHit = NewInnerHit().Name("inner")
	ignoreUnmapped := true
	q.IgnoreUnmapped = &ignoreUnmapped
	boost := 2.0
	q.Boost = &boost
	q.QueryName = "pid"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["parent_id"].(map[string]any)
	assert.Equal(t, "answer", m["type"])
	assert.Equal(t, "1", m["id"])
	assert.Contains(t, m, "inner_hits")
}

func TestPercolatorQuery_Coverage(t *testing.T) {
	t.Run("single document", func(t *testing.T) {
		q := NewPercolatorQuery()
		q.Field = "query"
		q.DocumentType = "_doc"
		q.Name = "pick-up-percolator"
		q.Documents = []any{map[string]any{"message": "A new bonsai tree in the office"}}
		q.IndexedDocumentIndex = "my-index"
		q.IndexedDocumentType = "my-type"
		q.IndexedDocumentID = "1"
		q.IndexedDocumentRouting = "routing1"
		q.IndexedDocumentPreference = "_local"
		ver := int64(1)
		q.IndexedDocumentVersion = &ver
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percolate"].(map[string]any)
		assert.Equal(t, "query", m["field"])
		assert.Equal(t, "_doc", m["document_type"])
		assert.Equal(t, "pick-up-percolator", m["name"])
		assert.Contains(t, m, "document")
		assert.Equal(t, "my-index", m["index"])
		assert.Equal(t, "my-type", m["type"])
		assert.Equal(t, "1", m["id"])
		assert.Equal(t, "routing1", m["routing"])
		assert.Equal(t, "_local", m["preference"])
		assert.Equal(t, int64(1), m["version"])
	})

	t.Run("multiple documents", func(t *testing.T) {
		q := NewPercolatorQuery()
		q.Field = "query"
		q.Documents = []any{
			map[string]any{"message": "doc1"},
			map[string]any{"message": "doc2"},
		}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percolate"].(map[string]any)
		assert.Contains(t, m, "documents")
	})

	t.Run("no documents", func(t *testing.T) {
		q := NewPercolatorQuery()
		q.Field = "query"
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percolate"].(map[string]any)
		assert.Equal(t, "query", m["field"])
	})
}

func TestRankFeatureQuery_Coverage(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		boost := 2.0
		q := NewRankFeatureQuery("pagerank")
		q.ScoreFunction = NewRankFeatureLogScoreFunction(8.0)
		q.Boost = &boost
		q.QueryName = "rf"
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rank_feature"].(map[string]any)
		assert.Equal(t, "pagerank", m["field"])
		assert.Equal(t, 2.0, m["boost"])
		assert.Equal(t, "rf", m["_name"])
		assert.Contains(t, m, "log")
	})

	t.Run("saturation", func(t *testing.T) {
		pivot := 7.0
		q := NewRankFeatureQuery("url_length")
		q.ScoreFunction = RankFeatureSaturationScoreFunction{Pivot: &pivot}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rank_feature"].(map[string]any)
		assert.Contains(t, m, "saturation")
	})

	t.Run("saturation_no_pivot", func(t *testing.T) {
		q := NewRankFeatureQuery("url_length")
		q.ScoreFunction = NewRankFeatureSaturationScoreFunction()
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rank_feature"].(map[string]any)
		assert.Contains(t, m, "saturation")
	})

	t.Run("sigmoid", func(t *testing.T) {
		q := NewRankFeatureQuery("pagerank")
		q.ScoreFunction = NewRankFeatureSigmoidScoreFunction(7.0, 1.0)
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rank_feature"].(map[string]any)
		assert.Contains(t, m, "sigmoid")
	})

	t.Run("linear", func(t *testing.T) {
		q := NewRankFeatureQuery("foot_traffic")
		q.ScoreFunction = NewRankFeatureLinearScoreFunction()
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rank_feature"].(map[string]any)
		assert.Contains(t, m, "linear")
	})
}

func TestRegexpQuery_Coverage(t *testing.T) {
	boost := 2.0
	ci := true
	maxDet := 10000
	q := NewRegexpQuery("name.first", "s.*y")
	q.Flags = "INTERSECTION"
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.CaseInsensitive = &ci
	q.MaxDeterminizedStates = &maxDet
	q.QueryName = "rq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["regexp"].(map[string]any)
	inner := m["name.first"].(map[string]any)
	assert.Equal(t, "s.*y", inner["value"])
}

func TestScriptQuery_Coverage(t *testing.T) {
	q := NewScriptQuery(NewScriptInline("doc['price'].value > 10"))
	q.QueryName = "sq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["script"].(map[string]any)
	assert.Contains(t, m, "script")
	assert.Equal(t, "sq", m["_name"])
}

func TestScriptScoreQuery_Coverage(t *testing.T) {
	ms := 0.5
	boost := 1.5
	q := NewScriptScoreQuery(NewMatchAllQuery(), NewScriptInline("_score * doc['popularity'].value"))
	q.MinScore = &ms
	q.Boost = &boost
	q.QueryName = "ssq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["script_score"].(map[string]any)
	assert.Contains(t, m, "query")
	assert.Contains(t, m, "script")
	assert.Equal(t, 0.5, m["min_score"])
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "ssq", m["_name"])
}

func TestSpanFirstQuery_Coverage(t *testing.T) {
	boost := 2.0
	q := NewSpanFirstQuery(NewSpanTermQuery("user", "kimchy"), 3)
	q.Boost = &boost
	q.QueryName = "sf"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_first"].(map[string]any)
	assert.Equal(t, 3, m["end"])
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "sf", m["query_name"])
	assert.Contains(t, m, "match")
}

func TestSpanNearQuery_Coverage(t *testing.T) {
	slop := 5
	inOrder := true
	boost := 2.0
	q := NewSpanNearQuery(
		NewSpanTermQuery("field", "quick"),
		NewSpanTermQuery("field", "fox"),
	)
	q.Slop = &slop
	q.InOrder = &inOrder
	q.Boost = &boost
	q.QueryName = "sn"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_near"].(map[string]any)
	assert.Equal(t, 5, m["slop"])
	assert.Equal(t, true, m["in_order"])
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "sn", m["query_name"])
	assert.Contains(t, m, "clauses")
}

func TestTermsSetQuery_Coverage(t *testing.T) {
	boost := 1.5
	q := NewTermsSetQuery("programming_languages", "c++", "java", "php")
	q.MinimumShouldMatchField = "required_matches"
	q.MinimumShouldMatchScript = NewScriptInline("Math.min(params.num_terms, 2)")
	q.Boost = &boost
	q.QueryName = "tsq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms_set"].(map[string]any)
	inner := m["programming_languages"].(map[string]any)
	assert.Contains(t, inner, "terms")
	assert.Equal(t, "required_matches", inner["minimum_should_match_field"])
	assert.Contains(t, inner, "minimum_should_match_script")
	assert.Equal(t, 1.5, inner["boost"])
	assert.Equal(t, "tsq", inner["_name"])
}

func TestFuzzyQuery_Coverage(t *testing.T) {
	boost := 2.0
	pl := 1
	me := 50
	trans := true
	q := NewFuzzyQuery("user", "ki")
	q.Boost = &boost
	q.Fuzziness = "AUTO"
	q.PrefixLength = &pl
	q.MaxExpansions = &me
	q.Transpositions = &trans
	q.Rewrite = "constant_score"
	q.QueryName = "fq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["fuzzy"].(map[string]any)
	inner := m["user"].(map[string]any)
	assert.Equal(t, "ki", inner["value"])
}

func TestIdsQuery_Coverage(t *testing.T) {
	boost := 1.5
	q := NewIdsQuery("1", "2")
	q.Boost = &boost
	q.QueryName = "ids_q"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["ids"].(map[string]any)
	assert.Equal(t, 1.5, m["boost"])
	assert.Equal(t, "ids_q", m["_name"])
}

func TestMatchQuery_Coverage(t *testing.T) {
	pl := 2
	me := 50
	cutoff := 0.001
	boost := 1.5
	trans := true
	lenient := true
	q := NewMatchQuery("message", "this is a test")
	q.Analyzer = "standard"
	q.Operator = "and"
	q.Fuzziness = "AUTO"
	q.PrefixLength = &pl
	q.MaxExpansions = &me
	q.MinimumShouldMatch = "2"
	q.FuzzyRewrite = "constant_score"
	q.FuzzyTranspositions = &trans
	q.Lenient = &lenient
	q.ZeroTermsQuery = "all"
	q.CutoffFrequency = &cutoff
	q.Boost = &boost
	q.QueryName = "mq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match"].(map[string]any)
	assert.Contains(t, m, "message")
}

func TestMatchAllQuery_Coverage(t *testing.T) {
	boost := 1.2
	q := NewMatchAllQuery()
	q.Boost = &boost
	q.QueryName = "ma"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_all"].(map[string]any)
	assert.Equal(t, 1.2, m["boost"])
	assert.Equal(t, "ma", m["_name"])
}

func TestMatchNoneQuery_Coverage(t *testing.T) {
	q := NewMatchNoneQuery()
	q.QueryName = "mn"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_none"].(map[string]any)
	assert.Equal(t, "mn", m["_name"])
}

func TestMatchPhraseQuery_Coverage(t *testing.T) {
	slop := 2
	boost := 1.5
	q := NewMatchPhraseQuery("message", "this is a test")
	q.Analyzer = "my_analyzer"
	q.Slop = &slop
	q.Boost = &boost
	q.QueryName = "mp"
	q.ZeroTermsQuery = "none"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_phrase"].(map[string]any)
	inner := m["message"].(map[string]any)
	assert.Equal(t, "my_analyzer", inner["Analyzer"])
}

func TestMatchPhrasePrefixQuery_Coverage(t *testing.T) {
	slop := 1
	me := 10
	boost := 2.0
	q := NewMatchPhrasePrefixQuery("message", "quick brown f")
	q.Analyzer = "standard"
	q.Slop = &slop
	q.MaxExpansions = &me
	q.Boost = &boost
	q.QueryName = "mpp"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_phrase_prefix"].(map[string]any)
	assert.Contains(t, m, "message")
}

func TestMatchBoolPrefixQuery_Coverage(t *testing.T) {
	pl := 2
	me := 50
	boost := 1.5
	trans := true
	q := NewMatchBoolPrefixQuery("message", "quick bro")
	q.Analyzer = "standard"
	q.MinimumShouldMatch = "1"
	q.Operator = "or"
	q.Fuzziness = "AUTO"
	q.PrefixLength = &pl
	q.MaxExpansions = &me
	q.FuzzyTranspositions = &trans
	q.FuzzyRewrite = "constant_score"
	q.Boost = &boost
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match_bool_prefix"].(map[string]any)
	assert.Contains(t, m, "message")
}

func TestNestedQuery_Coverage(t *testing.T) {
	boost := 2.0
	iu := true
	q := NewNestedQuery("obj", NewMatchAllQuery())
	q.ScoreMode = "avg"
	q.Boost = &boost
	q.QueryName = "nq"
	q.InnerHit = NewInnerHit().Name("inner")
	q.IgnoreUnmapped = &iu
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["nested"].(map[string]any)
	assert.Equal(t, "avg", m["score_mode"])
	assert.Equal(t, 2.0, m["boost"])
	assert.Equal(t, "nq", m["_name"])
	assert.Contains(t, m, "inner_hits")
	assert.Equal(t, true, m["ignore_unmapped"])
}

func TestTermQuery_Coverage(t *testing.T) {
	boost := 1.5
	ci := true
	q := NewTermQuery("status", "published")
	q.Boost = &boost
	q.CaseInsensitive = &ci
	q.QueryName = "tq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["term"].(map[string]any)
	inner := m["status"].(map[string]any)
	assert.Equal(t, 1.5, inner["boost"])
	assert.Equal(t, true, inner["case_insensitive"])
	assert.Equal(t, "tq", inner["_name"])
}

func TestTermsQuery_Coverage(t *testing.T) {
	t.Run("with terms lookup", func(t *testing.T) {
		q := NewTermsQuery("status")
		q.TermsLookup = NewTermsLookup().Index("lookup-idx").Id("1").Path("followers")
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["terms"].(map[string]any)
		assert.Contains(t, m, "status")
	})
}

func TestTypeQuery_Coverage(t *testing.T) {
	q := NewTypeQuery("_doc")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["type"].(map[string]any)
	assert.Equal(t, "_doc", m["value"])
}

func TestWildcardQuery_Coverage(t *testing.T) {
	boost := 2.0
	ci := true
	q := NewWildcardQuery("name", "ki*y")
	q.Boost = &boost
	q.Rewrite = "constant_score"
	q.QueryName = "wq"
	q.CaseInsensitive = &ci
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["wildcard"].(map[string]any)
	inner := m["name"].(map[string]any)
	assert.Equal(t, "ki*y", inner["value"])
}

func TestSpanTermQuery_Coverage(t *testing.T) {
	boost := 2.0
	q := NewSpanTermQuery("user", "kimchy")
	q.Boost = &boost
	q.QueryName = "st"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["span_term"].(map[string]any)
	inner := m["user"].(map[string]any)
	assert.Equal(t, "kimchy", inner["value"])
}

func TestRangeQuery_Coverage(t *testing.T) {
	boost := 1.5
	q := NewRangeQuery("timestamp").Gte("2020-01-01").Lte("2020-12-31")
	q.TimeZone = "+01:00"
	q.Format = "yyyy-MM-dd"
	q.Relation = "within"
	q.Boost = &boost
	q.QueryName = "rq"
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["range"].(map[string]any)
	ts := m["timestamp"].(map[string]any)
	assert.Equal(t, "+01:00", ts["time_zone"])
	assert.Equal(t, "yyyy-MM-dd", ts["format"])
	assert.Equal(t, "within", ts["relation"])
	assert.Equal(t, 1.5, ts["boost"])
	assert.Equal(t, "rq", m["_name"])
}

func TestQueryStringQuery_Coverage(t *testing.T) {
	allow := true
	lower := false
	posInc := true
	analyzeWild := true
	lenient := false
	escape := false
	boost := 1.5
	fp := 2
	fme := 50
	ps := 3
	maxDet := 10000
	tie := 0.3
	b := NewQueryStringQuery("(new york city) OR (big apple)")
	b.DefaultField = "content"
	b.Fields = []string{"title", "body"}
	b.FieldBoosts = map[string]*float64{"title": &boost}
	b.TieBreaker = &tie
	b.DefaultOperator = "AND"
	b.Analyzer = "standard"
	b.QuoteAnalyzer = "whitespace"
	b.MaxDeterminizedStates = &maxDet
	b.AllowLeadingWildcard = &allow
	b.LowercaseExpandedTerms = &lower
	b.EnablePositionIncrements = &posInc
	b.Fuzziness = "AUTO"
	b.Boost = &boost
	b.FuzzyPrefixLength = &fp
	b.FuzzyMaxExpansions = &fme
	b.FuzzyRewrite = "constant_score"
	b.PhraseSlop = &ps
	b.AnalyzeWildcard = &analyzeWild
	b.Rewrite = "scoring_boolean"
	b.MinimumShouldMatch = "1"
	b.QuoteFieldSuffix = ".exact"
	b.Lenient = &lenient
	b.QueryName = "qs"
	b.Locale = "en"
	b.TimeZone = "UTC"
	b.Escape = &escape
	b.Type = "best_fields"
	src, err := b.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["query_string"].(map[string]any)
	assert.Equal(t, "content", m["default_field"])
	assert.Contains(t, m, "fields")
	assert.Equal(t, 0.3, m["tie_breaker"])
	assert.Equal(t, "AND", m["default_operator"])
}

func TestSimpleQueryStringQuery_Coverage(t *testing.T) {
	lower := true
	lenient := true
	analyzeWild := true
	autoGen := true
	trans := true
	boost := 2.0
	q := NewSimpleQueryStringQuery(`"fried eggs" +(eggplant | potato) -frittata`)
	q.Fields = []string{"title", "body"}
	q.FieldBoosts = map[string]*float64{"title": &boost}
	q.Flags = "ALL"
	q.Analyzer = "simple"
	q.DefaultOperator = "OR"
	q.LowercaseExpandedTerms = &lower
	q.Lenient = &lenient
	q.AnalyzeWildcard = &analyzeWild
	q.Locale = "en"
	q.QueryName = "sqs"
	q.MinimumShouldMatch = "1"
	q.QuoteFieldSuffix = ".exact"
	q.Boost = &boost
	q.AutoGenerateSynonymsPhraseQuery = &autoGen
	q.FuzzyPrefixLength = 2
	q.FuzzyMaxExpansions = 50
	q.FuzzyTranspositions = &trans
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["simple_query_string"].(map[string]any)
	assert.Contains(t, m, "fields")
	assert.Equal(t, "ALL", m["flags"])
	assert.Equal(t, "or", m["default_operator"])
}

func TestGeoBoundingBoxQuery_Coverage(t *testing.T) {
	t.Run("wkt", func(t *testing.T) {
		q := NewGeoBoundingBoxQuery("pin.location")
		q.WKT = "BBOX (-74.1, -71.12, 40.73, 40.10)"
		q.Type = "indexed"
		q.ValidationMethod = "STRICT"
		ignoreUnmapped := true
		q.IgnoreUnmapped = &ignoreUnmapped
		q.QueryName = "gbb"
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["geo_bounding_box"].(map[string]any)
		locationBox := m["pin.location"].(map[string]any)
		assert.Contains(t, locationBox, "wkt")
		assert.Equal(t, "indexed", m["type"])
		assert.Equal(t, "STRICT", m["validation_method"])
		assert.Equal(t, true, m["ignore_unmapped"])
		assert.Equal(t, "gbb", m["_name"])
	})

	t.Run("top_right_bottom_left", func(t *testing.T) {
		q := NewGeoBoundingBoxQuery("pin.location")
		q.TopRight = []float64{-71.12, 40.73}
		q.BottomLeft = []float64{-74.1, 40.01}
		src, err := q.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["geo_bounding_box"].(map[string]any)
		locationBox := m["pin.location"].(map[string]any)
		assert.Contains(t, locationBox, "top_right")
		assert.Contains(t, locationBox, "bottom_left")
	})
}

func TestIntervalQueryFilter_Coverage(t *testing.T) {
	m := NewIntervalQueryRuleMatch("hello")

	t.Run("all filters", func(t *testing.T) {
		f := NewIntervalQueryFilter().
			After(m).Before(m).ContainedBy(m).Containing(m).
			Overlapping(m).NotContainedBy(m).NotContaining(m).NotOverlapping(m).
			Script(NewScriptInline("return true"))
		src, err := f.Source()
		require.NoError(t, err)
		sm := src.(map[string]any)
		assert.Len(t, sm, 9)
	})
}

func TestIntervalQueryRuleMatch_Coverage(t *testing.T) {
	r := NewIntervalQueryRuleMatch("hello world")
	r.MaxGaps(0).Ordered(false).Analyzer("english").UseField("body.desc")
	r.Filter(NewIntervalQueryFilter().After(NewIntervalQueryRuleMatch("world")))
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["match"].(map[string]any)
	assert.Equal(t, "hello world", m["query"])
	assert.Equal(t, 0, m["max_gaps"])
	assert.Equal(t, false, m["ordered"])
}

func TestIntervalQueryRuleAllOf_Coverage(t *testing.T) {
	f := NewIntervalQueryFilter().Before(NewIntervalQueryRuleMatch("world"))
	r := NewIntervalQueryRuleAllOf(
		NewIntervalQueryRuleMatch("quick"),
		NewIntervalQueryRuleMatch("fox"),
	).MaxGaps(10).Ordered(true).Filter(f)
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["all_of"].(map[string]any)
	assert.Contains(t, m, "intervals")
	assert.Contains(t, m, "ordered")
}

func TestIntervalQueryRuleAnyOf_Coverage(t *testing.T) {
	f := NewIntervalQueryFilter().Containing(NewIntervalQueryRuleMatch("x"))
	r := NewIntervalQueryRuleAnyOf(
		NewIntervalQueryRuleMatch("quick"),
	).Filter(f)
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["any_of"].(map[string]any)
	assert.Contains(t, m, "filter")
}

func TestIntervalQueryRuleFuzzy_Coverage(t *testing.T) {
	r := NewIntervalQueryRuleFuzzy("helo")
	r.PrefixLength(1).Fuzziness(2).Transpositions(false).Analyzer("standard").UseField("title")
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["fuzzy"].(map[string]any)
	assert.Equal(t, "helo", m["term"])
	assert.Equal(t, 1, m["prefix_length"])
}

func TestIntervalQueryRulePrefix_Coverage(t *testing.T) {
	r := NewIntervalQueryRulePrefix("hel")
	r.Analyzer("standard").UseField("title")
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["prefix"].(map[string]any)
	assert.Equal(t, "hel", m["prefix"])
}

func TestIntervalQueryRuleWildcard_Coverage(t *testing.T) {
	r := NewIntervalQueryRuleWildcard("he*o")
	r.Analyzer("standard").UseField("title")
	assert.True(t, r.isIntervalQueryRule())
	src, err := r.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["wildcard"].(map[string]any)
	assert.Equal(t, "he*o", m["pattern"])
}

func TestFSQScoreFunctions_Coverage(t *testing.T) {
	t.Run("exponential_decay_weight", func(t *testing.T) {
		fn := NewExponentialDecayFunction()
		fn.Weight(3.0)
		w := fn.GetWeight()
		require.NotNil(t, w)
		assert.Equal(t, 3.0, *w)
	})

	t.Run("gauss_decay_no_optional", func(t *testing.T) {
		fn := NewGaussDecayFunction()
		fn.FieldName("date").Scale("10d")
		assert.Equal(t, "gauss", fn.Name())
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "date")
	})

	t.Run("linear_decay_nil_weight", func(t *testing.T) {
		fn := NewLinearDecayFunction()
		assert.Nil(t, fn.GetWeight())
	})

	t.Run("script_function_set_script", func(t *testing.T) {
		fn := NewScriptFunction(nil)
		fn.Script(NewScriptInline("_score * 3"))
		assert.Equal(t, "script_score", fn.Name())
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "script")
	})

	t.Run("field_value_factor_no_optional", func(t *testing.T) {
		fn := NewFieldValueFactorFunction()
		fn.Field("likes")
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "likes", m["field"])
	})

	t.Run("weight_factor_weight_change", func(t *testing.T) {
		fn := NewWeightFactorFunction(1.0)
		fn.Weight(5.0)
		w := fn.GetWeight()
		assert.Equal(t, 5.0, *w)
		src, err := fn.Source()
		require.NoError(t, err)
		assert.Equal(t, 5.0, src)
	})

	t.Run("random_function_no_optional", func(t *testing.T) {
		fn := NewRandomFunction()
		assert.Nil(t, fn.GetWeight())
		src, err := fn.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.NotNil(t, m)
	})
}

func TestSearchRequest_Body(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		req := NewSearchRequest()
		body, err := req.Body()
		require.NoError(t, err)
		assert.NotEmpty(t, body)
	})

	t.Run("search_source", func(t *testing.T) {
		ss := NewSearchSource().Size(5)
		req := NewSearchRequest()
		req.Source(ss)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})

	t.Run("json_raw_message", func(t *testing.T) {
		raw := json.RawMessage(`{"query":{"match_all":{}}}`)
		req := NewSearchRequest()
		req.Source(raw)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})

	t.Run("json_raw_message_ptr", func(t *testing.T) {
		raw := json.RawMessage(`{"size":10}`)
		req := NewSearchRequest()
		req.Source(&raw)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "size")
	})

	t.Run("string", func(t *testing.T) {
		s := `{"query":{"match_all":{}}}`
		req := NewSearchRequest()
		req.Source(s)
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})

	t.Run("string_ptr", func(t *testing.T) {
		s := `{"query":{"match_all":{}}}`
		req := NewSearchRequest()
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
		req.Source(map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
		body, err := req.Body()
		require.NoError(t, err)
		assert.Contains(t, body, "match_all")
	})
}

func TestSearchRequest_sourceAsMap(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		req := NewSearchRequest()
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("search_source", func(t *testing.T) {
		ss := NewSearchSource().Size(5)
		req := NewSearchRequest()
		req.Source(ss)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("json_raw_message", func(t *testing.T) {
		raw := json.RawMessage(`{"size":10}`)
		req := NewSearchRequest()
		req.Source(raw)
		src, err := req.sourceAsMap()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("json_raw_message_ptr", func(t *testing.T) {
		raw := json.RawMessage(`{"size":10}`)
		req := NewSearchRequest()
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
		s := `{"size":10}`
		req := NewSearchRequest()
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

func TestSearchRequest_header(t *testing.T) {
	routing := "user_1"
	pref := "_local"
	req := NewSearchRequest()
	req.SearchType("dfs_query_then_fetch")
	req.Index("idx1", "idx2")
	req.Type("type1", "type2")
	req.Routing(routing)
	req.Preference(pref)
	req.RequestCache(true)
	req.IgnoreUnavailable(true)
	req.AllowNoIndices(true)
	req.ExpandWildcards("open,closed")
	req.AllowPartialSearchResults(true)
	req.Scroll("2m")
	h := req.header().(map[string]any)
	assert.Equal(t, "dfs_query_then_fetch", h["search_type"])
	assert.Equal(t, []string{"idx1", "idx2"}, h["indices"])
	assert.Equal(t, []string{"type1", "type2"}, h["types"])
	assert.Equal(t, "user_1", h["routing"])
	assert.Equal(t, "_local", h["preference"])
	assert.Equal(t, true, h["request_cache"])
	assert.Equal(t, true, h["ignore_unavailable"])
	assert.Equal(t, true, h["allow_no_indices"])
	assert.Equal(t, "open,closed", h["expand_wildcards"])
	assert.Equal(t, true, h["allow_partial_search_results"])
	assert.Equal(t, "2m", h["scroll"])
}

func TestSearchRequest_header_single(t *testing.T) {
	req := NewSearchRequest()
	req.Index("single-idx")
	req.Type("single-type")
	req.Routings("r1", "r2")
	h := req.header().(map[string]any)
	assert.Equal(t, "single-idx", h["index"])
	assert.Equal(t, "single-type", h["type"])
	assert.Equal(t, "r1,r2", h["routing"])
}

func TestSearchRequest_header_empty_routing(t *testing.T) {
	req := NewSearchRequest()
	empty := ""
	req.routing = &empty
	h := req.header().(map[string]any)
	_, ok := h["routing"]
	assert.False(t, ok)
}

func TestSearchRequest_header_nil_routings(t *testing.T) {
	req := NewSearchRequest()
	req.Routings()
}

func TestSearchRequest_misc_methods(t *testing.T) {
	req := NewSearchRequest()
	req.SearchTypeDfsQueryThenFetch()
	req.SearchTypeQueryThenFetch()
	assert.True(t, req.HasIndices() == false)
	req.Index("test")
	assert.True(t, req.HasIndices())
}

func TestSearchRequest_more_methods(t *testing.T) {
	req := NewSearchRequest()
	req.Timeout("1s")
	req.TerminateAfter(100)
	req.Query(NewMatchAllQuery())
	req.PostFilter(NewTermQuery("status", "published"))
	req.MinScore(0.5)
	req.From(0)
	req.Size(10)
	req.Explain(true)
	req.Version(true)
	req.IndexBoost("idx", 1.5)
	req.Stats("group1")
	req.FetchSource(true)
	req.FetchSourceIncludeExclude([]string{"a"}, []string{"b"})
	req.FetchSourceContext(NewFetchSourceContext(true))
	req.DocValueField("f1")
	req.DocValueFieldWithFormat(DocvalueField{Field: "f2"})
	req.DocValueFields("f3")
	req.DocValueFieldsWithFormat(DocvalueField{Field: "f4"})
	req.StoredField("sf1")
	req.NoStoredFields()
	req.StoredFields("sf2", "sf3")
	req.ScriptField(NewScriptField("x", NewScriptInline("1")))
	req.ScriptFields(NewScriptField("y", NewScriptInline("2")))
	req.Sort("ts", true)
	req.SortWithInfo(SortInfo{Field: "ts2", Ascending: true})
	req.SortBy(NewFieldSort("ts3"))
	req.SearchAfter(1, "a")
	req.Slice(NewSliceQuery())
	req.TrackScores(true)
	req.TrackTotalHits(true)
	req.Aggregation("terms", NewTermsAggregation().WithField("x"))
	req.Highlight(NewHighlight().Field("title"))
	req.Suggester(NewTermSuggester("s1").Field("title"))
	req.Rescorer(NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery())))
	req.ClearRescorers()
	req.Profile(true)
	req.Collapse(NewCollapseBuilder("user"))
	req.PointInTime(NewPointInTime("abc"))
	req.BatchedReduceSize(30)
	req.MaxConcurrentShardRequests(5)
	req.PreFilterShardSize(128)
	req.SearchSource(nil)
	body, err := req.Body()
	require.NoError(t, err)
	assert.NotEmpty(t, body)
}

func TestSearchSource_hasSort(t *testing.T) {
	ss := NewSearchSource()
	assert.False(t, ss.hasSort())
	ss.SortBy(NewFieldSort("ts"))
	assert.True(t, ss.hasSort())
}

func TestSearchSource_Source_full(t *testing.T) {
	ss := NewSearchSource()
	ss.from = -1
	ss.size = -1
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Empty(t, m)
}

func TestSearchSource_MarshalJSON_nil(t *testing.T) {
	var ss *SearchSource
	data, err := ss.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestScoreSort_Asc_Coverage(t *testing.T) {
	s := NewScoreSort().Asc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_score"].(map[string]any)
	assert.Equal(t, "asc", m["order"])
}

func TestGeoDistanceSort_Desc_Coverage(t *testing.T) {
	s := NewGeoDistanceSort("location").Point(40.0, -74.0).Desc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_geo_distance"].(map[string]any)
	assert.Equal(t, "desc", m["order"])
}

func TestScriptSort_Asc_Coverage(t *testing.T) {
	s := NewScriptSort(NewScriptInline("doc['x'].value"), "number").Asc()
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["_script"].(map[string]any)
	assert.Equal(t, "asc", m["order"])
}

func TestFieldSort_AllFields_Coverage(t *testing.T) {
	s := NewFieldSort("ts")
	s.Desc().Missing("_last").UnmappedType("date").SortMode("max").
		Filter(NewMatchAllQuery()).Path("nested").Nested(NewNestedSort("nested"))
	src, err := s.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["ts"].(map[string]any)
	assert.Equal(t, "desc", m["order"])
	assert.Equal(t, "_last", m["missing"])
	assert.Equal(t, "date", m["unmapped_type"])
	assert.Equal(t, "max", m["mode"])
	assert.Contains(t, m, "filter")
	assert.Equal(t, "nested", m["path"])
	assert.Contains(t, m, "nested")
}

func TestNestedSort_FullCoverage(t *testing.T) {
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

func TestCompletionSuggester_Coverage(t *testing.T) {
	t.Run("with_text", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Text("nir").Field("suggest").Size(5).ShardSize(10).Analyzer("standard")
		s.SkipDuplicates(true)
		src, err := s.Source(true)
		require.NoError(t, err)
		m := src.(map[string]any)["my-suggest"]
		assert.NotNil(t, m)
	})

	t.Run("with_prefix", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Prefix("nir").Field("suggest")
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("with_regex", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Regex("n[ea]r").Field("suggest")
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("single_context_query", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Prefix("nir").Field("suggest")
		s.ContextQuery(NewSuggesterCategoryQuery("genre", "rock"))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("multiple_context_queries", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Prefix("nir").Field("suggest")
		s.ContextQueries(
			NewSuggesterCategoryQuery("genre", "rock"),
			NewSuggesterCategoryQuery("type", "song"),
		)
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("fuzzy_options", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		opts := NewFuzzyCompletionSuggesterOptions().
			EditDistance(2).
			Transpositions(true).
			MinLength(3).
			PrefixLength(1).
			UnicodeAware(false).
			MaxDeterminizedStates(10000)
		s.PrefixWithOptions("nir", opts).Field("suggest")
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("fuzziness_creates_options", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.Fuzziness("AUTO")
		assert.NotNil(t, s.fuzzyOptions)
	})

	t.Run("fuzziness_updates_existing", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.FuzzyOptions(NewFuzzyCompletionSuggesterOptions())
		s.Fuzziness(2)
		assert.NotNil(t, s.fuzzyOptions)
	})

	t.Run("regex_options", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		opts := NewRegexCompletionSuggesterOptions().
			Flags("ALL").
			MaxDeterminizedStates(10000)
		s.RegexWithOptions("n.*", opts).Field("suggest")
		s.RegexOptions(NewRegexCompletionSuggesterOptions())
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("prefix_with_edit_distance", func(t *testing.T) {
		s := NewCompletionSuggester("my-suggest")
		s.PrefixWithEditDistance("nir", 2)
		assert.NotNil(t, s.fuzzyOptions)
	})
}

func TestContextSuggester_Coverage(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := NewContextSuggester("ctx-suggest")
		s.Prefix("nir").Field("suggest").Size(5)
		src, err := s.Source(true)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("single_context", func(t *testing.T) {
		s := NewContextSuggester("ctx-suggest")
		s.Prefix("nir").Field("suggest")
		s.ContextQuery(NewSuggesterCategoryQuery("genre", "rock"))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("multiple_contexts", func(t *testing.T) {
		s := NewContextSuggester("ctx-suggest")
		s.Prefix("nir").Field("suggest")
		s.ContextQueries(
			NewSuggesterCategoryQuery("genre", "rock"),
			NewSuggesterCategoryQuery("type", "song"),
		)
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestPhraseSuggester_Coverage(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("noble prize").
			Field("title.trigram").
			Analyzer("standard").
			Size(1).
			ShardSize(5).
			GramSize(1).
			MaxErrors(1.0).
			Separator("|").
			RealWordErrorLikelihood(0.95).
			Confidence(1.0).
			ForceUnigrams(true).
			TokenLimit(10).
			Highlight("<em>", "</em>").
			CollateQuery(NewScriptInline("return true")).
			CollatePreference("_local").
			CollateParams(map[string]any{"a": 1}).
			CollatePrune(true)
		s.CandidateGenerator(NewDirectCandidateGenerator("title").
			PreFilter("pre").PostFilter("post").SuggestMode("always").
			Accuracy(0.5).Size(5).Sort("score").StringDistance("internal_levenshtein").
			MaxEdits(2).MaxInspections(5).MaxTermFreq(0.01).
			PrefixLength(1).MinWordLength(4).MinDocFreq(0.0))
		s.SmoothingModel(NewStupidBackoffSmoothingModel(0.4))
		src, err := s.Source(true)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("smoothing_laplace", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("test").Field("f")
		s.SmoothingModel(NewLaplaceSmoothingModel(0.7))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("smoothing_linear_interpolation", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("test").Field("f")
		s.SmoothingModel(NewLinearInterpolationSmoothingModel(0.1, 0.2, 0.7))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("with_context", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("test").Field("f")
		s.ContextQuery(NewSuggesterCategoryQuery("genre", "rock"))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("with_multiple_contexts", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("test").Field("f")
		s.ContextQueries(
			NewSuggesterCategoryQuery("genre"),
			NewSuggesterCategoryQuery("type"),
		)
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("clear_generator", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.CandidateGenerator(NewDirectCandidateGenerator("f"))
		s.ClearCandidateGenerator()
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("gram_size_zero_ignored", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.GramSize(0)
		assert.Nil(t, s.gramSize)
	})

	t.Run("multiple_generators", func(t *testing.T) {
		s := NewPhraseSuggester("my-suggest")
		s.Text("test").Field("f")
		s.CandidateGenerators(
			NewDirectCandidateGenerator("f1"),
			NewDirectCandidateGenerator("f2"),
		)
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestTermSuggester_Coverage(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		s := NewTermSuggester("my-suggest")
		s.Text("openseerch").
			Field("title").
			Analyzer("standard").
			Size(3).
			ShardSize(10).
			SuggestMode("popular").
			Accuracy(0.5).
			Sort("score").
			StringDistance("internal_levenshtein").
			MaxEdits(2).
			MaxInspections(5).
			MaxTermFreq(0.01).
			PrefixLength(1).
			MinWordLength(4).
			MinDocFreq(0.0)
		src, err := s.Source(true)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("with_context", func(t *testing.T) {
		s := NewTermSuggester("my-suggest")
		s.Text("test").Field("f")
		s.ContextQuery(NewSuggesterCategoryQuery("genre"))
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("with_multiple_contexts", func(t *testing.T) {
		s := NewTermSuggester("my-suggest")
		s.Text("test").Field("f")
		s.ContextQueries(
			NewSuggesterCategoryQuery("genre"),
			NewSuggesterCategoryQuery("type"),
		)
		src, err := s.Source(false)
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestSuggestField_Coverage(t *testing.T) {
	t.Run("single_input", func(t *testing.T) {
		f := NewSuggestField("hello").Weight(1)
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "hello")
	})

	t.Run("multiple_inputs", func(t *testing.T) {
		f := NewSuggestField("hello", "world").Weight(5)
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "hello")
	})

	t.Run("single_context", func(t *testing.T) {
		f := NewSuggestField("hello").
			Weight(1).
			ContextQuery(NewSuggesterCategoryQuery("genre"))
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "contexts")
	})

	t.Run("multiple_contexts", func(t *testing.T) {
		f := NewSuggestField("hello").
			Weight(1).
			ContextQuery(
				NewSuggesterCategoryQuery("genre"),
				NewSuggesterCategoryQuery("type"),
			)
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "contexts")
	})

	t.Run("nil_input", func(t *testing.T) {
		f := NewSuggestField()
		f.Input("added")
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.Contains(t, string(data), "added")
	})

	t.Run("negative_weight", func(t *testing.T) {
		f := NewSuggestField("hello")
		data, err := f.MarshalJSON()
		require.NoError(t, err)
		assert.NotContains(t, string(data), "weight")
	})
}

func TestSearchSource_InnerHit_NoPathType(t *testing.T) {
	ss := NewSearchSource()
	ss.InnerHit("hit", NewInnerHit())
	src, err := ss.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "inner_hits")
}

func TestSourcePipeline_Coverage(t *testing.T) {
	body := map[string]any{"buckets_path": "a"}
	meta := map[string]any{"x": "y"}
	script := NewScriptInline("1")
	src, err := sourcePipeline("avg_bucket", body, meta, script)
	require.NoError(t, err)
	m := src.(map[string]any)["avg_bucket"].(map[string]any)
	assert.Contains(t, m, "script")
	assert.Contains(t, m, "meta")
}

func TestSourceAgg_Coverage(t *testing.T) {
	body := map[string]any{"field": "price"}
	subs := map[string]Aggregation{"sub_avg": AvgAggregation{Field: "price"}}
	meta := map[string]any{"x": "y"}
	script := NewScriptInline("1")
	src, err := sourceAgg("terms", body, subs, meta, script)
	require.NoError(t, err)
	m := src.(map[string]any)["terms"].(map[string]any)
	assert.Contains(t, m, "script")
	assert.Contains(t, m, "aggregations")
	assert.Contains(t, m, "meta")
}

func TestSuggesterCategoryMapping_Source_Coverage(t *testing.T) {
	m := NewSuggesterCategoryMapping("color")
	m.DefaultValues("red", "blue")
	m.FieldName("color_field")
	src, err := m.Source()
	require.NoError(t, err)
	m2 := src.(map[string]any)["color"].(map[string]any)
	assert.Equal(t, "category", m2["type"])
	assert.Contains(t, m2, "default")
	assert.Equal(t, "color_field", m2["path"])
}

func TestSuggesterCategoryMapping_NoDefaults_Coverage(t *testing.T) {
	m := NewSuggesterCategoryMapping("color")
	src, err := m.Source()
	require.NoError(t, err)
	m2 := src.(map[string]any)["color"].(map[string]any)
	assert.Contains(t, m2, "default")
}

func TestSuggesterCategoryQuery_Source_Coverage(t *testing.T) {
	q := NewSuggesterCategoryQuery("color")
	q.ValueWithBoost("red", 2)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "color")
}

func TestSuggesterCategoryQuery_Empty_Coverage(t *testing.T) {
	q := NewSuggesterCategoryQuery("color")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.Contains(t, m, "color")
}

func TestSuggesterCategoryIndex_Source_Coverage(t *testing.T) {
	t.Run("no_values", func(t *testing.T) {
		ci := NewSuggesterCategoryIndex("color")
		src, err := ci.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "color")
	})

	t.Run("single_value", func(t *testing.T) {
		ci := NewSuggesterCategoryIndex("color", "red")
		src, err := ci.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Equal(t, "red", m["color"])
	})

	t.Run("multiple_values", func(t *testing.T) {
		ci := NewSuggesterCategoryIndex("color", "red", "blue")
		src, err := ci.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.NotNil(t, m["color"])
	})
}

func TestSuggesterGeoMapping_Source_Coverage(t *testing.T) {
	m := NewSuggesterGeoMapping("location")
	m.DefaultLocations(GeoPointFromLatLon(40.0, -74.0), GeoPointFromLatLon(35.0, -110.0))
	m.Precision("5m", "1km")
	m.Neighbors(true)
	m.FieldName("pin")
	src, err := m.Source()
	require.NoError(t, err)
	m2 := src.(map[string]any)["location"].(map[string]any)
	assert.Equal(t, "geo", m2["type"])
	assert.Contains(t, m2, "precision")
	assert.Contains(t, m2, "neighbors")
	assert.Contains(t, m2, "default")
	assert.Equal(t, "pin", m2["path"])
}

func TestSuggesterGeoMapping_SingleLocation_Coverage(t *testing.T) {
	m := NewSuggesterGeoMapping("location")
	m.DefaultLocations(GeoPointFromLatLon(40.0, -74.0))
	src, err := m.Source()
	require.NoError(t, err)
	m2 := src.(map[string]any)["location"].(map[string]any)
	assert.Contains(t, m2, "default")
}

func TestSuggesterGeoQuery_Source_Coverage(t *testing.T) {
	q := NewSuggesterGeoQuery("location", GeoPointFromLatLon(40.0, -74.0))
	q.Precision("1km").Boost(2).Neighbours("2km", "5km")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["location"].(map[string]any)
	assert.Contains(t, m, "context")
	assert.Contains(t, m, "precision")
	assert.Contains(t, m, "boost")
}

func TestSuggesterGeoQuery_SingleNeighbour_Coverage(t *testing.T) {
	q := NewSuggesterGeoQuery("location", GeoPointFromLatLon(40.0, -74.0))
	q.Neighbours("2km")
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["location"].(map[string]any)
	assert.Equal(t, "2km", m["neighbours"])
}

func TestSuggesterGeoIndex_Source_Coverage(t *testing.T) {
	t.Run("no_locations", func(t *testing.T) {
		gi := NewSuggesterGeoIndex("location")
		src, err := gi.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.Contains(t, m, "location")
	})

	t.Run("single_location", func(t *testing.T) {
		gi := NewSuggesterGeoIndex("location").Locations(GeoPointFromLatLon(40.0, -74.0))
		src, err := gi.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.NotNil(t, m["location"])
	})

	t.Run("multiple_locations", func(t *testing.T) {
		gi := NewSuggesterGeoIndex("location").Locations(
			GeoPointFromLatLon(40.0, -74.0),
			GeoPointFromLatLon(35.0, -110.0),
		)
		src, err := gi.Source()
		require.NoError(t, err)
		m := src.(map[string]any)
		assert.NotNil(t, m["location"])
	})
}

func TestSuggesterCategoryQuery_ValueWithBoost(t *testing.T) {
	q := NewSuggesterCategoryQuery("color", "red")
	q.ValueWithBoost("blue", 3)
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)
	assert.NotNil(t, m["color"])
}

func TestRawStringQuery_Coverage(t *testing.T) {
	q := NewRawStringQuery(`{"match_all":{}}`)
	src, err := q.Source()
	require.NoError(t, err)
	assert.NotNil(t, src)
}

func TestFunctionScoreQuery_NilScoreFunc(t *testing.T) {
	q := NewFunctionScoreQuery()
	q.Add(NewMatchAllQuery(), NewWeightFactorFunction(1.0))
	src, err := q.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["function_score"].(map[string]any)
	assert.Contains(t, m, "functions")
}

func TestAggregationConstructors_Coverage(t *testing.T) {
	t.Run("children", func(t *testing.T) {
		a := NewChildrenAggregation()
		a.Type = "answer"
		a.SubAggs = map[string]Aggregation{"avg_score": AvgAggregation{Field: "score"}}
		a.Meta = map[string]any{"color": "blue"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["children"].(map[string]any)
		assert.Equal(t, "answer", m["type"])
		assert.Contains(t, m, "aggregations")
		assert.Contains(t, m, "meta")
	})

	t.Run("nested", func(t *testing.T) {
		a := NewNestedAggregation()
		a.Path = "comments"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["nested"].(map[string]any)
		assert.Equal(t, "comments", m["path"])
	})

	t.Run("filter", func(t *testing.T) {
		a := NewFilterAggregation()
		a.Filter = NewTermQuery("status", "active")
		a.SubAggs = map[string]Aggregation{"count": ValueCountAggregation{Field: "_id"}}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["filter"].(map[string]any)
		assert.Contains(t, m, "filter")
		assert.Contains(t, m, "aggregations")
	})

	t.Run("global", func(t *testing.T) {
		a := NewGlobalAggregation()
		a.SubAggs = map[string]Aggregation{"avg_price": AvgAggregation{Field: "price"}}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["global"].(map[string]any)
		assert.Contains(t, m, "aggregations")
	})

	t.Run("missing", func(t *testing.T) {
		a := NewMissingAggregation()
		a.Field = "price"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["missing"].(map[string]any)
		assert.Equal(t, "price", m["field"])
	})

	t.Run("sampler", func(t *testing.T) {
		a := NewSamplerAggregation()
		a.ShardSize = 200
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["sampler"].(map[string]any)
		assert.Equal(t, 200, m["shard_size"])
	})

	t.Run("reverse_nested", func(t *testing.T) {
		a := NewReverseNestedAggregation()
		a.Path = "comments"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["reverse_nested"].(map[string]any)
		assert.Equal(t, "comments", m["path"])
	})

	t.Run("geo_hash_grid", func(t *testing.T) {
		a := NewGeoHashGridAggregation()
		a.GeoHashField = "location"
		a.Precision = "5"
		sz := 10000
		ssz := 1000
		a.Size = &sz
		a.ShardSize = &ssz
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["geohash_grid"].(map[string]any)
		assert.Equal(t, "location", m["field"])
		assert.Equal(t, "5", m["precision"])
	})

	t.Run("rare_terms", func(t *testing.T) {
		a := NewRareTermsAggregation()
		a.Field = "tags"
		mdc := 2
		a.MaxDocCount = &mdc
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["rare_terms"].(map[string]any)
		assert.Equal(t, "tags", m["field"])
		assert.Equal(t, 2, m["max_doc_count"])
	})

	t.Run("matrix_stats", func(t *testing.T) {
		a := NewMatrixStatsAggregation()
		a.Fields = []string{"poverty", "income"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["matrix_stats"].(map[string]any)
		assert.Contains(t, m, "fields")
	})

	t.Run("cardinality", func(t *testing.T) {
		a := NewCardinalityAggregation()
		a.Field = "author"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["cardinality"].(map[string]any)
		assert.Equal(t, "author", m["field"])
	})

	t.Run("extended_stats", func(t *testing.T) {
		a := NewExtendedStatsAggregation()
		a.Field = "grade"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["extended_stats"].(map[string]any)
		assert.Equal(t, "grade", m["field"])
	})

	t.Run("geo_bounds", func(t *testing.T) {
		a := NewGeoBoundsAggregation()
		a.Field = "location"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["geo_bounds"].(map[string]any)
		assert.Equal(t, "location", m["field"])
	})

	t.Run("geo_centroid", func(t *testing.T) {
		a := NewGeoCentroidAggregation()
		a.Field = "location"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["geo_centroid"].(map[string]any)
		assert.Equal(t, "location", m["field"])
	})

	t.Run("max", func(t *testing.T) {
		a := NewMaxAggregation()
		a.Field = "price"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["max"].(map[string]any)
		assert.Equal(t, "price", m["field"])
	})

	t.Run("min", func(t *testing.T) {
		a := NewMinAggregation()
		a.Field = "price"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["min"].(map[string]any)
		assert.Equal(t, "price", m["field"])
	})

	t.Run("stats", func(t *testing.T) {
		a := NewStatsAggregation()
		a.Field = "grade"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["stats"].(map[string]any)
		assert.Equal(t, "grade", m["field"])
	})

	t.Run("sum", func(t *testing.T) {
		a := NewSumAggregation()
		a.Field = "price"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["sum"].(map[string]any)
		assert.Equal(t, "price", m["field"])
	})

	t.Run("avg", func(t *testing.T) {
		a := NewAvgAggregation()
		a.Field = "grade"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["avg"].(map[string]any)
		assert.Equal(t, "grade", m["field"])
	})

	t.Run("value_count", func(t *testing.T) {
		a := NewValueCountAggregation()
		a.Field = "grade"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["value_count"].(map[string]any)
		assert.Equal(t, "grade", m["field"])
	})

	t.Run("median_absolute_deviation", func(t *testing.T) {
		a := NewMedianAbsoluteDeviationAggregation()
		a.Field = "load_time"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["median_absolute_deviation"].(map[string]any)
		assert.Equal(t, "load_time", m["field"])
	})

	t.Run("percentiles", func(t *testing.T) {
		a := NewPercentilesAggregation()
		a.Field = "load_time"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percentiles"].(map[string]any)
		assert.Equal(t, "load_time", m["field"])
	})

	t.Run("percentile_ranks", func(t *testing.T) {
		a := NewPercentileRanksAggregation()
		a.Field = "load_time"
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percentile_ranks"].(map[string]any)
		assert.Equal(t, "load_time", m["field"])
	})

	t.Run("scripted_metric", func(t *testing.T) {
		a := NewScriptedMetricAggregation()
		a.InitScript = NewScriptInline("state.transactions = []")
		a.MapScript = NewScriptInline("state.transactions.add(doc.type.value)")
		a.CombineScript = NewScriptInline("return state.transactions")
		a.ReduceScript = NewScriptInline("return states")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["scripted_metric"].(map[string]any)
		assert.Contains(t, m, "init_script")
	})

	t.Run("top_hits", func(t *testing.T) {
		a := NewTopHitsAggregation()
		a.SearchSourceBuilder(nil)
		a.Size(5).From(0).SortBy(NewFieldSort("date").Desc()).FetchSource(true)
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["top_hits"].(map[string]any)
		assert.NotNil(t, m)
	})

	t.Run("weighted_avg", func(t *testing.T) {
		a := NewWeightedAvgAggregation()
		a.Value = &MultiValuesSourceFieldConfig{FieldName: "grade"}
		a.Weight = &MultiValuesSourceFieldConfig{FieldName: "weight"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["weighted_avg"].(map[string]any)
		assert.Contains(t, m, "value")
	})

	t.Run("weighted_avg", func(t *testing.T) {
		a := NewWeightedAvgAggregation()
		a.Value = &MultiValuesSourceFieldConfig{FieldName: "grade"}
		a.Weight = &MultiValuesSourceFieldConfig{FieldName: "weight"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["weighted_avg"].(map[string]any)
		assert.Contains(t, m, "value")
		assert.Contains(t, m, "weight")
	})
}

func TestPipelineAggregationConstructors_Coverage(t *testing.T) {
	t.Run("avg_bucket", func(t *testing.T) {
		a := NewAvgBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["avg_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("sum_bucket", func(t *testing.T) {
		a := NewSumBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["sum_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("max_bucket", func(t *testing.T) {
		a := NewMaxBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["max_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("min_bucket", func(t *testing.T) {
		a := NewMinBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["min_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("stats_bucket", func(t *testing.T) {
		a := NewStatsBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["stats_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("percentiles_bucket", func(t *testing.T) {
		a := NewPercentilesBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["percentiles_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("extended_stats_bucket", func(t *testing.T) {
		a := NewExtendedStatsBucketAggregation()
		a.BucketsPaths = []string{"sales_per_month>sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["extended_stats_bucket"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("derivative", func(t *testing.T) {
		a := NewDerivativeAggregation()
		a.BucketsPaths = []string{"sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["derivative"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("cumulative_sum", func(t *testing.T) {
		a := NewCumulativeSumAggregation()
		a.BucketsPaths = []string{"sales"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["cumulative_sum"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("serial_diff", func(t *testing.T) {
		a := NewSerialDiffAggregation()
		a.BucketsPaths = []string{"the_sum"}
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["serial_diff"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
	})

	t.Run("bucket_script", func(t *testing.T) {
		a := NewBucketScriptAggregation()
		a.BucketsPathsMap = map[string]string{"tshirt": "tshirt", "total": "_count"}
		a.Script = NewScriptInline("params.tshirt / params.total * 100")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["bucket_script"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
		assert.Contains(t, m, "script")
	})

	t.Run("bucket_selector", func(t *testing.T) {
		a := NewBucketSelectorAggregation()
		a.BucketsPathsMap = map[string]string{"totalSales": "total_sales"}
		a.Script = NewScriptInline("params.totalSales > 200")
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["bucket_selector"].(map[string]any)
		assert.Contains(t, m, "buckets_path")
		assert.Contains(t, m, "script")
	})

	t.Run("bucket_sort", func(t *testing.T) {
		a := NewBucketSortAggregation()
		a.Sorters = []Sorter{NewFieldSort("total_sales").Desc()}
		a.From = 0
		a.Size = 3
		src, err := a.Source()
		require.NoError(t, err)
		m := src.(map[string]any)["bucket_sort"].(map[string]any)
		assert.Contains(t, m, "sort")
		assert.Equal(t, 3, m["size"])
	})
}

func TestTermsAggregation_Methods_Coverage(t *testing.T) {
	a := NewTermsAggregation().
		WithField("status").
		WithScript(NewScriptInline("_value.toUpperCase()")).
		WithMissing("N/A").
		WithMeta(map[string]any{"color": "blue"}).
		WithRequiredSize(10).
		WithShardSize(100).
		WithShardMinDocCount(1).
		WithInclude(".*sport.*").
		WithIncludeValues("football", "basketball").
		WithExclude("golf").
		WithExcludeValues("tennis", "hockey").
		WithPartition(0).
		WithNumPartitions(10).
		WithIncludeExclude(&TermsAggregationIncludeExclude{Include: ".*", Exclude: "golf.*"}).
		WithValueType("string").
		OrderBy("term", true).
		OrderByCountAsc().
		OrderByCountDesc().
		OrderByTerm(true).
		OrderByTermAsc().
		OrderByTermDesc().
		OrderByKey(true).
		OrderByKeyAsc().
		OrderByKeyDesc().
		OrderByAggregation("_count", false).
		OrderByAggregationAndMetric("avg_price", "avg", false).
		WithExecutionHint("map").
		WithCollectionMode("breadth_first").
		WithShowTermDocCountError(true).
		WithMinDocCount(5)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["terms"].(map[string]any)
	assert.Equal(t, "status", m["field"])
}

func TestSignificantTermsAggregation_Coverage(t *testing.T) {
	a := NewSignificantTermsAggregation()
	a.FieldVal = "content"
	a = a.Include(".*")
	a = a.IncludeValues("foo", "bar")
	a = a.Exclude("stop.*")
	a = a.ExcludeValues("the", "a")
	a = a.Partition(0)
	a = a.NumPartitions(10)
	a = a.SetIncludeExclude(&TermsAggregationIncludeExclude{Include: ".*", Exclude: "stop.*"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["significant_terms"].(map[string]any)
	assert.Equal(t, "content", m["field"])
}

func TestSignificantTextAggregation_Coverage(t *testing.T) {
	a := NewSignificantTextAggregation()
	a.FieldVal = "content"
	a = a.ShardMinDocCount(1)
	a = a.WithSize(10)
	a = a.WithShardSize(100)
	a = a.IncludeValues("foo", "bar")
	a = a.Exclude("stop.*")
	a = a.ExcludeValues("the")
	a = a.Partition(0)
	a = a.NumPartitions(10)
	a = a.SetIncludeExclude(&TermsAggregationIncludeExclude{Include: ".*", Exclude: "stop.*"})
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["significant_text"].(map[string]any)
	assert.Equal(t, "content", m["field"])
}

func TestSignificanceHeuristics_Coverage(t *testing.T) {
	t.Run("chi_square", func(t *testing.T) {
		h := NewChiSquareSignificanceHeuristic()
		assert.Equal(t, "chi_square", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("gnd", func(t *testing.T) {
		h := NewGNDSignificanceHeuristic()
		assert.Equal(t, "gnd", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("jlh", func(t *testing.T) {
		h := NewJLHScoreSignificanceHeuristic()
		assert.Equal(t, "jlh", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("mutual_information", func(t *testing.T) {
		h := NewMutualInformationSignificanceHeuristic()
		assert.Equal(t, "mutual_information", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("percentage_score", func(t *testing.T) {
		h := NewPercentageScoreSignificanceHeuristic()
		assert.Equal(t, "percentage", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})

	t.Run("script", func(t *testing.T) {
		h := NewScriptSignificanceHeuristic()
		h.ScriptVal = NewScriptInline("_score * 2")
		assert.Equal(t, "script_heuristic", h.Name())
		src, err := h.Source()
		require.NoError(t, err)
		assert.NotNil(t, src)
	})
}

func TestMovAvgModels_Coverage(t *testing.T) {
	t.Run("ewma", func(t *testing.T) {
		m := NewEWMAMovAvgModel()
		assert.Equal(t, "ewma", m.Name())
		src := m.Settings()
		assert.NotNil(t, src)
	})

	t.Run("holt_linear", func(t *testing.T) {
		m := NewHoltLinearMovAvgModel()
		assert.Equal(t, "holt", m.Name())
		src := m.Settings()
		assert.NotNil(t, src)
	})

	t.Run("holt_winters", func(t *testing.T) {
		m := NewHoltWintersMovAvgModel()
		assert.Equal(t, "holt_winters", m.Name())
		src := m.Settings()
		assert.NotNil(t, src)
	})

	t.Run("linear", func(t *testing.T) {
		m := NewLinearMovAvgModel()
		assert.Equal(t, "linear", m.Name())
		src := m.Settings()
		assert.Nil(t, src)
	})

	t.Run("simple", func(t *testing.T) {
		m := NewSimpleMovAvgModel()
		assert.Equal(t, "simple", m.Name())
		src := m.Settings()
		assert.Nil(t, src)
	})
}

func TestHistogramAggregation_Coverage(t *testing.T) {
	a := NewHistogramAggregation()
	a.Field = "price"
	a.Interval = 50
	mdc := int64(0)
	a.MinDocCount = &mdc
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["histogram"].(map[string]any)
	assert.Equal(t, "price", m["field"])
	assert.Equal(t, float64(50), m["interval"])
}

func TestDateHistogramAggregation_Coverage(t *testing.T) {
	a := NewDateHistogramAggregation()
	a.Field = "date"
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["date_histogram"].(map[string]any)
	assert.Equal(t, "date", m["field"])
}

func TestGeoDistanceAggregation_Coverage(t *testing.T) {
	a := NewGeoDistanceAggregation()
	a.Field = "location"
	a.Origin = "52.376, 4.894"
	a.Ranges = []GeoDistanceRange{
		{To: "100"},
		{From: "100", To: "300"},
		{From: "300"},
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geo_distance"].(map[string]any)
	assert.Equal(t, "location", m["field"])
}

func TestAutoDateHistogram_Coverage(t *testing.T) {
	a := NewAutoDateHistogramAggregation()
	a.Field = "date"
	a = a.WithBuckets(10)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["auto_date_histogram"].(map[string]any)
	assert.Equal(t, "date", m["field"])
	assert.Equal(t, 10, m["buckets"])
}

func TestFiltersAggregation_Coverage(t *testing.T) {
	a := NewFiltersAggregation()
	a.NamedFilters = map[string]Query{
		"error":   NewTermQuery("body", "error"),
		"warning": NewTermQuery("body", "warning"),
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["filters"].(map[string]any)
	assert.Contains(t, m, "filters")
}

func TestAdjacencyMatrixAggregation_Coverage(t *testing.T) {
	a := NewAdjacencyMatrixAggregation()
	a.Filters = map[string]Query{
		"grpA": NewTermQuery("accounts", "hillary"),
		"grpB": NewTermQuery("accounts", "sidney"),
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["adjacency_matrix"].(map[string]any)
	assert.Contains(t, m, "filters")
}

func TestRangeAggregation_Coverage(t *testing.T) {
	a := NewRangeAggregation()
	a.FieldVal = "price"
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["range"].(map[string]any)
	assert.Equal(t, "price", m["field"])
}

func TestMultiTermsAggregation_Coverage(t *testing.T) {
	a := NewMultiTermsAggregation()
	a.MultiTermsData = []MultiTerm{{Field: "genre"}, {Field: "product"}}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["multi_terms"].(map[string]any)
	assert.Contains(t, m, "terms")
}

func TestCompositeAggregation_Coverage(t *testing.T) {
	a := NewCompositeAggregation()
	sz := 5
	a.Size = &sz
	a.ValuesSources = []CompositeAggregationValuesSource{
		NewCompositeAggregationTermsValuesSource("product").Field("product"),
		NewCompositeAggregationHistogramValuesSource("price", 50).Field("price"),
	}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["composite"].(map[string]any)
	assert.Contains(t, m, "sources")
}

func TestDiversifiedSamplerAggregation_Coverage(t *testing.T) {
	a := NewDiversifiedSamplerAggregation()
	a.Field = "author"
	a.ShardSize = 100
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["diversified_sampler"].(map[string]any)
	assert.Equal(t, "author", m["field"])
}

func TestGeoTileGridAggregation_Coverage(t *testing.T) {
	a := NewGeoTileGridAggregation()
	a.Field = "location"
	p := 8
	a.Precision = &p
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["geotile_grid"].(map[string]any)
	assert.Equal(t, "location", m["field"])
	assert.Equal(t, 8, m["precision"])
}

func TestTopHitsAggregation_Coverage(t *testing.T) {
	a := NewTopHitsAggregation()
	a.SearchSourceBuilder(nil).Size(5).From(0).Sort("date", false).FetchSource(true)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["top_hits"].(map[string]any)
	assert.NotNil(t, m)
}

func TestWeightedAvgAggregation_Coverage(t *testing.T) {
	a := NewWeightedAvgAggregation()
	a.Value = &MultiValuesSourceFieldConfig{FieldName: "grade"}
	a.Weight = &MultiValuesSourceFieldConfig{FieldName: "weight"}
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["weighted_avg"].(map[string]any)
	assert.Contains(t, m, "value")
}

func TestMovFnAggregation_Coverage(t *testing.T) {
	a := NewMovFnAggregation("the_sum", NewScriptInline("MovingFunctions.linearUnweightedAvg(values)"), 10)
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["moving_fn"].(map[string]any)
	assert.Contains(t, m, "buckets_path")
}

func TestMovAvgAggregation_Coverage(t *testing.T) {
	a := NewMovAvgAggregation()
	a.BucketsPaths = []string{"the_sum"}
	a.Window = ptr(5)
	a.Model = NewEWMAMovAvgModel()
	src, err := a.Source()
	require.NoError(t, err)
	m := src.(map[string]any)["moving_avg"].(map[string]any)
	assert.Contains(t, m, "buckets_path")
}
