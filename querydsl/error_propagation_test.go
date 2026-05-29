// error_propagation_test.go verifies that composite query and
// aggregation builders propagate errors from child Source() calls
// rather than swallowing them. Tests use the mock types in
// mocks_test.go (mockQueryError, mockAggregationError, etc.) as
// children injected into parent builders like BoolQuery, DisMaxQuery,
// and SearchSource.
package querydsl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Query error propagation

func TestEP_BoolQuery_MustMultiError(t *testing.T) {
	q := NewBoolQuery().Must(NewMatchAllQuery(), mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_BoolQuery_ShouldMultiError(t *testing.T) {
	q := NewBoolQuery().Should(NewMatchAllQuery(), mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_BoolQuery_MustNotMultiError(t *testing.T) {
	q := NewBoolQuery().MustNot(NewMatchAllQuery(), mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_BoolQuery_FilterMultiError(t *testing.T) {
	q := NewBoolQuery().Filter(NewMatchAllQuery(), mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_BoostingQuery_PositiveError(t *testing.T) {
	q := BoostingQuery{Positive: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_BoostingQuery_NegativeError(t *testing.T) {
	q := BoostingQuery{Negative: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_ConstantScoreQuery_FilterError(t *testing.T) {
	q := ConstantScoreQuery{Filter: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_DisMaxQuery_QueriesError(t *testing.T) {
	q := DisMaxQuery{Queries: []Query{NewMatchAllQuery(), mockQueryError{}}}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_FunctionScoreQuery_QueryError(t *testing.T) {
	q := NewFunctionScoreQuery().Query(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_FunctionScoreQuery_FilterError(t *testing.T) {
	q := NewFunctionScoreQuery().Filter(mockQueryError{})
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_FunctionScoreQuery_AddFilterError(t *testing.T) {
	q := NewFunctionScoreQuery().Add(mockQueryError{}, NewRandomFunction())
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_HasChildQuery_QueryError(t *testing.T) {
	q := HasChildQuery{Query: mockQueryError{}, Type: "answer"}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_HasParentQuery_QueryError(t *testing.T) {
	q := HasParentQuery{Query: mockQueryError{}, ParentType: "question"}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_NestedQuery_QueryError(t *testing.T) {
	q := NestedQuery{Query: mockQueryError{}, Path: "comments"}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_ScriptScoreQuery_QueryError(t *testing.T) {
	q := ScriptScoreQuery{Query: mockQueryError{}, Script: NewScript("1")}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_SpanFirstQuery_MatchError(t *testing.T) {
	q := SpanFirstQuery{Match: mockQueryError{}, End: 5}
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP_SpanNearQuery_ClausesError(t *testing.T) {
	q := SpanNearQuery{Clauses: []Query{mockQueryError{}}}
	_, err := q.Source()
	assert.Error(t, err)
}

// FilterAggregation filter error

func TestEP_FilterAggregation_FilterError(t *testing.T) {
	a := FilterAggregation{Filter: mockQueryError{}}
	_, err := a.Source()
	assert.Error(t, err)
}

// Aggregation error propagation via sub-aggregations

func TestEP_NestedAggregation_SubAggError(t *testing.T) {
	a := NestedAggregation{
		Path:    "items",
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestEP_GlobalAggregation_SubAggError(t *testing.T) {
	a := GlobalAggregation{
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestEP_ReverseNestedAggregation_SubAggError(t *testing.T) {
	a := ReverseNestedAggregation{
		Path:    "p",
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

func TestEP_FilterAggregation_SubAggError(t *testing.T) {
	a := FilterAggregation{
		Filter:  NewMatchAllQuery(),
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

// SearchSource error propagation

func TestEP_SearchSource_QueryError(t *testing.T) {
	_, err := NewSearchSource().Query(mockQueryError{}).Source()
	assert.Error(t, err)
}

func TestEP_SearchSource_PostFilterError(t *testing.T) {
	_, err := NewSearchSource().PostFilter(mockQueryError{}).Source()
	assert.Error(t, err)
}

func TestEP_SearchSource_SliceError(t *testing.T) {
	_, err := NewSearchSource().Slice(mockQueryError{}).Source()
	assert.Error(t, err)
}

func TestEP_SearchSource_AggregationError(t *testing.T) {
	ss := NewSearchSource()
	ss.aggregations["bad"] = mockAggregationError{}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestEP_SearchSource_SorterError(t *testing.T) {
	ss := NewSearchSource()
	ss.sorters = []Sorter{mockSorterError{}}
	_, err := ss.Source()
	assert.Error(t, err)
}

func TestEP_SearchSource_MultiRescorerError(t *testing.T) {
	r1 := NewRescore().Rescorer(NewQueryRescorer(NewMatchAllQuery()))
	r2 := NewRescore().Rescorer(NewQueryRescorer(mockQueryError{}))
	ss := NewSearchSource().Rescorer(r1).Rescorer(r2)
	_, err := ss.Source()
	assert.Error(t, err)
}

// marshalStruct error paths

func TestEP_MarshalStruct_UnmarshalableType(t *testing.T) {
	_, err := marshalStruct(make(chan int))
	assert.Error(t, err)
}

func TestEP_MarshalStruct_NonObject(t *testing.T) {
	_, err := marshalStruct([]int{1, 2, 3})
	assert.Error(t, err)
}

// NestedQuery InnerHit error propagation

func TestEP_NestedQuery_InnerHitError(t *testing.T) {
	ih := NewInnerHit().Query(mockQueryError{})
	q := NestedQuery{
		Query:    NewMatchAllQuery(),
		Path:     "comments",
		InnerHit: ih,
	}
	_, err := q.Source()
	assert.Error(t, err)
}

// HasChildQuery InnerHit error propagation

func TestEP_HasChildQuery_InnerHitError(t *testing.T) {
	ih := NewInnerHit().Query(mockQueryError{})
	q := HasChildQuery{
		Query:    NewMatchAllQuery(),
		Type:     "answer",
		InnerHit: ih,
	}
	_, err := q.Source()
	assert.Error(t, err)
}

// HasParentQuery InnerHit error propagation

func TestEP_HasParentQuery_InnerHitError(t *testing.T) {
	ih := NewInnerHit().Query(mockQueryError{})
	q := HasParentQuery{
		Query:      NewMatchAllQuery(),
		ParentType: "question",
		InnerHit:   ih,
	}
	_, err := q.Source()
	assert.Error(t, err)
}

// CollapseBuilder InnerHit error propagation

func TestEP_CollapseBuilder_InnerHitError(t *testing.T) {
	ih := NewInnerHit().Query(mockQueryError{})
	cb := NewCollapseBuilder("user").InnerHit(ih)
	_, err := cb.Source()
	assert.Error(t, err)
}

// SearchSource Collapse error (indirect)

func TestEP_SearchSource_CollapseError(t *testing.T) {
	ih := NewInnerHit().Query(mockQueryError{})
	cb := NewCollapseBuilder("user").InnerHit(ih)
	_, err := NewSearchSource().Collapse(cb).Source()
	assert.Error(t, err)
}

// PinnedQuery Organic query error

func TestEP_PinnedQuery_OrganicError(t *testing.T) {
	q := PinnedQuery{Organic: mockQueryError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

// RankFeatureQuery ScoreFunction error

func TestEP_RankFeatureQuery_ScoreFuncError(t *testing.T) {
	q := RankFeatureQuery{Field: "rank", ScoreFunction: mockRankFeatureFunctionError{}}
	_, err := q.Source()
	assert.Error(t, err)
}

// SortInfo error paths

func TestEP_SortInfo_FilterError(t *testing.T) {
	info := SortInfo{Field: "f", Filter: mockQueryError{}}
	_, err := info.Source()
	assert.Error(t, err)
}

func TestEP_SortInfo_NestedFilterError(t *testing.T) {
	info := SortInfo{Field: "f", NestedFilter: mockQueryError{}}
	_, err := info.Source()
	assert.Error(t, err)
}

func TestEP_SortInfo_NestedSortError(t *testing.T) {
	ns := NewNestedSort("p").Filter(mockQueryError{})
	info := SortInfo{Field: "f", Nested: ns}
	_, err := info.Source()
	assert.Error(t, err)
}

func TestEP_SortInfo_DeprecatedNestedSortError(t *testing.T) {
	ns := NewNestedSort("p").Filter(mockQueryError{})
	info := SortInfo{Field: "f", NestedSort: ns}
	_, err := info.Source()
	assert.Error(t, err)
}

// IntervalQuery with mockIntervalRuleError

func TestEP_IntervalQuery_RuleError(t *testing.T) {
	q := NewIntervalQuery("field", mockIntervalRuleError{})
	_, err := q.Source()
	assert.Error(t, err)
}

// GeoDistanceSort NestedSort error

func TestEP_GeoDistanceSort_NestedSortError(t *testing.T) {
	ns := NewNestedSort("p").Filter(mockQueryError{})
	s := NewGeoDistanceSort("loc").Point(0, 0).NestedSort(ns)
	_, err := s.Source()
	assert.Error(t, err)
}

// NestedSort nested nested error

func TestEP_NestedSort_InnerNestedSortError(t *testing.T) {
	inner := NewNestedSort("inner").Filter(mockQueryError{})
	s := NewNestedSort("outer").NestedSort(inner)
	_, err := s.Source()
	assert.Error(t, err)
}

// FieldSort NestedSort error

func TestEP_FieldSort_NestedSortError(t *testing.T) {
	ns := NewNestedSort("p").Filter(mockQueryError{})
	s := NewFieldSort("f").Nested(ns)
	_, err := s.Source()
	assert.Error(t, err)
}

// BucketSortAggregation sorter error

func TestEP_BucketSortAggregation_SorterError(t *testing.T) {
	a := BucketSortAggregation{Sorters: []Sorter{mockSorterError{}}}
	_, err := a.Source()
	assert.Error(t, err)
}

// TermsAggregation sub-agg error

func TestEP_TermsAggregation_SubAggError(t *testing.T) {
	a := TermsAggregation{
		Field:   "genre",
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

// RareTermsAggregation sub-agg error

func TestEP_RareTermsAggregation_SubAggError(t *testing.T) {
	a := RareTermsAggregation{
		Field:   "genre",
		SubAggs: map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}

// SignificantTermsAggregation sub-agg error

func TestEP_SignificantTermsAggregation_SubAggError(t *testing.T) {
	a := SignificantTermsAggregation{
		FieldVal: "genre",
		SubAggs:  map[string]Aggregation{"bad": mockAggregationError{}},
	}
	_, err := a.Source()
	assert.Error(t, err)
}
