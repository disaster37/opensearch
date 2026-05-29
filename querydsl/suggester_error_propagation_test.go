// suggester_error_propagation_test.go verifies that suggester builders
// (Completion, Context, Term, Phrase) and interval query components
// propagate errors from context queries, candidate generators, and
// smoothing models. Uses the mock types in mocks_test.go.
package querydsl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// CompletionSuggester context query error paths

func TestEP2_CompletionSuggester_SingleContextQueryError(t *testing.T) {
	s := NewCompletionSuggester("cs").
		Field("suggest").
		Prefix("ni").
		ContextQuery(mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_CompletionSuggester_MultiContextQueryError(t *testing.T) {
	s := NewCompletionSuggester("cs").
		Field("suggest").
		Prefix("ni").
		ContextQueries(mockContextQueryError{}, mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_CompletionSuggester_MultiContextQueryNonMap(t *testing.T) {
	s := NewCompletionSuggester("cs").
		Field("suggest").
		Prefix("ni").
		ContextQueries(mockContextQueryNonMap{}, mockContextQueryNonMap{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// ContextSuggester context query error paths

func TestEP2_ContextSuggester_SingleContextQueryError(t *testing.T) {
	s := NewContextSuggester("ctx").
		Field("suggest").
		Prefix("ni").
		ContextQuery(mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_ContextSuggester_MultiContextQueryError(t *testing.T) {
	s := NewContextSuggester("ctx").
		Field("suggest").
		Prefix("ni").
		ContextQueries(mockContextQueryError{}, mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_ContextSuggester_MultiContextQueryNonMap(t *testing.T) {
	s := NewContextSuggester("ctx").
		Field("suggest").
		Prefix("ni").
		ContextQueries(mockContextQueryNonMap{}, mockContextQueryNonMap{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// TermSuggester context query error paths

func TestEP2_TermSuggester_SingleContextQueryError(t *testing.T) {
	s := NewTermSuggester("ts").
		Field("title").
		Text("openseerch").
		ContextQuery(mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_TermSuggester_MultiContextQueryError(t *testing.T) {
	s := NewTermSuggester("ts").
		Field("title").
		Text("openseerch").
		ContextQueries(mockContextQueryError{}, mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// PhraseSuggester context query error paths

func TestEP2_PhraseSuggester_SingleContextQueryError(t *testing.T) {
	s := NewPhraseSuggester("ps").
		Field("title.trigram").
		Text("noble prize").
		ContextQuery(mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

func TestEP2_PhraseSuggester_MultiContextQueryError(t *testing.T) {
	s := NewPhraseSuggester("ps").
		Field("title.trigram").
		Text("noble prize").
		ContextQueries(mockContextQueryError{}, mockContextQueryError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// PhraseSuggester candidate generator error path

func TestEP2_PhraseSuggester_CandidateGeneratorError(t *testing.T) {
	s := NewPhraseSuggester("ps").
		Field("title.trigram").
		Text("noble prize").
		CandidateGenerator(mockCandidateGeneratorError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// PhraseSuggester smoothing model error path

func TestEP2_PhraseSuggester_SmoothingModelError(t *testing.T) {
	s := NewPhraseSuggester("ps").
		Field("title.trigram").
		Text("noble prize").
		SmoothingModel(mockSmoothingModelError{})
	_, err := s.Source(false)
	assert.Error(t, err)
}

// IntervalQueryRuleAllOf filter error path

func TestEP2_IntervalQueryRuleAllOf_FilterError(t *testing.T) {
	f := NewIntervalQueryFilter().Before(mockIntervalRuleError{})
	r := NewIntervalQueryRuleAllOf(NewIntervalQueryRuleMatch("a")).Filter(f)
	_, err := r.Source()
	assert.Error(t, err)
}

// IntervalQueryRuleAnyOf filter error path

func TestEP2_IntervalQueryRuleAnyOf_FilterError(t *testing.T) {
	f := NewIntervalQueryFilter().After(mockIntervalRuleError{})
	r := NewIntervalQueryRuleAnyOf(NewIntervalQueryRuleMatch("b")).Filter(f)
	_, err := r.Source()
	assert.Error(t, err)
}

// IntervalQueryFilter all rule field error paths

func TestEP2_IntervalQueryFilter_ContainedByError(t *testing.T) {
	f := NewIntervalQueryFilter().ContainedBy(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_IntervalQueryFilter_ContainingError(t *testing.T) {
	f := NewIntervalQueryFilter().Containing(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_IntervalQueryFilter_OverlappingError(t *testing.T) {
	f := NewIntervalQueryFilter().Overlapping(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_IntervalQueryFilter_NotContainedByError(t *testing.T) {
	f := NewIntervalQueryFilter().NotContainedBy(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_IntervalQueryFilter_NotContainingError(t *testing.T) {
	f := NewIntervalQueryFilter().NotContaining(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_IntervalQueryFilter_NotOverlappingError(t *testing.T) {
	f := NewIntervalQueryFilter().NotOverlapping(mockIntervalRuleError{})
	_, err := f.Source()
	assert.Error(t, err)
}

func TestEP2_ParentIdQuery_InnerHitError(t *testing.T) {
	ih := NewInnerHit()
	ih.Query(mockQueryError{})
	q := NewParentIdQuery("answer", "1")
	q.InnerHit = ih
	_, err := q.Source()
	assert.Error(t, err)
}

func TestEP2_SuggestField_MarshalJSON_SingleContextError(t *testing.T) {
	sf := NewSuggestField("test").Weight(5).ContextQuery(mockContextQueryError{})
	_, err := sf.MarshalJSON()
	assert.Error(t, err)
}

func TestEP2_SuggestField_MarshalJSON_MultiContextError(t *testing.T) {
	sf := NewSuggestField("a", "b").ContextQuery(mockContextQueryError{}, mockContextQueryError{})
	_, err := sf.MarshalJSON()
	assert.Error(t, err)
}

func TestEP2_SuggestField_MarshalJSON_MultiContextNonMap(t *testing.T) {
	sf := NewSuggestField("a", "b").ContextQuery(mockContextQueryNonMap{}, mockContextQueryNonMap{})
	_, err := sf.MarshalJSON()
	assert.Error(t, err)
}

func TestEP2_SearchRequest_Body_MarshalError(t *testing.T) {
	r := NewSearchRequest()
	r.searchSource.Query(mockNonSerializableQuery{})
	_, err := r.Body()
	assert.Error(t, err)
}
