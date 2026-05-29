// mocks_test.go defines test-only mock implementations of the interfaces
// declared in this package. They exist to exercise error-propagation paths
// in composite query/aggregation builders (e.g. BoolQuery, FunctionScoreQuery,
// SearchSource) without requiring a real OpenSearch cluster.
//
// Each mock implements one of the package's interfaces (Query, Aggregation,
// Sorter, Suggester, SuggesterContextQuery, ScoreFunction, CandidateGenerator,
// SmoothingModel, IntervalQueryRule, RankFeatureScoreFunction, or
// SignificanceHeuristic) and either:
//
//   - Always returns an error from Source() / Source(includeName), to simulate
//     a child component that failed. Used in "error propagation" tests to
//     verify that parent builders correctly propagate (not swallow) errors.
//
//   - Returns a non-map value from Source(), to test type-assertion branches
//     in builders that expect map[string]any (e.g. the multi-context-query
//     merge loop in CompletionSuggester.Source).
//
//   - Returns a value containing a non-JSON-serializable field (chan int),
//     to test json.Marshal failure branches.
//
// Naming convention: mock types follow the pattern "mock{Interface}" where
// Interface is the interface they implement, with a descriptive suffix
// indicating behavior ("Error" for error-returning, "NonMap" for non-map,
// "NonSerializable" for marshal-failing).
package querydsl

import "errors"

// ── Query / Aggregation mocks ──────────────────────────────────────────────
//
// Query and Aggregation both declare Source() (any, error), so the same
// mock can satisfy both interfaces. We keep separate names where the
// intent matters (e.g. mockQueryError vs mockAggregationError).

// mockQueryError implements Query and always returns an error.
// Used to verify that composite queries (Bool, FunctionScore, Nested, etc.)
// propagate errors from their child clauses.
type mockQueryError struct{}

func (mockQueryError) Source() (any, error) {
	return nil, errors.New("mock: query error")
}

// mockAggregationError implements Aggregation and always returns an error.
// Used in SearchSource, NestedAggregation, GlobalAggregation, etc. to verify
// that parent aggregations bubble up child Source() failures.
type mockAggregationError struct{}

func (mockAggregationError) Source() (any, error) {
	return nil, errors.New("mock: aggregation error")
}

// mockNonSerializableQuery implements Query. Its Source() returns a map
// containing a channel, which json.Marshal cannot encode. Used to exercise
// the json.Marshal failure branch in SearchRequest.Body().
type mockNonSerializableQuery struct{}

func (mockNonSerializableQuery) Source() (any, error) {
	return map[string]any{"bad": make(chan int)}, nil
}

// ── Sorter mock ────────────────────────────────────────────────────────────

// mockSorterError implements Sorter and always returns an error from Source().
// Used in SearchSource to verify that sort iteration propagates child errors.
type mockSorterError struct{}

func (mockSorterError) Source() (any, error) {
	return nil, errors.New("mock: sort error")
}

// ── Suggester mocks ────────────────────────────────────────────────────────

// mockSuggesterError implements Suggester (Name() + Source(bool)).
// Source(false) always returns an error, exercising SearchSource's
// suggester-iteration error path.
type mockSuggesterError struct{}

func (mockSuggesterError) Name() string             { return "mock" }
func (mockSuggesterError) Source(bool) (any, error) { return nil, errors.New("mock: suggester error") }

// ── SuggesterContextQuery mocks ─────────────────────────────────────────────

// mockContextQueryError implements SuggesterContextQuery and always returns
// an error. Used in CompletionSuggester, ContextSuggester, TermSuggester,
// and PhraseSuggester to verify that context-query iteration propagates errors.
type mockContextQueryError struct{}

func (mockContextQueryError) Source() (any, error) { return nil, errors.New("mock: context error") }

// mockContextQueryNonMap implements SuggesterContextQuery and returns a
// non-map value (string). The multi-context-query merge loops in the
// suggesters' Source() methods expect map[string]any and must detect
// this case via the type assertion `m, ok := src.(map[string]any)`.
type mockContextQueryNonMap struct{}

func (mockContextQueryNonMap) Source() (any, error) { return "not-a-map", nil }

// ── ScoreFunction mock (FunctionScoreQuery) ────────────────────────────────

// mockScoreFunctionError implements ScoreFunction (Name, GetWeight, Source).
// Source always returns an error, exercising FunctionScoreQuery's
// score-function-iteration error path.
type mockScoreFunctionError struct{}

func (mockScoreFunctionError) Name() string         { return "mock_score" }
func (mockScoreFunctionError) GetWeight() *float64  { return nil }
func (mockScoreFunctionError) Source() (any, error) { return nil, errors.New("mock: score func error") }

// ── Suggester component mocks ──────────────────────────────────────────────

// mockCandidateGeneratorError implements CandidateGenerator (Type + Source).
// Used in PhraseSuggester to verify that candidate-generator iteration
// propagates errors.
type mockCandidateGeneratorError struct{}

func (mockCandidateGeneratorError) Type() string { return "direct_generator" }
func (mockCandidateGeneratorError) Source() (any, error) {
	return nil, errors.New("mock: candidate generator error")
}

// mockSmoothingModelError implements SmoothingModel (Type + Source).
// Used in PhraseSuggester to verify that the smoothing model error
// propagates from Source().
type mockSmoothingModelError struct{}

func (mockSmoothingModelError) Type() string         { return "mock_smoothing" }
func (mockSmoothingModelError) Source() (any, error) { return nil, errors.New("mock: smoothing error") }

// ── RankFeatureScoreFunction mock ──────────────────────────────────────────

// mockRankFeatureFunctionError implements RankFeatureScoreFunction.
// Used in RankFeatureQuery to verify error propagation.
type mockRankFeatureFunctionError struct{}

func (mockRankFeatureFunctionError) Name() string { return "mock_rf" }
func (mockRankFeatureFunctionError) Source() (any, error) {
	return nil, errors.New("mock: rank feature error")
}

// ── IntervalQueryRule mock ─────────────────────────────────────────────────

// mockIntervalRuleError implements IntervalQueryRule and always returns
// an error from Source(). Used to exercise error paths in IntervalQuery,
// IntervalQueryRuleAllOf, IntervalQueryRuleAnyOf, and IntervalQueryFilter.
type mockIntervalRuleError struct{}

func (mockIntervalRuleError) Source() (any, error) {
	return nil, errors.New("mock: interval rule error")
}
func (mockIntervalRuleError) isIntervalQueryRule() bool { return true }

// ── SignificanceHeuristic mock ─────────────────────────────────────────────

// mockSignificanceHeuristicError implements SignificanceHeuristic
// (Name + Source). Used in SignificantTermsAggregation and
// SignificantTextAggregation to verify heuristic error propagation.
type mockSignificanceHeuristicError struct{}

func (mockSignificanceHeuristicError) Name() string { return "mock_heuristic" }
func (mockSignificanceHeuristicError) Source() (any, error) {
	return nil, errors.New("mock: heuristic error")
}
