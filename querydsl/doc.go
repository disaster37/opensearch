// Package querydsl provides a programmatic, type-safe query builder DSL for
// constructing OpenSearch queries and aggregations.
//
// The Query DSL (Domain Specific Language) allows you to build complex search
// queries in Go using a fluent, chainable API instead of writing raw JSON.
//
// # Query Types
//
// The package provides builders for all OpenSearch query types:
//
//   - MatchQuery: Full-text search with analysis
//   - BoolQuery: Combine queries with must/should/must_not/filter
//   - TermQuery: Exact value matching
//   - RangeQuery: Numeric, date, and string range queries
//   - NestedQuery: Query nested object fields
//   - ExistsQuery: Check field existence
//   - WildcardQuery: Pattern matching with * and ?
//   - RegexpQuery: Regular expression matching
//   - FuzzyQuery: Fuzzy matching with edit distance
//   - And many more...
//
// # Building Queries
//
// Use the builder pattern to construct queries:
//
//	// Simple match query
//	query := querydsl.MatchQuery("title", "OpenSearch")
//
//	// Bool query with multiple clauses
//	boolQuery := querydsl.BoolQuery().
//	    Must(querydsl.MatchQuery("status", "published")).
//	    Filter(querydsl.RangeQuery("date").Gte("2024-01-01")).
//	    Should(querydsl.TermQuery("tags", "elasticsearch")).
//	    Should(querydsl.TermQuery("tags", "opensearch")).
//	    MinimumShouldMatch("1")
//
//	// Nested query
//	nestedQuery := querydsl.NestedQuery("comments",
//	    querydsl.MatchQuery("comments.text", "bug"),
//	    "avg", // score_mode
//	)
//
// # Aggregations
//
// Build analytics aggregations using the same pattern:
//
//	// Terms aggregation with sub-aggregation
//	agg := querydsl.TermsAggregation("status").
//	    Size(10).
//	    SubAggregation("avg_score",
//	        querydsl.AvgAggregation("score"),
//	    )
//
//	// Date histogram with moving average
//	timeSeries := querydsl.DateHistogramAggregation("timestamp").
//	    CalendarInterval("1d").
//	    SubAggregation("avg_value",
//	        querydsl.AvgAggregation("value"),
//	    ).
//	    SubAggregation("moving_avg",
//	        querydsl.MovAvgAggregation("avg_value").
//	            Window(7).
//	            Predict(3),
//	    )
//
// # Sorting
//
// Sort results using the sort builders:
//
//	// Sort by score descending, then by date ascending
//	searchSource := querydsl.SearchSource().
//	    Query(boolQuery).
//	    Sort(querydsl.ScoreSort(false)).       // descending
//	    Sort(querydsl.FieldSort("date").Asc()) // ascending
//
// # Scripting
//
// Use Script to reference inline or stored scripts:
//
//	// Inline script
//	script := querydsl.Script().
//	    Source("doc['price'].value * doc['quantity'].value").
//	    Lang("painless")
//
//	// Stored script
//	storedScript := querydsl.Script().
//	    Id("calculate_total").
//	    Params(map[string]any{
//	        "tax_rate": 0.08,
//	    })
//
// # Point in Time
//
// Use PointInTime for consistent search across changing data:
//
//	pit := querydsl.PointInTime("pit-id-here", "5m")
//	searchSource := querydsl.SearchSource().
//	    Query(querydsl.MatchAllQuery()).
//	    Pit(pit)
package querydsl
