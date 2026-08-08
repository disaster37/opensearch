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
//	query := querydsl.NewMatchQuery("title", "OpenSearch")
//
//	// Bool query with multiple clauses
//	boolQuery := querydsl.NewBoolQuery().
//	    Must(querydsl.NewMatchQuery("status", "published")).
//	    Filter(querydsl.NewRangeQuery("date").Gte("2024-01-01")).
//	    Should(querydsl.NewTermQuery("tags", "elasticsearch")).
//	    Should(querydsl.NewTermQuery("tags", "opensearch")).
//	    MinimumShouldMatch("1")
//
//	// Nested query
//	nestedQuery := querydsl.NewNestedQuery("comments",
//	    querydsl.NewMatchQuery("comments.text", "bug"),
//	)
//
// # Aggregations
//
// Build analytics aggregations using the same pattern:
//
//	// Terms aggregation with sub-aggregation
//	agg := querydsl.NewTermsAggregation().
//	    Field("status").
//	    Size(10).
//	    SubAggregation("avg_score",
//	        querydsl.NewAvgAggregation().Field("score"),
//	    )
//
//	// Date histogram with moving average
//	timeSeries := querydsl.NewDateHistogramAggregation().
//	    Field("timestamp").
//	    CalendarInterval("1d").
//	    SubAggregation("avg_value",
//	        querydsl.NewAvgAggregation().Field("value"),
//	    ).
//	    SubAggregation("moving_avg",
//	        querydsl.NewMovAvgAggregation().
//	            Window(7).
//	            Predict(3),
//	    )
//
// # Sorting
//
// Sort results using the sort builders:
//
//	// Sort by score descending, then by date ascending
//	searchSource := querydsl.NewSearchSource().
//	    Query(boolQuery).
//	    Sort(querydsl.NewScoreSort().Desc()).   // descending
//	    Sort(querydsl.NewFieldSort("date").Asc()) // ascending
//
// # Scripting
//
// Use Script to reference inline or stored scripts:
//
//	// Inline script
//	script := querydsl.NewScript("doc['price'].value * doc['quantity'].value").
//	    Lang("painless")
//
//	// Stored script
//	storedScript := querydsl.NewScript("").
//	    Id("calculate_total").
//	    Params(map[string]any{
//	        "tax_rate": 0.08,
//	    })
//
// # Point in Time
//
// Use PointInTime for consistent search across changing data:
//
//	pit := querydsl.NewPointInTimeWithKeepAlive("pit-id-here", "5m")
//	searchSource := querydsl.NewSearchSource().
//	    Query(querydsl.NewMatchAllQuery()).
//	    Pit(pit)
package querydsl
