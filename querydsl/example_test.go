package querydsl_test

import (
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/querydsl"
)

func ExampleBoolQuery() {
	q := querydsl.NewBoolQuery().
		Must(querydsl.NewTermQuery("title", "opensearch")).
		Should(querydsl.NewTermQuery("status", "published")).
		Filter(querydsl.NewRangeQuery("date").Gte("2024-01-01").Lte("2024-12-31")).
		MinimumShouldMatch("1")

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "bool": {
	//     "filter": {
	//       "range": {
	//         "date": {
	//           "from": "2024-01-01",
	//           "include_lower": true,
	//           "include_upper": true,
	//           "to": "2024-12-31"
	//         }
	//       }
	//     },
	//     "minimum_should_match": "1",
	//     "must": {
	//       "term": {
	//         "title": "opensearch"
	//       }
	//     },
	//     "should": {
	//       "term": {
	//         "status": "published"
	//       }
	//     }
	//   }
	// }
}

func ExampleMatchQuery() {
	q := querydsl.NewMatchQuery("message", "hello world")

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "match": {
	//     "message": "hello world"
	//   }
	// }
}

func ExampleMatchPhraseQuery() {
	q := querydsl.NewMatchPhraseQuery("message", "quick brown fox")

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "match_phrase": {
	//     "message": {
	//       "Analyzer": "",
	//       "Boost": null,
	//       "Field": "message",
	//       "Query": "quick brown fox",
	//       "QueryName": "",
	//       "Slop": null,
	//       "ZeroTermsQuery": ""
	//     }
	//   }
	// }
}

func ExampleRangeQuery() {
	q := querydsl.NewRangeQuery("timestamp").
		Gte("2024-01-01").
		Lte("2024-12-31")
	q.Format = "yyyy-MM-dd"

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "range": {
	//     "timestamp": {
	//       "format": "yyyy-MM-dd",
	//       "from": "2024-01-01",
	//       "include_lower": true,
	//       "include_upper": true,
	//       "to": "2024-12-31"
	//     }
	//   }
	// }
}

func ExampleNestedQuery() {
	inner := querydsl.NewTermQuery("comments.author", "john")
	q := querydsl.NewNestedQuery("comments", inner)
	q.ScoreMode = "avg"

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "nested": {
	//     "path": "comments",
	//     "query": {
	//       "term": {
	//         "comments.author": "john"
	//       }
	//     },
	//     "score_mode": "avg"
	//   }
	// }
}

func ExampleExistsQuery() {
	q := querydsl.NewExistsQuery("email")

	src, err := q.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "exists": {
	//     "field": "email"
	//   }
	// }
}

func ExampleTermsAggregation() {
	avgPrice := querydsl.AvgAggregation{Field: "price"}
	agg := querydsl.NewTermsAggregation().
		WithField("genre").
		WithSize(5).
		WithSubAggregation("avg_price", avgPrice)

	src, err := agg.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "terms": {
	//     "aggregations": {
	//       "avg_price": {
	//         "avg": {
	//           "field": "price"
	//         }
	//       }
	//     },
	//     "field": "genre",
	//     "size": 5
	//   }
	// }
}

func ExampleDateHistogramAggregation() {
	sumSales := querydsl.SumAggregation{Field: "sales"}
	agg := querydsl.NewDateHistogramAggregation()
	aggPtr := agg.
		Field_("timestamp").
		CalendarInterval_("month").
		TimeZone_("UTC").
		SubAggregation("total_sales", sumSales)

	src, err := aggPtr.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "date_histogram": {
	//     "aggregations": {
	//       "total_sales": {
	//         "sum": {
	//           "field": "sales"
	//         }
	//       }
	//     },
	//     "calendar_interval": "month",
	//     "field": "timestamp",
	//     "time_zone": "UTC"
	//   }
	// }
}

func ExampleRangeAggregation() {
	keyed := true
	agg := &querydsl.RangeAggregation{
		FieldVal: "price",
		Keyed:    &keyed,
	}
	agg = agg.
		AddUnboundedFromWithKey("cheap", 50).
		BetweenWithKey("moderate", 50, 100).
		AddUnboundedToWithKey("expensive", 100)

	src, err := agg.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "range": {
	//     "field": "price",
	//     "keyed": true,
	//     "ranges": [
	//       {
	//         "key": "cheap",
	//         "to": 50
	//       },
	//       {
	//         "from": 50,
	//         "key": "moderate",
	//         "to": 100
	//       },
	//       {
	//         "from": 100,
	//         "key": "expensive"
	//       }
	//     ]
	//   }
	// }
}

func ExampleFilterAggregation() {
	agg := querydsl.FilterAggregation{
		Filter: querydsl.NewTermQuery("status", "active"),
	}

	src, err := agg.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "filter": {
	//     "filter": {
	//       "term": {
	//         "status": "active"
	//       }
	//     }
	//   }
	// }
}

func ExampleSearchSource() {
	q := querydsl.NewBoolQuery().
		Must(querydsl.NewTermQuery("status", "published")).
		Filter(querydsl.NewRangeQuery("date").Gte("2024-01-01").Lte("2024-12-31"))

	genreAgg := querydsl.NewTermsAggregation().
		WithField("genre").
		WithSize(10)

	src, err := querydsl.NewSearchSource().
		Query(q).
		From(0).
		Size(20).
		SortBy(querydsl.NewFieldSort("timestamp").Desc()).
		Aggregation("genres", genreAgg).
		Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "aggregations": {
	//     "genres": {
	//       "terms": {
	//         "field": "genre",
	//         "size": 10
	//       }
	//     }
	//   },
	//   "from": 0,
	//   "query": {
	//     "bool": {
	//       "filter": {
	//         "range": {
	//           "date": {
	//             "from": "2024-01-01",
	//             "include_lower": true,
	//             "include_upper": true,
	//             "to": "2024-12-31"
	//           }
	//         }
	//       },
	//       "must": {
	//         "term": {
	//           "status": "published"
	//         }
	//       }
	//     }
	//   },
	//   "size": 20,
	//   "sort": [
	//     {
	//       "timestamp": {
	//         "order": "desc"
	//       }
	//     }
	//   ]
	// }
}

func ExampleScript() {
	s := querydsl.NewScript("doc['price'].value * params.factor").
		Param("factor", 1.1).
		Lang("painless")

	src, err := s.Source()
	if err != nil {
		panic(err)
	}
	data, _ := json.MarshalIndent(src, "", "  ")
	fmt.Println(string(data))
	// Output:
	// {
	//   "lang": "painless",
	//   "params": {
	//     "factor": 1.1
	//   },
	//   "source": "doc['price'].value * params.factor"
	// }
}
