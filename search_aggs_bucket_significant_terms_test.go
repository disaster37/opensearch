// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestSignificantTermsAggregation(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithArgs(t *testing.T) {
	agg := NewSignificantTermsAggregation().
		Field("crime_type").
		ExecutionHint("map").
		ShardSize(5).
		MinDocCount(10).
		BackgroundFilter(NewTermQuery("city", "London"))
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"background_filter":{"term":{"city":"London"}},"execution_hint":"map","field":"crime_type","min_doc_count":10,"shard_size":5}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithIncludeExclude(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type").Include(".*sport.*").Exclude("water_.*")
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"exclude":"water_.*","field":"crime_type","include":".*sport.*"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithIncludeExcludeValues(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type").IncludeValues("mazda", "honda").ExcludeValues("rover", "jensen")
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"exclude":["rover","jensen"],"field":"crime_type","include":["mazda","honda"]}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithPartitions(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("account_id").Partition(0).NumPartitions(20)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"account_id","include":{"num_partitions":20,"partition":0}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationSubAggregation(t *testing.T) {
	crimeTypesAgg := NewSignificantTermsAggregation().Field("crime_type")
	agg := NewTermsAggregation().Field("force")
	agg = agg.SubAggregation("significantCrimeTypes", crimeTypesAgg)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"aggregations":{"significantCrimeTypes":{"significant_terms":{"field":"crime_type"}}},"terms":{"field":"force"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithMetaData(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.Meta(map[string]interface{}{"name": "Oliver"})
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"meta":{"name":"Oliver"},"significant_terms":{"field":"crime_type"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithChiSquare(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewChiSquareSignificanceHeuristic().
			BackgroundIsSuperset(true).
			IncludeNegatives(false),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"chi_square":{"background_is_superset":true,"include_negatives":false},"field":"crime_type"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithGND(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewGNDSignificanceHeuristic(),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type","gnd":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithJLH(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewJLHScoreSignificanceHeuristic(),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type","jlh":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithMutualInformation(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewMutualInformationSignificanceHeuristic().
			BackgroundIsSuperset(false).
			IncludeNegatives(true),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type","mutual_information":{"background_is_superset":false,"include_negatives":true}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithPercentageScore(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewPercentageScoreSignificanceHeuristic(),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type","percentage":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSignificantTermsAggregationWithScript(t *testing.T) {
	agg := NewSignificantTermsAggregation().Field("crime_type")
	agg = agg.SignificanceHeuristic(
		NewScriptSignificanceHeuristic().
			Script(NewScript("_subset_freq/(_superset_freq - _subset_freq + 1)")),
	)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"significant_terms":{"field":"crime_type","script_heuristic":{"script":{"source":"_subset_freq/(_superset_freq - _subset_freq + 1)"}}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
