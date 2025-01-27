// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import "errors"

// -- Sorter --

// Sorter is an interface for sorting strategies, e.g. ScoreSort or FieldSort.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-sort.html.
type Sorter interface {
	Source() (interface{}, error)
}

// -- SortInfo --

// SortInfo contains information about sorting a field.
type SortInfo struct {
	Sorter
	Field          string
	Ascending      bool
	Missing        interface{}
	IgnoreUnmapped *bool
	UnmappedType   string
	SortMode       string
	NestedFilter   Query // deprecated in 6.1 and replaced by Filter
	Filter         Query
	NestedPath     string // deprecated in 6.1 and replaced by Path
	Path           string
	NestedSort     *NestedSort // deprecated in 6.1 and replaced by Nested
	Nested         *NestedSort
}

func (info SortInfo) Source() (interface{}, error) {
	prop := make(map[string]interface{})
	if info.Ascending {
		prop["order"] = "asc"
	} else {
		prop["order"] = "desc"
	}
	if info.Missing != nil {
		prop["missing"] = info.Missing
	}
	if info.IgnoreUnmapped != nil {
		prop["ignore_unmapped"] = *info.IgnoreUnmapped
	}
	if info.UnmappedType != "" {
		prop["unmapped_type"] = info.UnmappedType
	}
	if info.SortMode != "" {
		prop["mode"] = info.SortMode
	}
	if info.Filter != nil {
		src, err := info.Filter.Source()
		if err != nil {
			return nil, err
		}
		prop["filter"] = src
	} else if info.NestedFilter != nil {
		src, err := info.NestedFilter.Source()
		if err != nil {
			return nil, err
		}
		prop["nested_filter"] = src // deprecated in 6.1
	}
	if info.Path != "" {
		prop["path"] = info.Path
	} else if info.NestedPath != "" {
		prop["nested_path"] = info.NestedPath // deprecated in 6.1
	}
	if info.Nested != nil {
		src, err := info.Nested.Source()
		if err != nil {
			return nil, err
		}
		prop["nested"] = src
	} else if info.NestedSort != nil {
		src, err := info.NestedSort.Source()
		if err != nil {
			return nil, err
		}
		prop["nested"] = src
	}
	source := make(map[string]interface{})
	source[info.Field] = prop
	return source, nil
}

// -- SortByDoc --

// SortByDoc sorts by the "_doc" field, as described in
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-scroll.html.
//
// Example:
//
//	ss := opensearch.NewSearchSource()
//	ss = ss.SortBy(opensearch.SortByDoc{})
type SortByDoc struct {
	Sorter
}

// Source returns the JSON-serializable data.
func (s SortByDoc) Source() (interface{}, error) {
	return "_doc", nil
}

// -- ScoreSort --

// ScoreSort sorts by relevancy score.
type ScoreSort struct {
	Sorter
	ascending bool
}

// NewScoreSort creates a new ScoreSort.
func NewScoreSort() *ScoreSort {
	return &ScoreSort{ascending: false} // Descending by default!
}

// Order defines whether sorting ascending (default) or descending.
func (s *ScoreSort) Order(ascending bool) *ScoreSort {
	s.ascending = ascending
	return s
}

// Asc sets ascending sort order.
func (s *ScoreSort) Asc() *ScoreSort {
	s.ascending = true
	return s
}

// Desc sets descending sort order.
func (s *ScoreSort) Desc() *ScoreSort {
	s.ascending = false
	return s
}

// Source returns the JSON-serializable data.
func (s *ScoreSort) Source() (interface{}, error) {
	source := make(map[string]interface{})
	x := make(map[string]interface{})
	source["_score"] = x
	if s.ascending {
		x["order"] = "asc"
	} else {
		x["order"] = "desc"
	}
	return source, nil
}

// -- FieldSort --

// FieldSort sorts by a given field.
type FieldSort struct {
	Sorter
	fieldName    string
	ascending    bool
	missing      interface{}
	unmappedType *string
	sortMode     *string
	filter       Query
	path         *string
	nested       *NestedSort
}

// NewFieldSort creates a new FieldSort.
func NewFieldSort(fieldName string) *FieldSort {
	return &FieldSort{
		fieldName: fieldName,
		ascending: true,
	}
}

// FieldName specifies the name of the field to be used for sorting.
func (s *FieldSort) FieldName(fieldName string) *FieldSort {
	s.fieldName = fieldName
	return s
}

// Order defines whether sorting ascending (default) or descending.
func (s *FieldSort) Order(ascending bool) *FieldSort {
	s.ascending = ascending
	return s
}

// Asc sets ascending sort order.
func (s *FieldSort) Asc() *FieldSort {
	s.ascending = true
	return s
}

// Desc sets descending sort order.
func (s *FieldSort) Desc() *FieldSort {
	s.ascending = false
	return s
}

// Missing sets the value to be used when a field is missing in a document.
// You can also use "_last" or "_first" to sort missing last or first
// respectively.
func (s *FieldSort) Missing(missing interface{}) *FieldSort {
	s.missing = missing
	return s
}

// UnmappedType sets the type to use when the current field is not mapped
// in an index.
func (s *FieldSort) UnmappedType(typ string) *FieldSort {
	s.unmappedType = &typ
	return s
}

// SortMode specifies what values to pick in case a document contains
// multiple values for the targeted sort field. Possible values are:
// min, max, sum, and avg.
func (s *FieldSort) SortMode(sortMode string) *FieldSort {
	s.sortMode = &sortMode
	return s
}

// NestedFilter sets a filter that nested objects should match with
// in order to be taken into account for sorting.
// Deprecated: Use Filter instead.
func (s *FieldSort) NestedFilter(nestedFilter Query) *FieldSort {
	s.filter = nestedFilter
	return s
}

// Filter sets a filter that nested objects should match with
// in order to be taken into account for sorting.
func (s *FieldSort) Filter(filter Query) *FieldSort {
	s.filter = filter
	return s
}

// NestedPath is used if sorting occurs on a field that is inside a
// nested object.
// Deprecated: Use Path instead.
func (s *FieldSort) NestedPath(nestedPath string) *FieldSort {
	s.path = &nestedPath
	return s
}

// Path is used if sorting occurs on a field that is inside a
// nested object.
func (s *FieldSort) Path(path string) *FieldSort {
	s.path = &path
	return s
}

// NestedSort is available starting with 6.1 and will replace NestedFilter
// and NestedPath.
// Deprecated: Use Nested instead.
func (s *FieldSort) NestedSort(nestedSort *NestedSort) *FieldSort {
	s.nested = nestedSort
	return s
}

// Nested is available starting with 6.1 and will replace Filter and Path.
func (s *FieldSort) Nested(nested *NestedSort) *FieldSort {
	s.nested = nested
	return s
}

// Source returns the JSON-serializable data.
func (s *FieldSort) Source() (interface{}, error) {
	source := make(map[string]interface{})
	x := make(map[string]interface{})
	source[s.fieldName] = x
	if s.ascending {
		x["order"] = "asc"
	} else {
		x["order"] = "desc"
	}
	if s.missing != nil {
		x["missing"] = s.missing
	}
	if s.unmappedType != nil {
		x["unmapped_type"] = *s.unmappedType
	}
	if s.sortMode != nil {
		x["mode"] = *s.sortMode
	}
	if s.filter != nil {
		src, err := s.filter.Source()
		if err != nil {
			return nil, err
		}
		x["filter"] = src
	}
	if s.path != nil {
		x["path"] = *s.path
	}
	if s.nested != nil {
		src, err := s.nested.Source()
		if err != nil {
			return nil, err
		}
		x["nested"] = src
	}
	return source, nil
}

// -- GeoDistanceSort --

// GeoDistanceSort allows for sorting by geographic distance.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-sort.html#_geo_distance_sorting.
type GeoDistanceSort struct {
	Sorter
	fieldName      string
	points         []*GeoPoint
	geohashes      []string
	distanceType   *string
	unit           string
	ignoreUnmapped *bool
	ascending      bool
	sortMode       *string
	nestedFilter   Query
	nestedPath     *string
	nestedSort     *NestedSort
}

// NewGeoDistanceSort creates a new sorter for geo distances.
func NewGeoDistanceSort(fieldName string) *GeoDistanceSort {
	return &GeoDistanceSort{
		fieldName: fieldName,
		ascending: true,
	}
}

// FieldName specifies the name of the (geo) field to use for sorting.
func (s *GeoDistanceSort) FieldName(fieldName string) *GeoDistanceSort {
	s.fieldName = fieldName
	return s
}

// Order defines whether sorting ascending (default) or descending.
func (s *GeoDistanceSort) Order(ascending bool) *GeoDistanceSort {
	s.ascending = ascending
	return s
}

// Asc sets ascending sort order.
func (s *GeoDistanceSort) Asc() *GeoDistanceSort {
	s.ascending = true
	return s
}

// Desc sets descending sort order.
func (s *GeoDistanceSort) Desc() *GeoDistanceSort {
	s.ascending = false
	return s
}

// Point specifies a point to create the range distance aggregations from.
func (s *GeoDistanceSort) Point(lat, lon float64) *GeoDistanceSort {
	s.points = append(s.points, GeoPointFromLatLon(lat, lon))
	return s
}

// Points specifies the geo point(s) to create the range distance aggregations from.
func (s *GeoDistanceSort) Points(points ...*GeoPoint) *GeoDistanceSort {
	s.points = append(s.points, points...)
	return s
}

// GeoHashes specifies the geo point to create the range distance aggregations from.
func (s *GeoDistanceSort) GeoHashes(geohashes ...string) *GeoDistanceSort {
	s.geohashes = append(s.geohashes, geohashes...)
	return s
}

// Unit specifies the distance unit to use. It defaults to km.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/common-options.html#distance-units
// for details.
func (s *GeoDistanceSort) Unit(unit string) *GeoDistanceSort {
	s.unit = unit
	return s
}

// IgnoreUnmapped indicates whether the unmapped field should be treated as
// a missing value. Setting it to true is equivalent to specifying an
// unmapped_type in the field sort. The default is false (unmapped field
// causes the search to fail).
func (s *GeoDistanceSort) IgnoreUnmapped(ignoreUnmapped bool) *GeoDistanceSort {
	s.ignoreUnmapped = &ignoreUnmapped
	return s
}

// GeoDistance is an alias for DistanceType.
func (s *GeoDistanceSort) GeoDistance(geoDistance string) *GeoDistanceSort {
	return s.DistanceType(geoDistance)
}

// DistanceType describes how to compute the distance, e.g. "arc" or "plane".
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-sort.html#geo-sorting
// for details.
func (s *GeoDistanceSort) DistanceType(distanceType string) *GeoDistanceSort {
	s.distanceType = &distanceType
	return s
}

// SortMode specifies what values to pick in case a document contains
// multiple values for the targeted sort field. Possible values are:
// min, max, sum, and avg.
func (s *GeoDistanceSort) SortMode(sortMode string) *GeoDistanceSort {
	s.sortMode = &sortMode
	return s
}

// NestedFilter sets a filter that nested objects should match with
// in order to be taken into account for sorting.
func (s *GeoDistanceSort) NestedFilter(nestedFilter Query) *GeoDistanceSort {
	s.nestedFilter = nestedFilter
	return s
}

// NestedPath is used if sorting occurs on a field that is inside a
// nested object.
func (s *GeoDistanceSort) NestedPath(nestedPath string) *GeoDistanceSort {
	s.nestedPath = &nestedPath
	return s
}

// NestedSort is available starting with 6.1 and will replace NestedFilter
// and NestedPath.
func (s *GeoDistanceSort) NestedSort(nestedSort *NestedSort) *GeoDistanceSort {
	s.nestedSort = nestedSort
	return s
}

// Source returns the JSON-serializable data.
func (s *GeoDistanceSort) Source() (interface{}, error) {
	source := make(map[string]interface{})
	x := make(map[string]interface{})
	source["_geo_distance"] = x

	// Points
	var ptarr []interface{}
	for _, pt := range s.points {
		ptarr = append(ptarr, pt.Source())
	}
	for _, geohash := range s.geohashes {
		ptarr = append(ptarr, geohash)
	}
	x[s.fieldName] = ptarr

	if s.unit != "" {
		x["unit"] = s.unit
	}
	if s.ignoreUnmapped != nil {
		x["ignore_unmapped"] = *s.ignoreUnmapped
	}
	if s.distanceType != nil {
		x["distance_type"] = *s.distanceType
	}

	if s.ascending {
		x["order"] = "asc"
	} else {
		x["order"] = "desc"
	}
	if s.sortMode != nil {
		x["mode"] = *s.sortMode
	}
	if s.nestedFilter != nil {
		src, err := s.nestedFilter.Source()
		if err != nil {
			return nil, err
		}
		x["nested_filter"] = src
	}
	if s.nestedPath != nil {
		x["nested_path"] = *s.nestedPath
	}
	if s.nestedSort != nil {
		src, err := s.nestedSort.Source()
		if err != nil {
			return nil, err
		}
		x["nested"] = src
	}
	return source, nil
}

// -- ScriptSort --

// ScriptSort sorts by a custom script. See
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/modules-scripting.html#modules-scripting
// for details about scripting.
type ScriptSort struct {
	Sorter
	script       *Script
	typ          string
	ascending    bool
	sortMode     *string
	nestedFilter Query
	nestedPath   *string
	nestedSort   *NestedSort
}

// NewScriptSort creates and initializes a new ScriptSort.
// You must provide a script and a type, e.g. "string" or "number".
func NewScriptSort(script *Script, typ string) *ScriptSort {
	return &ScriptSort{
		script:    script,
		typ:       typ,
		ascending: true,
	}
}

// Type sets the script type, which can be either "string" or "number".
func (s *ScriptSort) Type(typ string) *ScriptSort {
	s.typ = typ
	return s
}

// Order defines whether sorting ascending (default) or descending.
func (s *ScriptSort) Order(ascending bool) *ScriptSort {
	s.ascending = ascending
	return s
}

// Asc sets ascending sort order.
func (s *ScriptSort) Asc() *ScriptSort {
	s.ascending = true
	return s
}

// Desc sets descending sort order.
func (s *ScriptSort) Desc() *ScriptSort {
	s.ascending = false
	return s
}

// SortMode specifies what values to pick in case a document contains
// multiple values for the targeted sort field. Possible values are:
// min or max.
func (s *ScriptSort) SortMode(sortMode string) *ScriptSort {
	s.sortMode = &sortMode
	return s
}

// NestedFilter sets a filter that nested objects should match with
// in order to be taken into account for sorting.
func (s *ScriptSort) NestedFilter(nestedFilter Query) *ScriptSort {
	s.nestedFilter = nestedFilter
	return s
}

// NestedPath is used if sorting occurs on a field that is inside a
// nested object.
func (s *ScriptSort) NestedPath(nestedPath string) *ScriptSort {
	s.nestedPath = &nestedPath
	return s
}

// NestedSort is available starting with 6.1 and will replace NestedFilter
// and NestedPath.
func (s *ScriptSort) NestedSort(nestedSort *NestedSort) *ScriptSort {
	s.nestedSort = nestedSort
	return s
}

// Source returns the JSON-serializable data.
func (s *ScriptSort) Source() (interface{}, error) {
	if s.script == nil {
		return nil, errors.New("ScriptSort expected a script")
	}
	source := make(map[string]interface{})
	x := make(map[string]interface{})
	source["_script"] = x

	src, err := s.script.Source()
	if err != nil {
		return nil, err
	}
	x["script"] = src

	x["type"] = s.typ

	if s.ascending {
		x["order"] = "asc"
	} else {
		x["order"] = "desc"
	}
	if s.sortMode != nil {
		x["mode"] = *s.sortMode
	}
	if s.nestedFilter != nil {
		src, err := s.nestedFilter.Source()
		if err != nil {
			return nil, err
		}
		x["nested_filter"] = src
	}
	if s.nestedPath != nil {
		x["nested_path"] = *s.nestedPath
	}
	if s.nestedSort != nil {
		src, err := s.nestedSort.Source()
		if err != nil {
			return nil, err
		}
		x["nested"] = src
	}
	return source, nil
}

// -- NestedSort --

// NestedSort is used for fields that are inside a nested object.
// It takes a "path" argument and an optional nested filter that the
// nested objects should match with in order to be taken into account
// for sorting.
//
// NestedSort is available from 6.1 and replaces nestedFilter and nestedPath
// in the other sorters.
type NestedSort struct {
	Sorter
	path       string
	filter     Query
	nestedSort *NestedSort
}

// NewNestedSort creates a new NestedSort.
func NewNestedSort(path string) *NestedSort {
	return &NestedSort{path: path}
}

// Filter sets the filter.
func (s *NestedSort) Filter(filter Query) *NestedSort {
	s.filter = filter
	return s
}

// NestedSort embeds another level of nested sorting.
func (s *NestedSort) NestedSort(nestedSort *NestedSort) *NestedSort {
	s.nestedSort = nestedSort
	return s
}

// Source returns the JSON-serializable data.
func (s *NestedSort) Source() (interface{}, error) {
	source := make(map[string]interface{})

	if s.path != "" {
		source["path"] = s.path
	}
	if s.filter != nil {
		src, err := s.filter.Source()
		if err != nil {
			return nil, err
		}
		source["filter"] = src
	}
	if s.nestedSort != nil {
		src, err := s.nestedSort.Source()
		if err != nil {
			return nil, err
		}
		source["nested"] = src
	}

	return source, nil
}
