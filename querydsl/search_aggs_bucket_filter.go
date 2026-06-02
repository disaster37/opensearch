package querydsl

// FilterAggregation produces a single bucket of documents that match a query.
// All documents passing the filter land in one bucket; documents that do not
// match are excluded. Sub-aggregations are computed only on the filtered set.
//
// Typical use: "show stats only for documents matching status=active".
//
// JSON output shape:
//
//	{"filter": {"term": {"status": "active"}}}
type FilterAggregation struct {
	Filter  Query
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewFilterAggregation returns a zero-value FilterAggregation.
func NewFilterAggregation() *FilterAggregation { return &FilterAggregation{} }

// WithFilter sets the query filter for this aggregation.
func (a *FilterAggregation) WithFilter(filter Query) *FilterAggregation {
	a.Filter = filter
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *FilterAggregation) WithSubAggregation(name string, sub Aggregation) *FilterAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *FilterAggregation) WithMeta(meta map[string]any) *FilterAggregation {
	a.Meta = meta
	return a
}

func (a FilterAggregation) Source() (any, error) {
	src, err := a.Filter.Source()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"filter": src}
	return sourceAgg("filter", body, a.SubAggs, a.Meta, nil)
}
