package querydsl

// TopHitsAggregation returns the top matching documents within each bucket,
// keeping track of the top documents as computed by the parent aggregation's
// grouping. Supports sorting, highlighting, source filtering, and all other
// search source options scoped to the bucket's documents.
//
// JSON output shape:
//
//	{"top_hits": {"size": 5, "sort": [{"date": {"order": "desc"}}]}}
type TopHitsAggregation struct {
	SearchSource *SearchSource
}

// NewTopHitsAggregation returns a new TopHitsAggregation with a default SearchSource.
func NewTopHitsAggregation() *TopHitsAggregation {
	return &TopHitsAggregation{SearchSource: NewSearchSource()}
}

func (a *TopHitsAggregation) SearchSourceBuilder(ss *SearchSource) *TopHitsAggregation {
	if ss == nil {
		ss = NewSearchSource()
	}
	a.SearchSource = ss
	return a
}

func (a *TopHitsAggregation) From(from int) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.From(from)
	return a
}

func (a *TopHitsAggregation) Size(size int) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.Size(size)
	return a
}

func (a *TopHitsAggregation) TrackScores(v bool) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.TrackScores(v)
	return a
}

func (a *TopHitsAggregation) Explain(v bool) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.Explain(v)
	return a
}

func (a *TopHitsAggregation) Version(v bool) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.Version(v)
	return a
}

func (a *TopHitsAggregation) NoStoredFields() *TopHitsAggregation {
	a.SearchSource = a.SearchSource.NoStoredFields()
	return a
}

func (a *TopHitsAggregation) FetchSource(v bool) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.FetchSource(v)
	return a
}

func (a *TopHitsAggregation) FetchSourceContext(ctx *FetchSourceContext) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.FetchSourceContext(ctx)
	return a
}

func (a *TopHitsAggregation) DocvalueFields(fields ...string) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.DocvalueFields(fields...)
	return a
}

func (a *TopHitsAggregation) DocvalueFieldsWithFormat(fields ...DocvalueField) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.DocvalueFieldsWithFormat(fields...)
	return a
}

func (a *TopHitsAggregation) DocvalueField(field string) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.DocvalueField(field)
	return a
}

func (a *TopHitsAggregation) DocvalueFieldWithFormat(field DocvalueField) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.DocvalueFieldWithFormat(field)
	return a
}

func (a *TopHitsAggregation) ScriptFields(fields ...*ScriptField) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.ScriptFields(fields...)
	return a
}

func (a *TopHitsAggregation) ScriptField(field *ScriptField) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.ScriptField(field)
	return a
}

func (a *TopHitsAggregation) Sort(field string, ascending bool) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.Sort(field, ascending)
	return a
}

func (a *TopHitsAggregation) SortWithInfo(info SortInfo) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.SortWithInfo(info)
	return a
}

func (a *TopHitsAggregation) SortBy(sorter ...Sorter) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.SortBy(sorter...)
	return a
}

func (a *TopHitsAggregation) Highlight(highlight *Highlight) *TopHitsAggregation {
	a.SearchSource = a.SearchSource.Highlight(highlight)
	return a
}

func (a *TopHitsAggregation) Highlighter() *Highlight {
	return a.SearchSource.Highlighter()
}

func (a *TopHitsAggregation) Source() (any, error) {
	src, err := a.SearchSource.Source()
	if err != nil {
		return nil, err
	}
	return map[string]any{"top_hits": src}, nil
}
