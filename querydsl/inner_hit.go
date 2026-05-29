package querydsl

// InnerHit implements a simple join for parent/child, nested, and even
// top-level documents in Opensearch.
// It is an experimental feature for Opensearch versions 1.5 (or greater).
// See http://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-inner-hits.html
// for documentation.
//
// See the tests for SearchSource, HasChildFilter, HasChildQuery,
// HasParentFilter, HasParentQuery, NestedFilter, and NestedQuery
// for usage examples.
type InnerHit struct {
	source *SearchSource
	path   string
	typ    string

	name string
}

// NewInnerHit creates a new InnerHit.
func NewInnerHit() *InnerHit {
	return &InnerHit{source: NewSearchSource()}
}

// Path sets the path to the nested object for this inner hit.
func (hit *InnerHit) Path(path string) *InnerHit {
	hit.path = path
	return hit
}

// Type sets the child or parent type for this inner hit in a parent/child
// relationship.
func (hit *InnerHit) Type(typ string) *InnerHit {
	hit.typ = typ
	return hit
}

// Query sets the query to filter inner hits.
func (hit *InnerHit) Query(query Query) *InnerHit {
	hit.source.Query(query)
	return hit
}

// Collapse adds field collapsing to the inner hits.
func (hit *InnerHit) Collapse(collapse *CollapseBuilder) *InnerHit {
	hit.source.Collapse(collapse)
	return hit
}

// From sets the starting offset for inner hits.
func (hit *InnerHit) From(from int) *InnerHit {
	hit.source.From(from)
	return hit
}

// Size sets the maximum number of inner hits to return.
func (hit *InnerHit) Size(size int) *InnerHit {
	hit.source.Size(size)
	return hit
}

// TrackScores controls whether scores are calculated for inner hits.
func (hit *InnerHit) TrackScores(trackScores bool) *InnerHit {
	hit.source.TrackScores(trackScores)
	return hit
}

// Explain controls whether an explanation of the scoring is returned
// with each inner hit.
func (hit *InnerHit) Explain(explain bool) *InnerHit {
	hit.source.Explain(explain)
	return hit
}

// Version controls whether each inner hit is returned with its version.
func (hit *InnerHit) Version(version bool) *InnerHit {
	hit.source.Version(version)
	return hit
}

// StoredField adds a single stored field to return with each inner hit.
func (hit *InnerHit) StoredField(storedFieldName string) *InnerHit {
	hit.source.StoredField(storedFieldName)
	return hit
}

// StoredFields adds stored fields to return with each inner hit.
func (hit *InnerHit) StoredFields(storedFieldNames ...string) *InnerHit {
	hit.source.StoredFields(storedFieldNames...)
	return hit
}

// NoStoredFields disables loading of stored fields for inner hits.
func (hit *InnerHit) NoStoredFields() *InnerHit {
	hit.source.NoStoredFields()
	return hit
}

// FetchSource controls whether the _source is returned with each inner hit.
func (hit *InnerHit) FetchSource(fetchSource bool) *InnerHit {
	hit.source.FetchSource(fetchSource)
	return hit
}

// FetchSourceContext configures how the _source is fetched for inner hits.
func (hit *InnerHit) FetchSourceContext(fetchSourceContext *FetchSourceContext) *InnerHit {
	hit.source.FetchSourceContext(fetchSourceContext)
	return hit
}

// DocvalueFields adds field data cache fields to return with each inner hit.
func (hit *InnerHit) DocvalueFields(docvalueFields ...string) *InnerHit {
	hit.source.DocvalueFields(docvalueFields...)
	return hit
}

// DocvalueFieldsWithFormat adds docvalue fields with format to return
// with each inner hit.
func (hit *InnerHit) DocvalueFieldsWithFormat(docvalueFields ...DocvalueField) *InnerHit {
	hit.source.DocvalueFieldsWithFormat(docvalueFields...)
	return hit
}

// DocvalueField adds a single docvalue field to return with each inner hit.
func (hit *InnerHit) DocvalueField(docvalueField string) *InnerHit {
	hit.source.DocvalueField(docvalueField)
	return hit
}

// DocvalueFieldWithFormat adds a single docvalue field with format to return
// with each inner hit.
func (hit *InnerHit) DocvalueFieldWithFormat(docvalueField DocvalueField) *InnerHit {
	hit.source.DocvalueFieldWithFormat(docvalueField)
	return hit
}

// ScriptFields adds script-computed fields to return with each inner hit.
func (hit *InnerHit) ScriptFields(scriptFields ...*ScriptField) *InnerHit {
	hit.source.ScriptFields(scriptFields...)
	return hit
}

// ScriptField adds a single script-computed field to return with each inner hit.
func (hit *InnerHit) ScriptField(scriptField *ScriptField) *InnerHit {
	hit.source.ScriptField(scriptField)
	return hit
}

// Sort adds a simple field sort order to inner hits.
func (hit *InnerHit) Sort(field string, ascending bool) *InnerHit {
	hit.source.Sort(field, ascending)
	return hit
}

// SortWithInfo adds a sort order to inner hits using a [SortInfo].
func (hit *InnerHit) SortWithInfo(info SortInfo) *InnerHit {
	hit.source.SortWithInfo(info)
	return hit
}

// SortBy adds one or more sort strategies to inner hits.
func (hit *InnerHit) SortBy(sorter ...Sorter) *InnerHit {
	hit.source.SortBy(sorter...)
	return hit
}

// Highlight adds highlighting to inner hits.
func (hit *InnerHit) Highlight(highlight *Highlight) *InnerHit {
	hit.source.Highlight(highlight)
	return hit
}

// Highlighter returns the [Highlight] for inner hits, creating one if
// it does not already exist.
func (hit *InnerHit) Highlighter() *Highlight {
	return hit.source.Highlighter()
}

// Name assigns a name to this inner hit for identification in the response.
func (hit *InnerHit) Name(name string) *InnerHit {
	hit.name = name
	return hit
}

func (hit *InnerHit) Source() (any, error) {
	src, err := hit.source.Source()
	if err != nil {
		return nil, err
	}
	source, ok := src.(map[string]any)
	if !ok {
		return nil, nil
	}

	// Notice that hit.typ and hit.path are not exported here.
	// They are only used with SearchSource and serialized there.

	if hit.name != "" {
		source["name"] = hit.name
	}
	return source, nil
}
