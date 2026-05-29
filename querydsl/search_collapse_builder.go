package querydsl

// CollapseBuilder enables field collapsing on a search request, grouping
// results by a field value. It optionally supports expanding collapsed
// groups with [InnerHit] and limiting concurrent group requests.
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-collapse.html
// for details.
//
// Typical usage:
//
//	collapse := querydsl.NewCollapseBuilder("user").
//	    InnerHit(querydsl.NewInnerHit().Name("last_tweets").Size(5))
type CollapseBuilder struct {
	field                      string
	innerHits                  []*InnerHit
	maxConcurrentGroupRequests *int
}

// NewCollapseBuilder creates a new CollapseBuilder.
func NewCollapseBuilder(field string) *CollapseBuilder {
	return &CollapseBuilder{field: field}
}

// Field to collapse.
func (b *CollapseBuilder) Field(field string) *CollapseBuilder {
	b.field = field
	return b
}

// InnerHit option to expand the collapsed results.
func (b *CollapseBuilder) InnerHit(innerHits ...*InnerHit) *CollapseBuilder {
	b.innerHits = append(b.innerHits, innerHits...)
	return b
}

// MaxConcurrentGroupRequests is the maximum number of group requests that are
// allowed to be ran concurrently in the inner_hits phase.
func (b *CollapseBuilder) MaxConcurrentGroupRequests(max int) *CollapseBuilder {
	b.maxConcurrentGroupRequests = &max
	return b
}

// Source generates the JSON serializable fragment for the CollapseBuilder.
func (b *CollapseBuilder) Source() (any, error) {
	// {
	//   "field": "user",
	//   "inner_hits": [{
	//     "name": "last_tweets",
	//     "size": 5,
	//     "sort": [{ "date": "asc" }]
	//   }],
	//   "max_concurrent_group_searches": 4
	// }
	src := map[string]any{
		"field": b.field,
	}

	if len(b.innerHits) > 0 {
		var innerHits []any
		for _, h := range b.innerHits {
			hits, err := h.Source()
			if err != nil {
				return nil, err
			}
			innerHits = append(innerHits, hits)
		}
		src["inner_hits"] = innerHits
	}

	if b.maxConcurrentGroupRequests != nil {
		src["max_concurrent_group_searches"] = *b.maxConcurrentGroupRequests
	}

	return src, nil
}
