package querydsl

type PinnedQuery struct {
	IDs     []string
	Organic Query
}

func NewPinnedQuery() *PinnedQuery {
	return &PinnedQuery{}
}

// WithIDs sets the list of document IDs to pin at the top of results.
func (q *PinnedQuery) WithIDs(ids ...string) *PinnedQuery {
	q.IDs = ids
	return q
}

// WithOrganic sets the organic query whose results follow the pinned documents.
func (q *PinnedQuery) WithOrganic(organic Query) *PinnedQuery {
	q.Organic = organic
	return q
}

func (q PinnedQuery) Source() (any, error) {
	params := map[string]any{}
	if len(q.IDs) > 0 {
		params["ids"] = q.IDs
	}
	if q.Organic != nil {
		src, err := q.Organic.Source()
		if err != nil {
			return nil, err
		}
		params["organic"] = src
	}
	return map[string]any{"pinned": params}, nil
}
