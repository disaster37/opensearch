package querydsl

type PinnedQuery struct {
	IDs     []string
	Organic Query
}

func NewPinnedQuery() PinnedQuery {
	return PinnedQuery{}
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
