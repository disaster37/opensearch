package querydsl

// ParentIdQuery matches child documents that belong to a specific parent
// document identified by its ID. It corresponds to the OpenSearch parent_id
// query in JSON DSL:
//
//	{"parent_id": {"type": "answer", "id": "1"}}
//
// Key parameters:
//   - Type: the child document type (join relation name).
//   - ID: the ID of the parent document.
//   - IgnoreUnmapped: when true, unmapped types are skipped instead of
//     failing the query.
//   - InnerHit: optional [InnerHit] to return matched child document details.
//
// Use NewParentIdQuery(typ, id) to create an instance.
type ParentIdQuery struct {
	Type           string    `json:"type"`
	ID             string    `json:"id"`
	IgnoreUnmapped *bool     `json:"ignore_unmapped,omitempty"`
	Boost          *float64  `json:"boost,omitempty"`
	QueryName      string    `json:"_name,omitempty"`
	InnerHit       *InnerHit `json:"-"`
}

// NewParentIdQuery creates a ParentIdQuery for the given child type and parent ID.
func NewParentIdQuery(typ, id string) ParentIdQuery {
	return ParentIdQuery{Type: typ, ID: id}
}

func (q ParentIdQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	if q.InnerHit != nil {
		src, err := q.InnerHit.Source()
		if err != nil {
			return nil, err
		}
		m["inner_hits"] = src
	}
	return map[string]any{"parent_id": m}, nil
}
