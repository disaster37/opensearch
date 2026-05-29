package querydsl

type SliceQuery struct {
	Field string `json:"field,omitempty"`
	ID    *int   `json:"id,omitempty"`
	Max   *int   `json:"max,omitempty"`
}

func NewSliceQuery() SliceQuery {
	return SliceQuery{}
}

func (q SliceQuery) Source() (any, error) {
	return marshalStruct(q)
}
