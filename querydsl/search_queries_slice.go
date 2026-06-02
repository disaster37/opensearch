package querydsl

type SliceQuery struct {
	Field string `json:"field,omitempty"`
	ID    *int   `json:"id,omitempty"`
	Max   *int   `json:"max,omitempty"`
}

func NewSliceQuery() *SliceQuery {
	return &SliceQuery{}
}

// WithField sets the field to use for slicing (defaults to _id or _uid).
func (q *SliceQuery) WithField(field string) *SliceQuery {
	q.Field = field
	return q
}

// WithID sets the slice ID for this shard.
func (q *SliceQuery) WithID(id int) *SliceQuery {
	q.ID = &id
	return q
}

// WithMax sets the total number of slices.
func (q *SliceQuery) WithMax(max int) *SliceQuery {
	q.Max = &max
	return q
}

func (q SliceQuery) Source() (any, error) {
	return marshalStruct(q)
}
