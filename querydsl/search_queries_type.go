package querydsl

type TypeQuery struct {
	Value string `json:"value"`
}

func NewTypeQuery(typ string) *TypeQuery {
	return &TypeQuery{Value: typ}
}

func (q TypeQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	return map[string]any{"type": body}, nil
}
