package querydsl

type WrapperQuery struct {
	Query string `json:"query"`
}

func NewWrapperQuery(source string) *WrapperQuery {
	return &WrapperQuery{Query: source}
}

func (q WrapperQuery) Source() (any, error) {
	return map[string]any{"wrapper": q}, nil
}
