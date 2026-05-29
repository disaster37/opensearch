package querydsl

import json "github.com/goccy/go-json"

type RawStringQuery string

func NewRawStringQuery(q string) RawStringQuery {
	return RawStringQuery(q)
}

func (q RawStringQuery) Source() (any, error) {
	var f any
	err := json.Unmarshal([]byte(q), &f)
	return f, err
}
