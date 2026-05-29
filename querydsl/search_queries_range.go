package querydsl

// RangeQuery matches documents with values that fall within a given range.
// It supports numeric, date, and string field types. The JSON DSL output is:
//
//	{"range": {"field": {"from": ..., "to": ..., "include_lower": true, ...}}}
//
// Use NewRangeQuery(field) to create a query and then chain Gt, Gte, Lt,
// Lte calls to specify the bounds.
type RangeQuery struct {
	Field        string
	From         any      `json:"from,omitempty"`
	To           any      `json:"to,omitempty"`
	IncludeLower bool     `json:"-"`
	IncludeUpper bool     `json:"-"`
	TimeZone     string   `json:"time_zone,omitempty"`
	Boost        *float64 `json:"boost,omitempty"`
	QueryName    string   `json:"_name,omitempty"`
	Format       string   `json:"format,omitempty"`
	Relation     string   `json:"relation,omitempty"`
}

// NewRangeQuery creates a RangeQuery for the given field with inclusive
// lower and upper bounds by default (include_lower=true, include_upper=true).
func NewRangeQuery(field string) RangeQuery {
	return RangeQuery{Field: field, IncludeLower: true, IncludeUpper: true}
}

// Gt sets the lower bound of the range to be exclusive (from, exclusive).
func (q RangeQuery) Gt(from any) RangeQuery {
	q.From = from
	q.IncludeLower = false
	return q
}

// Gte sets the lower bound of the range to be inclusive (from, inclusive).
func (q RangeQuery) Gte(from any) RangeQuery {
	q.From = from
	q.IncludeLower = true
	return q
}

// Lt sets the upper bound of the range to be exclusive (to, exclusive).
func (q RangeQuery) Lt(to any) RangeQuery {
	q.To = to
	q.IncludeUpper = false
	return q
}

// Lte sets the upper bound of the range to be inclusive (to, inclusive).
func (q RangeQuery) Lte(to any) RangeQuery {
	q.To = to
	q.IncludeUpper = true
	return q
}

func (q RangeQuery) Source() (any, error) {
	params := map[string]any{
		"from":          q.From,
		"to":            q.To,
		"include_lower": q.IncludeLower,
		"include_upper": q.IncludeUpper,
	}
	if q.TimeZone != "" {
		params["time_zone"] = q.TimeZone
	}
	if q.Format != "" {
		params["format"] = q.Format
	}
	if q.Relation != "" {
		params["relation"] = q.Relation
	}
	if q.Boost != nil {
		params["boost"] = *q.Boost
	}
	body := map[string]any{q.Field: params}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	return map[string]any{"range": body}, nil
}
