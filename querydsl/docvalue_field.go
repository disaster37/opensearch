package querydsl

// DocvalueField represents a docvalue field, its name and
// its format (optional).
type DocvalueField struct {
	Field  string
	Format string
}

// Source serializes the DocvalueField into JSON.
func (d DocvalueField) Source() (any, error) {
	if d.Format == "" {
		return d.Field, nil
	}
	return map[string]any{
		"field":  d.Field,
		"format": d.Format,
	}, nil
}

// DocvalueFields is a slice of DocvalueField instances.
type DocvalueFields []DocvalueField

// Source serializes the DocvalueFields into JSON.
func (d DocvalueFields) Source() (any, error) {
	if d == nil {
		return nil, nil
	}
	v := make([]any, 0)
	for _, f := range d {
		src, _ := f.Source()
		v = append(v, src)
	}
	return v, nil
}
