package querydsl

// FieldField represents a field, its name and
// its format (optional).
type FieldField struct {
	Field  string
	Format string
}

// Source serializes the FieldField into JSON.
func (d FieldField) Source() (any, error) {
	if d.Format == "" {
		return d.Field, nil
	}
	return map[string]any{
		"field":  d.Field,
		"format": d.Format,
	}, nil
}

// FieldFields is a slice of FieldField instances.
type FieldFields []FieldField

// Source serializes the FieldFields into JSON.
func (d FieldFields) Source() (any, error) {
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
