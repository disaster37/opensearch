// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// FieldField represents a field, its name and
// its format (optional).
type FieldField struct {
	Field  string
	Format string
}

// Source serializes the FieldField into JSON.
func (d FieldField) Source() (interface{}, error) {
	if d.Format == "" {
		return d.Field, nil
	}
	return map[string]interface{}{
		"field":  d.Field,
		"format": d.Format,
	}, nil
}

// FieldFields is a slice of FieldField instances.
type FieldFields []FieldField

// Source serializes the FieldFields into JSON.
func (d FieldFields) Source() (interface{}, error) {
	if d == nil {
		return nil, nil
	}
	v := make([]interface{}, 0)
	for _, f := range d {
		src, err := f.Source()
		if err != nil {
			return nil, err
		}
		v = append(v, src)
	}
	return v, nil
}
