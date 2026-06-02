package querydsl

type GeoBoundingBoxQuery struct {
	Field            string
	TopLeft          any
	TopRight         any
	BottomLeft       any
	BottomRight      any
	WKT              any
	Type             string
	ValidationMethod string
	IgnoreUnmapped   *bool
	QueryName        string
}

func NewGeoBoundingBoxQuery(field string) *GeoBoundingBoxQuery {
	return &GeoBoundingBoxQuery{Field: field}
}

// WithTopLeft sets the top-left corner of the bounding box.
func (q *GeoBoundingBoxQuery) WithTopLeft(v any) *GeoBoundingBoxQuery {
	q.TopLeft = v
	return q
}

// WithTopRight sets the top-right corner of the bounding box.
func (q *GeoBoundingBoxQuery) WithTopRight(v any) *GeoBoundingBoxQuery {
	q.TopRight = v
	return q
}

// WithBottomLeft sets the bottom-left corner of the bounding box.
func (q *GeoBoundingBoxQuery) WithBottomLeft(v any) *GeoBoundingBoxQuery {
	q.BottomLeft = v
	return q
}

// WithBottomRight sets the bottom-right corner of the bounding box.
func (q *GeoBoundingBoxQuery) WithBottomRight(v any) *GeoBoundingBoxQuery {
	q.BottomRight = v
	return q
}

// WithWKT sets the bounding box as a WKT shape.
func (q *GeoBoundingBoxQuery) WithWKT(wkt any) *GeoBoundingBoxQuery {
	q.WKT = wkt
	return q
}

// WithType sets the execution type of the geo bounding box query.
func (q *GeoBoundingBoxQuery) WithType(t string) *GeoBoundingBoxQuery {
	q.Type = t
	return q
}

// WithValidationMethod sets the validation method for geo coordinates.
func (q *GeoBoundingBoxQuery) WithValidationMethod(method string) *GeoBoundingBoxQuery {
	q.ValidationMethod = method
	return q
}

// WithIgnoreUnmapped sets whether to ignore unmapped fields.
func (q *GeoBoundingBoxQuery) WithIgnoreUnmapped(ignore bool) *GeoBoundingBoxQuery {
	q.IgnoreUnmapped = &ignore
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *GeoBoundingBoxQuery) WithQueryName(name string) *GeoBoundingBoxQuery {
	q.QueryName = name
	return q
}

func (q GeoBoundingBoxQuery) TopLeftCoords(top, left float64) GeoBoundingBoxQuery {
	q.TopLeft = []float64{left, top}
	return q
}

func (q GeoBoundingBoxQuery) BottomRightCoords(bottom, right float64) GeoBoundingBoxQuery {
	q.BottomRight = []float64{right, bottom}
	return q
}

func (q GeoBoundingBoxQuery) Source() (any, error) {
	box := map[string]any{}
	if q.WKT != nil {
		box["wkt"] = q.WKT
	} else {
		if q.TopLeft != nil {
			box["top_left"] = q.TopLeft
		}
		if q.TopRight != nil {
			box["top_right"] = q.TopRight
		}
		if q.BottomLeft != nil {
			box["bottom_left"] = q.BottomLeft
		}
		if q.BottomRight != nil {
			box["bottom_right"] = q.BottomRight
		}
	}
	params := map[string]any{q.Field: box}
	if q.Type != "" {
		params["type"] = q.Type
	}
	if q.ValidationMethod != "" {
		params["validation_method"] = q.ValidationMethod
	}
	if q.IgnoreUnmapped != nil {
		params["ignore_unmapped"] = *q.IgnoreUnmapped
	}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"geo_bounding_box": params}, nil
}
