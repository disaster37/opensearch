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

func NewGeoBoundingBoxQuery(field string) GeoBoundingBoxQuery {
	return GeoBoundingBoxQuery{Field: field}
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
