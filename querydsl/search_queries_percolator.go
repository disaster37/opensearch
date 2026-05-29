package querydsl

type PercolatorQuery struct {
	Field                     string
	Name                      string
	DocumentType              string
	Documents                 []any
	IndexedDocumentIndex      string
	IndexedDocumentType       string
	IndexedDocumentID         string
	IndexedDocumentRouting    string
	IndexedDocumentPreference string
	IndexedDocumentVersion    *int64
}

func NewPercolatorQuery() PercolatorQuery {
	return PercolatorQuery{}
}

func (q PercolatorQuery) Source() (any, error) {
	params := map[string]any{"field": q.Field}
	if q.DocumentType != "" {
		params["document_type"] = q.DocumentType
	}
	if q.Name != "" {
		params["name"] = q.Name
	}
	switch len(q.Documents) {
	case 1:
		params["document"] = q.Documents[0]
	default:
		if len(q.Documents) > 1 {
			params["documents"] = q.Documents
		}
	}
	if q.IndexedDocumentIndex != "" {
		params["index"] = q.IndexedDocumentIndex
	}
	if q.IndexedDocumentType != "" {
		params["type"] = q.IndexedDocumentType
	}
	if q.IndexedDocumentID != "" {
		params["id"] = q.IndexedDocumentID
	}
	if q.IndexedDocumentRouting != "" {
		params["routing"] = q.IndexedDocumentRouting
	}
	if q.IndexedDocumentPreference != "" {
		params["preference"] = q.IndexedDocumentPreference
	}
	if q.IndexedDocumentVersion != nil {
		params["version"] = *q.IndexedDocumentVersion
	}
	return map[string]any{"percolate": params}, nil
}
