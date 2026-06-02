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

func NewPercolatorQuery() *PercolatorQuery {
	return &PercolatorQuery{}
}

// WithField sets the field containing the percolator query.
func (q *PercolatorQuery) WithField(field string) *PercolatorQuery {
	q.Field = field
	return q
}

// WithName sets an optional name for the percolator query.
func (q *PercolatorQuery) WithName(name string) *PercolatorQuery {
	q.Name = name
	return q
}

// WithDocumentType sets the document type for the percolated document.
func (q *PercolatorQuery) WithDocumentType(documentType string) *PercolatorQuery {
	q.DocumentType = documentType
	return q
}

// WithDocuments sets the documents to be percolated.
func (q *PercolatorQuery) WithDocuments(documents ...any) *PercolatorQuery {
	q.Documents = documents
	return q
}

// WithIndexedDocumentIndex sets the index of the indexed document to percolate.
func (q *PercolatorQuery) WithIndexedDocumentIndex(index string) *PercolatorQuery {
	q.IndexedDocumentIndex = index
	return q
}

// WithIndexedDocumentType sets the type of the indexed document to percolate.
func (q *PercolatorQuery) WithIndexedDocumentType(docType string) *PercolatorQuery {
	q.IndexedDocumentType = docType
	return q
}

// WithIndexedDocumentID sets the ID of the indexed document to percolate.
func (q *PercolatorQuery) WithIndexedDocumentID(id string) *PercolatorQuery {
	q.IndexedDocumentID = id
	return q
}

// WithIndexedDocumentRouting sets the routing value for the indexed document.
func (q *PercolatorQuery) WithIndexedDocumentRouting(routing string) *PercolatorQuery {
	q.IndexedDocumentRouting = routing
	return q
}

// WithIndexedDocumentPreference sets the preference for fetching the indexed document.
func (q *PercolatorQuery) WithIndexedDocumentPreference(preference string) *PercolatorQuery {
	q.IndexedDocumentPreference = preference
	return q
}

// WithIndexedDocumentVersion sets the version of the indexed document to percolate.
func (q *PercolatorQuery) WithIndexedDocumentVersion(version int64) *PercolatorQuery {
	q.IndexedDocumentVersion = &version
	return q
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
