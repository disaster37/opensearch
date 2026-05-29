package querydsl

// Suggester is the interface implemented by all suggestion strategies.
// A Suggester produces a JSON-serializable representation of a suggest
// clause for the OpenSearch _suggest API.
//
// Built-in implementations include [CompletionSuggester], [PhraseSuggester],
// and [TermSuggester].
type Suggester interface {
	// Name returns the name of this suggester as it appears in the response.
	Name() string
	// Source returns the JSON-serializable suggest clause. When includeName
	// is true the result is wrapped in a map keyed by the suggester name.
	Source(includeName bool) (any, error)
}
