package querydsl

// TermSuggester suggests individual terms based on edit distance from the
// input text. It supports configuring the suggest mode, string distance
// metric, accuracy, and other term-level options.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-term.html.
//
// Typical usage:
//
//	suggester := querydsl.NewTermSuggester("my-suggest").
//	    Field("title").
//	    Text("openseerch").
//	    Size(3)
type TermSuggester struct {
	Suggester
	name           string
	text           string
	field          string
	analyzer       string
	size           *int
	shardSize      *int
	contextQueries []SuggesterContextQuery

	// fields specific to term suggester
	suggestMode    string
	accuracy       *float64
	sort           string
	stringDistance string
	maxEdits       *int
	maxInspections *int
	maxTermFreq    *float64
	prefixLength   *int
	minWordLength  *int
	minDocFreq     *float64
}

// NewTermSuggester creates a new TermSuggester.
func NewTermSuggester(name string) *TermSuggester {
	return &TermSuggester{
		name: name,
	}
}

// Name returns the name of this suggester.
func (q *TermSuggester) Name() string {
	return q.name
}

// Text sets the input text for the term suggester.
func (q *TermSuggester) Text(text string) *TermSuggester {
	q.text = text
	return q
}

// Field sets the field to query for term suggestions.
func (q *TermSuggester) Field(field string) *TermSuggester {
	q.field = field
	return q
}

// Analyzer sets the analyzer used to analyze the suggestion text.
func (q *TermSuggester) Analyzer(analyzer string) *TermSuggester {
	q.analyzer = analyzer
	return q
}

// Size sets the maximum number of term suggestions to return.
func (q *TermSuggester) Size(size int) *TermSuggester {
	q.size = &size
	return q
}

// ShardSize sets the number of suggestions each shard returns.
func (q *TermSuggester) ShardSize(shardSize int) *TermSuggester {
	q.shardSize = &shardSize
	return q
}

// ContextQuery adds a single context query to filter suggestions.
func (q *TermSuggester) ContextQuery(query SuggesterContextQuery) *TermSuggester {
	q.contextQueries = append(q.contextQueries, query)
	return q
}

// ContextQueries adds context queries to filter suggestions.
func (q *TermSuggester) ContextQueries(queries ...SuggesterContextQuery) *TermSuggester {
	q.contextQueries = append(q.contextQueries, queries...)
	return q
}

// SuggestMode sets the suggest mode, e.g. "missing", "popular", or "always".
func (q *TermSuggester) SuggestMode(suggestMode string) *TermSuggester {
	q.suggestMode = suggestMode
	return q
}

// Accuracy sets the minimum score a suggestion must achieve to be included
// in the results, expressed as a fraction between 0.0 and 1.0.
func (q *TermSuggester) Accuracy(accuracy float64) *TermSuggester {
	q.accuracy = &accuracy
	return q
}

// Sort sets the sort order of suggestions, e.g. "score" or "frequency".
func (q *TermSuggester) Sort(sort string) *TermSuggester {
	q.sort = sort
	return q
}

// StringDistance sets the string distance algorithm, e.g. "internal_levenshtein",
// "damerau_levenshtein", "levenshtein", "internal_ngram", or "ngram".
func (q *TermSuggester) StringDistance(stringDistance string) *TermSuggester {
	q.stringDistance = stringDistance
	return q
}

// MaxEdits sets the maximum number of edits a term can have to be considered
// a suggestion. Valid values are 1 or 2.
func (q *TermSuggester) MaxEdits(maxEdits int) *TermSuggester {
	q.maxEdits = &maxEdits
	return q
}

// MaxInspections sets the number of term inspections performed per shard
// for each suggestion candidate.
func (q *TermSuggester) MaxInspections(maxInspections int) *TermSuggester {
	q.maxInspections = &maxInspections
	return q
}

// MaxTermFreq sets the maximum frequency (as a fraction of total docs) a
// term can have to be included in the suggestion results.
func (q *TermSuggester) MaxTermFreq(maxTermFreq float64) *TermSuggester {
	q.maxTermFreq = &maxTermFreq
	return q
}

// PrefixLength sets the number of prefix characters that must match between
// the input term and the suggestion. Higher values improve accuracy and
// performance but reduce the number of suggestions.
func (q *TermSuggester) PrefixLength(prefixLength int) *TermSuggester {
	q.prefixLength = &prefixLength
	return q
}

// MinWordLength sets the minimum length a suggestion must have to be returned.
func (q *TermSuggester) MinWordLength(minWordLength int) *TermSuggester {
	q.minWordLength = &minWordLength
	return q
}

// MinDocFreq sets the minimum document frequency a term must have to be
// included in the suggestion results.
func (q *TermSuggester) MinDocFreq(minDocFreq float64) *TermSuggester {
	q.minDocFreq = &minDocFreq
	return q
}

// termSuggesterRequest is necessary because the order in which
// the JSON elements are routed to Opensearch is relevant.
// We got into trouble when using plain maps because the text element
// needs to go before the term element.
type termSuggesterRequest struct {
	Text string `json:"text"`
	Term any    `json:"term"`
}

// Source generates the source for the term suggester.
func (q *TermSuggester) Source(includeName bool) (any, error) {
	// "suggest" : {
	//   "my-suggest-1" : {
	//     "text" : "the amsterdma meetpu",
	//     "term" : {
	//       "field" : "body"
	//     }
	//   },
	//   "my-suggest-2" : {
	//     "text" : "the rottredam meetpu",
	//     "term" : {
	//       "field" : "title",
	//     }
	//   }
	// }
	ts := &termSuggesterRequest{}
	if q.text != "" {
		ts.Text = q.text
	}

	suggester := make(map[string]any)
	ts.Term = suggester

	if q.analyzer != "" {
		suggester["analyzer"] = q.analyzer
	}
	if q.field != "" {
		suggester["field"] = q.field
	}
	if q.size != nil {
		suggester["size"] = *q.size
	}
	if q.shardSize != nil {
		suggester["shard_size"] = *q.shardSize
	}
	switch len(q.contextQueries) {
	case 0:
	case 1:
		src, err := q.contextQueries[0].Source()
		if err != nil {
			return nil, err
		}
		suggester["contexts"] = src
	default:
		ctxq := make([]any, len(q.contextQueries))
		for i, query := range q.contextQueries {
			src, err := query.Source()
			if err != nil {
				return nil, err
			}
			ctxq[i] = src
		}
		suggester["contexts"] = ctxq
	}

	// Specific to term suggester
	if q.suggestMode != "" {
		suggester["suggest_mode"] = q.suggestMode
	}
	if q.accuracy != nil {
		suggester["accuracy"] = *q.accuracy
	}
	if q.sort != "" {
		suggester["sort"] = q.sort
	}
	if q.stringDistance != "" {
		suggester["string_distance"] = q.stringDistance
	}
	if q.maxEdits != nil {
		suggester["max_edits"] = *q.maxEdits
	}
	if q.maxInspections != nil {
		suggester["max_inspections"] = *q.maxInspections
	}
	if q.maxTermFreq != nil {
		suggester["max_term_freq"] = *q.maxTermFreq
	}
	if q.prefixLength != nil {
		suggester["prefix_length"] = *q.prefixLength
	}
	if q.minWordLength != nil {
		suggester["min_word_length"] = *q.minWordLength
	}
	if q.minDocFreq != nil {
		suggester["min_doc_freq"] = *q.minDocFreq
	}

	if !includeName {
		return ts, nil
	}

	source := make(map[string]any)
	source[q.name] = ts
	return source, nil
}
