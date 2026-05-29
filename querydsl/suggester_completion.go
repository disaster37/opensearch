package querydsl

import "errors"

// CompletionSuggester is a fast suggester for type-ahead completion. It uses
// a dedicated completion field and supports fuzzy matching, regex prefixes,
// context-aware suggestions, and duplicate skipping.
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-completion.html
// for more details.
//
// Typical usage:
//
//	suggester := querydsl.NewCompletionSuggester("song-suggest").
//	    Field("suggest").
//	    Prefix("nir")
type CompletionSuggester struct {
	Suggester
	name           string
	text           string
	prefix         string
	regex          string
	field          string
	analyzer       string
	size           *int
	shardSize      *int
	contextQueries []SuggesterContextQuery

	fuzzyOptions   *FuzzyCompletionSuggesterOptions
	regexOptions   *RegexCompletionSuggesterOptions
	skipDuplicates *bool
}

// NewCompletionSuggester creates a new CompletionSuggester with the given name.
func NewCompletionSuggester(name string) *CompletionSuggester {
	return &CompletionSuggester{
		name: name,
	}
}

// Name returns the name of this suggester.
func (q *CompletionSuggester) Name() string {
	return q.name
}

// Text sets the global input text for the suggester.
func (q *CompletionSuggester) Text(text string) *CompletionSuggester {
	q.text = text
	return q
}

// Prefix sets the prefix to query the completion suggester with.
func (q *CompletionSuggester) Prefix(prefix string) *CompletionSuggester {
	q.prefix = prefix
	return q
}

// PrefixWithEditDistance sets the prefix and enables fuzzy matching with
// the specified edit distance.
func (q *CompletionSuggester) PrefixWithEditDistance(prefix string, editDistance any) *CompletionSuggester {
	q.prefix = prefix
	q.fuzzyOptions = NewFuzzyCompletionSuggesterOptions().EditDistance(editDistance)
	return q
}

// PrefixWithOptions sets the prefix and passes full fuzzy matching options.
func (q *CompletionSuggester) PrefixWithOptions(prefix string, options *FuzzyCompletionSuggesterOptions) *CompletionSuggester {
	q.prefix = prefix
	q.fuzzyOptions = options
	return q
}

// FuzzyOptions sets the options that control fuzzy matching behavior.
func (q *CompletionSuggester) FuzzyOptions(options *FuzzyCompletionSuggesterOptions) *CompletionSuggester {
	q.fuzzyOptions = options
	return q
}

// Fuzziness is a shortcut for setting only the edit distance on the fuzzy
// options, creating them if they do not already exist.
func (q *CompletionSuggester) Fuzziness(fuzziness any) *CompletionSuggester {
	if q.fuzzyOptions == nil {
		q.fuzzyOptions = NewFuzzyCompletionSuggesterOptions()
	}
	q.fuzzyOptions = q.fuzzyOptions.EditDistance(fuzziness)
	return q
}

// Regex sets the regular expression to query the completion suggester with.
func (q *CompletionSuggester) Regex(regex string) *CompletionSuggester {
	q.regex = regex
	return q
}

// RegexWithOptions sets the regex and passes full regex matching options.
func (q *CompletionSuggester) RegexWithOptions(regex string, options *RegexCompletionSuggesterOptions) *CompletionSuggester {
	q.regex = regex
	q.regexOptions = options
	return q
}

// RegexOptions sets the options that control regex matching behavior.
func (q *CompletionSuggester) RegexOptions(options *RegexCompletionSuggesterOptions) *CompletionSuggester {
	q.regexOptions = options
	return q
}

// SkipDuplicates controls whether duplicate suggestions should be filtered out.
func (q *CompletionSuggester) SkipDuplicates(skipDuplicates bool) *CompletionSuggester {
	q.skipDuplicates = &skipDuplicates
	return q
}

// Field sets the completion field to query against.
func (q *CompletionSuggester) Field(field string) *CompletionSuggester {
	q.field = field
	return q
}

// Analyzer sets the analyzer to use when analyzing the suggestion text.
func (q *CompletionSuggester) Analyzer(analyzer string) *CompletionSuggester {
	q.analyzer = analyzer
	return q
}

// Size sets the maximum number of suggestions to return.
func (q *CompletionSuggester) Size(size int) *CompletionSuggester {
	q.size = &size
	return q
}

// ShardSize sets the number of suggestions each shard returns. The coordinating
// node merges shard results to produce the final size.
func (q *CompletionSuggester) ShardSize(shardSize int) *CompletionSuggester {
	q.shardSize = &shardSize
	return q
}

// ContextQuery adds a single context query to filter suggestions by context.
func (q *CompletionSuggester) ContextQuery(query SuggesterContextQuery) *CompletionSuggester {
	q.contextQueries = append(q.contextQueries, query)
	return q
}

// ContextQueries adds context queries to filter suggestions by context.
func (q *CompletionSuggester) ContextQueries(queries ...SuggesterContextQuery) *CompletionSuggester {
	q.contextQueries = append(q.contextQueries, queries...)
	return q
}

// completionSuggesterRequest is necessary because the order in which
// the JSON elements are routed to Opensearch is relevant.
// We got into trouble when using plain maps because the text element
// needs to go before the completion element.
type completionSuggesterRequest struct {
	Text       string `json:"text,omitempty"`
	Prefix     string `json:"prefix,omitempty"`
	Regex      string `json:"regex,omitempty"`
	Completion any    `json:"completion,omitempty"`
}

// Source creates the JSON data for the completion suggester.
func (q *CompletionSuggester) Source(includeName bool) (any, error) {
	cs := &completionSuggesterRequest{}

	if q.text != "" {
		cs.Text = q.text
	}
	if q.prefix != "" {
		cs.Prefix = q.prefix
	}
	if q.regex != "" {
		cs.Regex = q.regex
	}

	suggester := make(map[string]any)
	cs.Completion = suggester

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
		ctxq := make(map[string]any)
		for _, query := range q.contextQueries {
			src, err := query.Source()
			if err != nil {
				return nil, err
			}
			// Merge the dictionary into ctxq
			m, ok := src.(map[string]any)
			if !ok {
				return nil, errors.New("opensearch: context query is not a map")
			}
			for k, v := range m {
				ctxq[k] = v
			}
		}
		suggester["contexts"] = ctxq
	}

	// Fuzzy options
	if q.fuzzyOptions != nil {
		suggester["fuzzy"], _ = q.fuzzyOptions.Source()
	}

	// Regex options
	if q.regexOptions != nil {
		suggester["regex"], _ = q.regexOptions.Source()
	}

	if q.skipDuplicates != nil {
		suggester["skip_duplicates"] = *q.skipDuplicates
	}

	// TODO(oe) Add completion-suggester specific parameters here

	if !includeName {
		return cs, nil
	}

	source := make(map[string]any)
	source[q.name] = cs
	return source, nil
}

// -- Fuzzy options --

// FuzzyCompletionSuggesterOptions represents the options for fuzzy completion suggester.
type FuzzyCompletionSuggesterOptions struct {
	editDistance          any
	transpositions        *bool
	minLength             *int
	prefixLength          *int
	unicodeAware          *bool
	maxDeterminizedStates *int
}

// NewFuzzyCompletionSuggesterOptions initializes a new FuzzyCompletionSuggesterOptions instance.
func NewFuzzyCompletionSuggesterOptions() *FuzzyCompletionSuggesterOptions {
	return &FuzzyCompletionSuggesterOptions{}
}

// EditDistance specifies the maximum number of edits, e.g. a number like "1" or "2"
// or a string like "0..2" or ">5".
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/common-options.html#fuzziness
// for details.
func (o *FuzzyCompletionSuggesterOptions) EditDistance(editDistance any) *FuzzyCompletionSuggesterOptions {
	o.editDistance = editDistance
	return o
}

// Transpositions, if set to true, are counted as one change instead of two (defaults to true).
func (o *FuzzyCompletionSuggesterOptions) Transpositions(transpositions bool) *FuzzyCompletionSuggesterOptions {
	o.transpositions = &transpositions
	return o
}

// MinLength represents the minimum length of the input before fuzzy suggestions are returned (defaults to 3).
func (o *FuzzyCompletionSuggesterOptions) MinLength(minLength int) *FuzzyCompletionSuggesterOptions {
	o.minLength = &minLength
	return o
}

// PrefixLength represents the minimum length of the input, which is not checked for
// fuzzy alternatives (defaults to 1).
func (o *FuzzyCompletionSuggesterOptions) PrefixLength(prefixLength int) *FuzzyCompletionSuggesterOptions {
	o.prefixLength = &prefixLength
	return o
}

// UnicodeAware, if true, all measurements (like fuzzy edit distance, transpositions, and lengths)
// are measured in Unicode code points instead of in bytes. This is slightly slower than
// raw bytes, so it is set to false by default.
func (o *FuzzyCompletionSuggesterOptions) UnicodeAware(unicodeAware bool) *FuzzyCompletionSuggesterOptions {
	o.unicodeAware = &unicodeAware
	return o
}

// MaxDeterminizedStates is currently undocumented in Opensearch. It represents
// the maximum automaton states allowed for fuzzy expansion.
func (o *FuzzyCompletionSuggesterOptions) MaxDeterminizedStates(max int) *FuzzyCompletionSuggesterOptions {
	o.maxDeterminizedStates = &max
	return o
}

// Source creates the JSON data.
func (o *FuzzyCompletionSuggesterOptions) Source() (any, error) {
	out := make(map[string]any)

	if o.editDistance != nil {
		out["fuzziness"] = o.editDistance
	}
	if o.transpositions != nil {
		out["transpositions"] = *o.transpositions
	}
	if o.minLength != nil {
		out["min_length"] = *o.minLength
	}
	if o.prefixLength != nil {
		out["prefix_length"] = *o.prefixLength
	}
	if o.unicodeAware != nil {
		out["unicode_aware"] = *o.unicodeAware
	}
	if o.maxDeterminizedStates != nil {
		out["max_determinized_states"] = *o.maxDeterminizedStates
	}

	return out, nil
}

// -- Regex options --

// RegexCompletionSuggesterOptions represents the options for regex completion suggester.
type RegexCompletionSuggesterOptions struct {
	flags                 any // string or int
	maxDeterminizedStates *int
}

// NewRegexCompletionSuggesterOptions initializes a new RegexCompletionSuggesterOptions instance.
func NewRegexCompletionSuggesterOptions() *RegexCompletionSuggesterOptions {
	return &RegexCompletionSuggesterOptions{}
}

// Flags represents internal regex flags.
// Possible flags are ALL (default), ANYSTRING, COMPLEMENT, EMPTY, INTERSECTION, INTERVAL, or NONE.
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-completion.html#regex
// for details.
func (o *RegexCompletionSuggesterOptions) Flags(flags any) *RegexCompletionSuggesterOptions {
	o.flags = flags
	return o
}

// MaxDeterminizedStates represents the maximum automaton states allowed for regex expansion.
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-completion.html#regex
// for details.
func (o *RegexCompletionSuggesterOptions) MaxDeterminizedStates(max int) *RegexCompletionSuggesterOptions {
	o.maxDeterminizedStates = &max
	return o
}

// Source creates the JSON data.
func (o *RegexCompletionSuggesterOptions) Source() (any, error) {
	out := make(map[string]any)

	if o.flags != nil {
		out["flags"] = o.flags
	}
	if o.maxDeterminizedStates != nil {
		out["max_determinized_states"] = *o.maxDeterminizedStates
	}

	return out, nil
}
