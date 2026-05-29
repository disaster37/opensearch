package querydsl

// PhraseSuggester provides phrase-level suggestions by analyzing the input
// as a whole phrase and proposing alternatives on a per-token basis within
// a given string distance. It supports candidate generators, smoothing
// models, collation, and highlighting.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-phrase.html.
//
// Typical usage:
//
//	suggester := querydsl.NewPhraseSuggester("my-suggest").
//	    Field("title.trigram").
//	    Text("noble prize").
//	    Size(1)
type PhraseSuggester struct {
	Suggester
	name           string
	text           string
	field          string
	analyzer       string
	size           *int
	shardSize      *int
	contextQueries []SuggesterContextQuery

	// fields specific to a phrase suggester
	maxErrors               *float64
	separator               *string
	realWordErrorLikelihood *float64
	confidence              *float64
	generators              map[string][]CandidateGenerator
	gramSize                *int
	smoothingModel          SmoothingModel
	forceUnigrams           *bool
	tokenLimit              *int
	preTag, postTag         *string
	collateQuery            *Script
	collatePreference       *string
	collateParams           map[string]any
	collatePrune            *bool
}

// NewPhraseSuggester creates a new PhraseSuggester.
func NewPhraseSuggester(name string) *PhraseSuggester {
	return &PhraseSuggester{
		name:          name,
		collateParams: make(map[string]any),
	}
}

// Name returns the name of this suggester.
func (q *PhraseSuggester) Name() string {
	return q.name
}

// Text sets the input text for the phrase suggester.
func (q *PhraseSuggester) Text(text string) *PhraseSuggester {
	q.text = text
	return q
}

// Field sets the field to query for phrase suggestions.
func (q *PhraseSuggester) Field(field string) *PhraseSuggester {
	q.field = field
	return q
}

// Analyzer sets the analyzer used to analyze the suggestion text.
func (q *PhraseSuggester) Analyzer(analyzer string) *PhraseSuggester {
	q.analyzer = analyzer
	return q
}

// Size sets the maximum number of phrase suggestions to return.
func (q *PhraseSuggester) Size(size int) *PhraseSuggester {
	q.size = &size
	return q
}

// ShardSize sets the number of suggestions each shard returns.
func (q *PhraseSuggester) ShardSize(shardSize int) *PhraseSuggester {
	q.shardSize = &shardSize
	return q
}

// ContextQuery adds a single context query to filter suggestions.
func (q *PhraseSuggester) ContextQuery(query SuggesterContextQuery) *PhraseSuggester {
	q.contextQueries = append(q.contextQueries, query)
	return q
}

// ContextQueries adds context queries to filter suggestions.
func (q *PhraseSuggester) ContextQueries(queries ...SuggesterContextQuery) *PhraseSuggester {
	q.contextQueries = append(q.contextQueries, queries...)
	return q
}

// GramSize sets the size of the n-grams used to build the phrase suggester
// index. Values less than 1 are ignored.
func (q *PhraseSuggester) GramSize(gramSize int) *PhraseSuggester {
	if gramSize >= 1 {
		q.gramSize = &gramSize
	}
	return q
}

// MaxErrors sets the maximum fraction of erroneous tokens before a candidate
// is discarded.
func (q *PhraseSuggester) MaxErrors(maxErrors float64) *PhraseSuggester {
	q.maxErrors = &maxErrors
	return q
}

// Separator sets the token separator used to split bigrams into individual terms.
func (q *PhraseSuggester) Separator(separator string) *PhraseSuggester {
	q.separator = &separator
	return q
}

// RealWordErrorLikelihood sets the likelihood that a term is misspelled even
// if it exists in the index.
func (q *PhraseSuggester) RealWordErrorLikelihood(realWordErrorLikelihood float64) *PhraseSuggester {
	q.realWordErrorLikelihood = &realWordErrorLikelihood
	return q
}

// Confidence sets the confidence threshold; suggestions with a score lower
// than confidence * input score are filtered out.
func (q *PhraseSuggester) Confidence(confidence float64) *PhraseSuggester {
	q.confidence = &confidence
	return q
}

// CandidateGenerator adds a candidate generator that proposes term alternatives.
func (q *PhraseSuggester) CandidateGenerator(generator CandidateGenerator) *PhraseSuggester {
	if q.generators == nil {
		q.generators = make(map[string][]CandidateGenerator)
	}
	typ := generator.Type()
	if _, found := q.generators[typ]; !found {
		q.generators[typ] = make([]CandidateGenerator, 0)
	}
	q.generators[typ] = append(q.generators[typ], generator)
	return q
}

// CandidateGenerators adds candidate generators that propose term alternatives.
func (q *PhraseSuggester) CandidateGenerators(generators ...CandidateGenerator) *PhraseSuggester {
	for _, g := range generators {
		q = q.CandidateGenerator(g)
	}
	return q
}

// ClearCandidateGenerator removes all candidate generators.
func (q *PhraseSuggester) ClearCandidateGenerator() *PhraseSuggester {
	q.generators = nil
	return q
}

// ForceUnigrams controls whether the candidate generator should always
// produce unigram candidates even when the input is a multi-token phrase.
func (q *PhraseSuggester) ForceUnigrams(forceUnigrams bool) *PhraseSuggester {
	q.forceUnigrams = &forceUnigrams
	return q
}

// SmoothingModel sets the smoothing model used to balance the weight
// between the n-gram and unigram candidates.
func (q *PhraseSuggester) SmoothingModel(smoothingModel SmoothingModel) *PhraseSuggester {
	q.smoothingModel = smoothingModel
	return q
}

// TokenLimit sets the maximum number of candidate tokens considered per
// suggestion.
func (q *PhraseSuggester) TokenLimit(tokenLimit int) *PhraseSuggester {
	q.tokenLimit = &tokenLimit
	return q
}

// Highlight sets pre and post tags that wrap changed tokens in the
// suggestion for highlighting purposes.
func (q *PhraseSuggester) Highlight(preTag, postTag string) *PhraseSuggester {
	q.preTag = &preTag
	q.postTag = &postTag
	return q
}

// CollateQuery sets the script used to collate and check suggestions
// before returning them to the user.
func (q *PhraseSuggester) CollateQuery(collateQuery *Script) *PhraseSuggester {
	q.collateQuery = collateQuery
	return q
}

// CollatePreference sets the routing preference used during collation.
func (q *PhraseSuggester) CollatePreference(collatePreference string) *PhraseSuggester {
	q.collatePreference = &collatePreference
	return q
}

// CollateParams sets the parameters passed to the collation script.
func (q *PhraseSuggester) CollateParams(collateParams map[string]any) *PhraseSuggester {
	q.collateParams = collateParams
	return q
}

// CollatePrune controls whether to prune suggestions that do not pass
// the collation check.
func (q *PhraseSuggester) CollatePrune(collatePrune bool) *PhraseSuggester {
	q.collatePrune = &collatePrune
	return q
}

// phraseSuggesterRequest is necessary because the order in which
// the JSON elements are routed to Opensearch is relevant.
// We got into trouble when using plain maps because the text element
// needs to go before the simple_phrase element.
type phraseSuggesterRequest struct {
	Text   string `json:"text"`
	Phrase any    `json:"phrase"`
}

// Source generates the source for the phrase suggester.
func (q *PhraseSuggester) Source(includeName bool) (any, error) {
	ps := &phraseSuggesterRequest{}

	if q.text != "" {
		ps.Text = q.text
	}

	suggester := make(map[string]any)
	ps.Phrase = suggester

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
		var ctxq []any
		for _, query := range q.contextQueries {
			src, err := query.Source()
			if err != nil {
				return nil, err
			}
			ctxq = append(ctxq, src)
		}
		suggester["contexts"] = ctxq
	}

	// Phase-specified parameters
	if q.realWordErrorLikelihood != nil {
		suggester["real_word_error_likelihood"] = *q.realWordErrorLikelihood
	}
	if q.confidence != nil {
		suggester["confidence"] = *q.confidence
	}
	if q.separator != nil {
		suggester["separator"] = *q.separator
	}
	if q.maxErrors != nil {
		suggester["max_errors"] = *q.maxErrors
	}
	if q.gramSize != nil {
		suggester["gram_size"] = *q.gramSize
	}
	if q.forceUnigrams != nil {
		suggester["force_unigrams"] = *q.forceUnigrams
	}
	if q.tokenLimit != nil {
		suggester["token_limit"] = *q.tokenLimit
	}
	if len(q.generators) > 0 {
		for typ, generators := range q.generators {
			var arr []any
			for _, g := range generators {
				src, err := g.Source()
				if err != nil {
					return nil, err
				}
				arr = append(arr, src)
			}
			suggester[typ] = arr
		}
	}
	if q.smoothingModel != nil {
		src, err := q.smoothingModel.Source()
		if err != nil {
			return nil, err
		}
		x := make(map[string]any)
		x[q.smoothingModel.Type()] = src
		suggester["smoothing"] = x
	}
	if q.preTag != nil {
		hl := make(map[string]string)
		hl["pre_tag"] = *q.preTag
		if q.postTag != nil {
			hl["post_tag"] = *q.postTag
		}
		suggester["highlight"] = hl
	}
	if q.collateQuery != nil {
		collate := make(map[string]any)
		suggester["collate"] = collate
		collate["query"], _ = q.collateQuery.Source()
		if q.collatePreference != nil {
			collate["preference"] = *q.collatePreference
		}
		if len(q.collateParams) > 0 {
			collate["params"] = q.collateParams
		}
		if q.collatePrune != nil {
			collate["prune"] = *q.collatePrune
		}
	}

	if !includeName {
		return ps, nil
	}

	source := make(map[string]any)
	source[q.name] = ps
	return source, nil
}

// -- Smoothing models --

type SmoothingModel interface {
	Type() string
	Source() (any, error)
}

// StupidBackoffSmoothingModel implements a stupid backoff smoothing model.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-phrase.html#_smoothing_models
// for details about smoothing models.
type StupidBackoffSmoothingModel struct {
	discount float64
}

func NewStupidBackoffSmoothingModel(discount float64) *StupidBackoffSmoothingModel {
	return &StupidBackoffSmoothingModel{
		discount: discount,
	}
}

func (sm *StupidBackoffSmoothingModel) Type() string {
	return "stupid_backoff"
}

func (sm *StupidBackoffSmoothingModel) Source() (any, error) {
	source := make(map[string]any)
	source["discount"] = sm.discount
	return source, nil
}

// --

// LaplaceSmoothingModel implements a laplace smoothing model.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-phrase.html#_smoothing_models
// for details about smoothing models.
type LaplaceSmoothingModel struct {
	alpha float64
}

func NewLaplaceSmoothingModel(alpha float64) *LaplaceSmoothingModel {
	return &LaplaceSmoothingModel{
		alpha: alpha,
	}
}

func (sm *LaplaceSmoothingModel) Type() string {
	return "laplace"
}

func (sm *LaplaceSmoothingModel) Source() (any, error) {
	source := make(map[string]any)
	source["alpha"] = sm.alpha
	return source, nil
}

// --

// LinearInterpolationSmoothingModel implements a linear interpolation
// smoothing model.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-phrase.html#_smoothing_models
// for details about smoothing models.
type LinearInterpolationSmoothingModel struct {
	trigramLamda  float64
	bigramLambda  float64
	unigramLambda float64
}

func NewLinearInterpolationSmoothingModel(trigramLamda, bigramLambda, unigramLambda float64) *LinearInterpolationSmoothingModel {
	return &LinearInterpolationSmoothingModel{
		trigramLamda:  trigramLamda,
		bigramLambda:  bigramLambda,
		unigramLambda: unigramLambda,
	}
}

func (sm *LinearInterpolationSmoothingModel) Type() string {
	return "linear_interpolation"
}

func (sm *LinearInterpolationSmoothingModel) Source() (any, error) {
	source := make(map[string]any)
	source["trigram_lambda"] = sm.trigramLamda
	source["bigram_lambda"] = sm.bigramLambda
	source["unigram_lambda"] = sm.unigramLambda
	return source, nil
}

// -- CandidateGenerator --

type CandidateGenerator interface {
	Type() string
	Source() (any, error)
}

// DirectCandidateGenerator implements a direct candidate generator.
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-suggesters-phrase.html#_smoothing_models
// for details about smoothing models.
type DirectCandidateGenerator struct {
	field          string
	preFilter      *string
	postFilter     *string
	suggestMode    *string
	accuracy       *float64
	size           *int
	sort           *string
	stringDistance *string
	maxEdits       *int
	maxInspections *int
	maxTermFreq    *float64
	prefixLength   *int
	minWordLength  *int
	minDocFreq     *float64
}

func NewDirectCandidateGenerator(field string) *DirectCandidateGenerator {
	return &DirectCandidateGenerator{
		field: field,
	}
}

func (g *DirectCandidateGenerator) Type() string {
	return "direct_generator"
}

func (g *DirectCandidateGenerator) Field(field string) *DirectCandidateGenerator {
	g.field = field
	return g
}

func (g *DirectCandidateGenerator) PreFilter(preFilter string) *DirectCandidateGenerator {
	g.preFilter = &preFilter
	return g
}

func (g *DirectCandidateGenerator) PostFilter(postFilter string) *DirectCandidateGenerator {
	g.postFilter = &postFilter
	return g
}

func (g *DirectCandidateGenerator) SuggestMode(suggestMode string) *DirectCandidateGenerator {
	g.suggestMode = &suggestMode
	return g
}

func (g *DirectCandidateGenerator) Accuracy(accuracy float64) *DirectCandidateGenerator {
	g.accuracy = &accuracy
	return g
}

func (g *DirectCandidateGenerator) Size(size int) *DirectCandidateGenerator {
	g.size = &size
	return g
}

func (g *DirectCandidateGenerator) Sort(sort string) *DirectCandidateGenerator {
	g.sort = &sort
	return g
}

func (g *DirectCandidateGenerator) StringDistance(stringDistance string) *DirectCandidateGenerator {
	g.stringDistance = &stringDistance
	return g
}

func (g *DirectCandidateGenerator) MaxEdits(maxEdits int) *DirectCandidateGenerator {
	g.maxEdits = &maxEdits
	return g
}

func (g *DirectCandidateGenerator) MaxInspections(maxInspections int) *DirectCandidateGenerator {
	g.maxInspections = &maxInspections
	return g
}

func (g *DirectCandidateGenerator) MaxTermFreq(maxTermFreq float64) *DirectCandidateGenerator {
	g.maxTermFreq = &maxTermFreq
	return g
}

func (g *DirectCandidateGenerator) PrefixLength(prefixLength int) *DirectCandidateGenerator {
	g.prefixLength = &prefixLength
	return g
}

func (g *DirectCandidateGenerator) MinWordLength(minWordLength int) *DirectCandidateGenerator {
	g.minWordLength = &minWordLength
	return g
}

func (g *DirectCandidateGenerator) MinDocFreq(minDocFreq float64) *DirectCandidateGenerator {
	g.minDocFreq = &minDocFreq
	return g
}

func (g *DirectCandidateGenerator) Source() (any, error) {
	source := make(map[string]any)
	if g.field != "" {
		source["field"] = g.field
	}
	if g.suggestMode != nil {
		source["suggest_mode"] = *g.suggestMode
	}
	if g.accuracy != nil {
		source["accuracy"] = *g.accuracy
	}
	if g.size != nil {
		source["size"] = *g.size
	}
	if g.sort != nil {
		source["sort"] = *g.sort
	}
	if g.stringDistance != nil {
		source["string_distance"] = *g.stringDistance
	}
	if g.maxEdits != nil {
		source["max_edits"] = *g.maxEdits
	}
	if g.maxInspections != nil {
		source["max_inspections"] = *g.maxInspections
	}
	if g.maxTermFreq != nil {
		source["max_term_freq"] = *g.maxTermFreq
	}
	if g.prefixLength != nil {
		source["prefix_length"] = *g.prefixLength
	}
	if g.minWordLength != nil {
		source["min_word_length"] = *g.minWordLength
	}
	if g.minDocFreq != nil {
		source["min_doc_freq"] = *g.minDocFreq
	}
	if g.preFilter != nil {
		source["pre_filter"] = *g.preFilter
	}
	if g.postFilter != nil {
		source["post_filter"] = *g.postFilter
	}
	return source, nil
}
