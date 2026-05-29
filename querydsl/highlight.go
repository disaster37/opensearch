package querydsl

// Highlight configures search result highlighting for one or more fields.
// It controls which fields are highlighted, the fragment size, pre/post tags,
// the highlighter type, and many other options. It is attached to a
// [SearchSource] via [SearchSource.Highlight].
//
// For details, see:
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-request-highlighting.html
//
// Typical usage:
//
//	hl := querydsl.NewHighlight().
//	    Field("title").
//	    Field("body").
//	    PreTags("<em>").
//	    PostTags("</em>")
type Highlight struct {
	fields                []*HighlighterField
	tagsSchema            *string
	highlightFilter       *bool
	fragmentSize          *int
	numOfFragments        *int
	preTags               []string
	postTags              []string
	order                 *string
	encoder               *string
	requireFieldMatch     *bool
	maxAnalyzedOffset     *int
	boundaryMaxScan       *int
	boundaryChars         *string
	boundaryScannerType   *string
	boundaryScannerLocale *string
	highlighterType       *string
	fragmenter            *string
	highlightQuery        Query
	noMatchSize           *int
	phraseLimit           *int
	options               map[string]any
	forceSource           *bool
	useExplicitFieldOrder bool
}

// NewHighlight creates a new Highlight with default settings.
func NewHighlight() *Highlight {
	hl := &Highlight{
		options: make(map[string]any),
	}
	return hl
}

// Fields adds one or more pre-configured highlighter fields.
func (hl *Highlight) Fields(fields ...*HighlighterField) *Highlight {
	hl.fields = append(hl.fields, fields...)
	return hl
}

// Field adds a field to highlight by name. Use [Highlight.Fields] to pass
// a pre-configured [HighlighterField] with per-field options.
func (hl *Highlight) Field(name string) *Highlight {
	field := NewHighlighterField(name)
	hl.fields = append(hl.fields, field)
	return hl
}

// TagsSchema sets the tags schema, e.g. "styled".
func (hl *Highlight) TagsSchema(schemaName string) *Highlight {
	hl.tagsSchema = &schemaName
	return hl
}

// HighlightFilter controls whether to highlight only filtered results.
func (hl *Highlight) HighlightFilter(highlightFilter bool) *Highlight {
	hl.highlightFilter = &highlightFilter
	return hl
}

// FragmentSize sets the size of each highlighted fragment in characters.
func (hl *Highlight) FragmentSize(fragmentSize int) *Highlight {
	hl.fragmentSize = &fragmentSize
	return hl
}

// NumOfFragments sets the maximum number of highlight fragments to return.
func (hl *Highlight) NumOfFragments(numOfFragments int) *Highlight {
	hl.numOfFragments = &numOfFragments
	return hl
}

// Encoder sets the encoder for the highlighted text, e.g. "html".
func (hl *Highlight) Encoder(encoder string) *Highlight {
	hl.encoder = &encoder
	return hl
}

// PreTags sets the HTML tags inserted before each highlighted fragment.
func (hl *Highlight) PreTags(preTags ...string) *Highlight {
	hl.preTags = append(hl.preTags, preTags...)
	return hl
}

// PostTags sets the HTML tags inserted after each highlighted fragment.
func (hl *Highlight) PostTags(postTags ...string) *Highlight {
	hl.postTags = append(hl.postTags, postTags...)
	return hl
}

// Order sets the order in which highlight fragments are returned, e.g. "score".
func (hl *Highlight) Order(order string) *Highlight {
	hl.order = &order
	return hl
}

// RequireFieldMatch controls whether highlighting only occurs when the
// query matches the field being highlighted.
func (hl *Highlight) RequireFieldMatch(requireFieldMatch bool) *Highlight {
	hl.requireFieldMatch = &requireFieldMatch
	return hl
}

// MaxAnalyzedOffset sets the maximum number of characters from the source
// to analyze for highlighting.
func (hl *Highlight) MaxAnalyzedOffset(maxAnalyzedOffset int) *Highlight {
	hl.maxAnalyzedOffset = &maxAnalyzedOffset
	return hl
}

// BoundaryMaxScan sets the maximum distance to scan for a boundary character.
func (hl *Highlight) BoundaryMaxScan(boundaryMaxScan int) *Highlight {
	hl.boundaryMaxScan = &boundaryMaxScan
	return hl
}

// BoundaryChars sets the characters that are considered boundaries for
// fragment building, e.g. ".,!? \t\n".
func (hl *Highlight) BoundaryChars(boundaryChars string) *Highlight {
	hl.boundaryChars = &boundaryChars
	return hl
}

// BoundaryScannerType sets the type of boundary scanner to use, e.g.
// "chars", "sentence", or "word".
func (hl *Highlight) BoundaryScannerType(boundaryScannerType string) *Highlight {
	hl.boundaryScannerType = &boundaryScannerType
	return hl
}

// BoundaryScannerLocale sets the locale for the boundary scanner.
func (hl *Highlight) BoundaryScannerLocale(boundaryScannerLocale string) *Highlight {
	hl.boundaryScannerLocale = &boundaryScannerLocale
	return hl
}

// HighlighterType sets the type of highlighter to use, e.g. "unified" (default),
// "plain", or "fvh" (Fast Vector Highlighter).
func (hl *Highlight) HighlighterType(highlighterType string) *Highlight {
	hl.highlighterType = &highlighterType
	return hl
}

// Fragmenter sets the fragmenter to use, e.g. "simple" or "span". Only
// applicable to the plain highlighter.
func (hl *Highlight) Fragmenter(fragmenter string) *Highlight {
	hl.fragmenter = &fragmenter
	return hl
}

// HighlightQuery sets a separate query used exclusively for highlighting,
// independent of the main search query.
func (hl *Highlight) HighlightQuery(highlightQuery Query) *Highlight {
	hl.highlightQuery = highlightQuery
	return hl
}

// NoMatchSize sets the number of characters to return from the beginning
// of the field when there is no matching fragment.
func (hl *Highlight) NoMatchSize(noMatchSize int) *Highlight {
	hl.noMatchSize = &noMatchSize
	return hl
}

// Options sets a generic map of highlighter options.
func (hl *Highlight) Options(options map[string]any) *Highlight {
	hl.options = options
	return hl
}

// ForceSource controls whether to force source-based highlighting even
// when other options are available.
func (hl *Highlight) ForceSource(forceSource bool) *Highlight {
	hl.forceSource = &forceSource
	return hl
}

// UseExplicitFieldOrder determines whether fields are serialized as an
// ordered array (true) or a map (false). Some highlighters require a
// specific field order.
func (hl *Highlight) UseExplicitFieldOrder(useExplicitFieldOrder bool) *Highlight {
	hl.useExplicitFieldOrder = useExplicitFieldOrder
	return hl
}

// Creates the query source for the bool query.
func (hl *Highlight) Source() (any, error) {
	// Returns the map inside of "highlight":
	// "highlight":{
	//   ... this ...
	// }
	source := make(map[string]any)
	if hl.tagsSchema != nil {
		source["tags_schema"] = *hl.tagsSchema
	}
	if len(hl.preTags) > 0 {
		source["pre_tags"] = hl.preTags
	}
	if len(hl.postTags) > 0 {
		source["post_tags"] = hl.postTags
	}
	if hl.order != nil {
		source["order"] = *hl.order
	}
	if hl.highlightFilter != nil {
		source["highlight_filter"] = *hl.highlightFilter
	}
	if hl.fragmentSize != nil {
		source["fragment_size"] = *hl.fragmentSize
	}
	if hl.numOfFragments != nil {
		source["number_of_fragments"] = *hl.numOfFragments
	}
	if hl.encoder != nil {
		source["encoder"] = *hl.encoder
	}
	if hl.requireFieldMatch != nil {
		source["require_field_match"] = *hl.requireFieldMatch
	}
	if hl.maxAnalyzedOffset != nil {
		source["max_analyzed_offset"] = *hl.maxAnalyzedOffset
	}
	if hl.boundaryMaxScan != nil {
		source["boundary_max_scan"] = *hl.boundaryMaxScan
	}
	if hl.boundaryChars != nil {
		source["boundary_chars"] = *hl.boundaryChars
	}
	if hl.boundaryScannerType != nil {
		source["boundary_scanner"] = *hl.boundaryScannerType
	}
	if hl.boundaryScannerLocale != nil {
		source["boundary_scanner_locale"] = *hl.boundaryScannerLocale
	}
	if hl.highlighterType != nil {
		source["type"] = *hl.highlighterType
	}
	if hl.fragmenter != nil {
		source["fragmenter"] = *hl.fragmenter
	}
	if hl.highlightQuery != nil {
		src, err := hl.highlightQuery.Source()
		if err != nil {
			return nil, err
		}
		source["highlight_query"] = src
	}
	if hl.noMatchSize != nil {
		source["no_match_size"] = *hl.noMatchSize
	}
	if hl.phraseLimit != nil {
		source["phrase_limit"] = *hl.phraseLimit
	}
	if len(hl.options) > 0 {
		source["options"] = hl.options
	}
	if hl.forceSource != nil {
		source["force_source"] = *hl.forceSource
	}

	if len(hl.fields) > 0 {
		if hl.useExplicitFieldOrder {
			// Use a slice for the fields
			var fields []map[string]any
			for _, field := range hl.fields {
				src, err := field.Source()
				if err != nil {
					return nil, err
				}
				fmap := make(map[string]any)
				fmap[field.Name] = src
				fields = append(fields, fmap)
			}
			source["fields"] = fields
		} else {
			// Use a map for the fields
			fields := make(map[string]any)
			for _, field := range hl.fields {
				src, err := field.Source()
				if err != nil {
					return nil, err
				}
				fields[field.Name] = src
			}
			source["fields"] = fields
		}
	}

	return source, nil
}

// HighlighterField configures per-field highlighting options such as
// fragment size, pre/post tags, highlighter type, and boundary settings.
// It is used with [Highlight.Fields].
type HighlighterField struct {
	Name string

	preTags           []string
	postTags          []string
	fragmentSize      int
	fragmentOffset    int
	numOfFragments    int
	highlightFilter   *bool
	order             *string
	requireFieldMatch *bool
	boundaryMaxScan   int
	boundaryChars     []rune
	highlighterType   *string
	fragmenter        *string
	highlightQuery    Query
	noMatchSize       *int
	matchedFields     []string
	phraseLimit       *int
	options           map[string]any
	forceSource       *bool

	/*
		Name              string
		preTags           []string
		postTags          []string
		fragmentSize      int
		numOfFragments    int
		fragmentOffset    int
		highlightFilter   *bool
		order             string
		requireFieldMatch *bool
		boundaryMaxScan   int
		boundaryChars     []rune
		highlighterType   string
		fragmenter        string
		highlightQuery    Query
		noMatchSize       *int
		matchedFields     []string
		options           map[string]any
		forceSource       *bool
	*/
}

// NewHighlighterField creates a new HighlighterField for the given field name.
func NewHighlighterField(name string) *HighlighterField {
	return &HighlighterField{
		Name:            name,
		preTags:         make([]string, 0),
		postTags:        make([]string, 0),
		fragmentSize:    -1,
		fragmentOffset:  -1,
		numOfFragments:  -1,
		boundaryMaxScan: -1,
		boundaryChars:   make([]rune, 0),
		matchedFields:   make([]string, 0),
		options:         make(map[string]any),
	}
}

// PreTags sets the per-field HTML tags inserted before each highlighted fragment.
func (f *HighlighterField) PreTags(preTags ...string) *HighlighterField {
	f.preTags = append(f.preTags, preTags...)
	return f
}

// PostTags sets the per-field HTML tags inserted after each highlighted fragment.
func (f *HighlighterField) PostTags(postTags ...string) *HighlighterField {
	f.postTags = append(f.postTags, postTags...)
	return f
}

// FragmentSize sets the per-field fragment size in characters.
func (f *HighlighterField) FragmentSize(fragmentSize int) *HighlighterField {
	f.fragmentSize = fragmentSize
	return f
}

// FragmentOffset sets the character offset at which to start highlighting.
func (f *HighlighterField) FragmentOffset(fragmentOffset int) *HighlighterField {
	f.fragmentOffset = fragmentOffset
	return f
}

// NumOfFragments sets the maximum number of highlight fragments to return
// for this field.
func (f *HighlighterField) NumOfFragments(numOfFragments int) *HighlighterField {
	f.numOfFragments = numOfFragments
	return f
}

// HighlightFilter controls whether to highlight only filtered results for this field.
func (f *HighlighterField) HighlightFilter(highlightFilter bool) *HighlighterField {
	f.highlightFilter = &highlightFilter
	return f
}

// Order sets the order in which highlight fragments are returned for this field.
func (f *HighlighterField) Order(order string) *HighlighterField {
	f.order = &order
	return f
}

// RequireFieldMatch controls whether highlighting for this field only occurs
// when the query matches it.
func (f *HighlighterField) RequireFieldMatch(requireFieldMatch bool) *HighlighterField {
	f.requireFieldMatch = &requireFieldMatch
	return f
}

// BoundaryMaxScan sets the maximum distance to scan for a boundary character.
func (f *HighlighterField) BoundaryMaxScan(boundaryMaxScan int) *HighlighterField {
	f.boundaryMaxScan = boundaryMaxScan
	return f
}

// BoundaryChars sets the characters considered as boundaries for fragment building.
func (f *HighlighterField) BoundaryChars(boundaryChars ...rune) *HighlighterField {
	f.boundaryChars = append(f.boundaryChars, boundaryChars...)
	return f
}

// HighlighterType sets the per-field highlighter type, e.g. "unified", "plain", or "fvh".
func (f *HighlighterField) HighlighterType(highlighterType string) *HighlighterField {
	f.highlighterType = &highlighterType
	return f
}

// Fragmenter sets the per-field fragmenter, e.g. "simple" or "span".
func (f *HighlighterField) Fragmenter(fragmenter string) *HighlighterField {
	f.fragmenter = &fragmenter
	return f
}

// HighlightQuery sets a separate query used exclusively for highlighting this field.
func (f *HighlighterField) HighlightQuery(highlightQuery Query) *HighlighterField {
	f.highlightQuery = highlightQuery
	return f
}

// NoMatchSize sets the number of characters to return from the beginning
// of the field when there is no matching fragment.
func (f *HighlighterField) NoMatchSize(noMatchSize int) *HighlighterField {
	f.noMatchSize = &noMatchSize
	return f
}

// Options sets a generic map of per-field highlighter options.
func (f *HighlighterField) Options(options map[string]any) *HighlighterField {
	f.options = options
	return f
}

// MatchedFields lists the fields to query when using the Fast Vector Highlighter
// with matched_fields support.
func (f *HighlighterField) MatchedFields(matchedFields ...string) *HighlighterField {
	f.matchedFields = append(f.matchedFields, matchedFields...)
	return f
}

// PhraseLimit sets the maximum number of phrases the Fast Vector Highlighter
// considers when generating highlights.
func (f *HighlighterField) PhraseLimit(phraseLimit int) *HighlighterField {
	f.phraseLimit = &phraseLimit
	return f
}

// ForceSource controls whether to force source-based highlighting for this field.
func (f *HighlighterField) ForceSource(forceSource bool) *HighlighterField {
	f.forceSource = &forceSource
	return f
}

func (f *HighlighterField) Source() (any, error) {
	source := make(map[string]any)

	if len(f.preTags) > 0 {
		source["pre_tags"] = f.preTags
	}
	if len(f.postTags) > 0 {
		source["post_tags"] = f.postTags
	}
	if f.fragmentSize != -1 {
		source["fragment_size"] = f.fragmentSize
	}
	if f.numOfFragments != -1 {
		source["number_of_fragments"] = f.numOfFragments
	}
	if f.fragmentOffset != -1 {
		source["fragment_offset"] = f.fragmentOffset
	}
	if f.highlightFilter != nil {
		source["highlight_filter"] = *f.highlightFilter
	}
	if f.order != nil {
		source["order"] = *f.order
	}
	if f.requireFieldMatch != nil {
		source["require_field_match"] = *f.requireFieldMatch
	}
	if f.boundaryMaxScan != -1 {
		source["boundary_max_scan"] = f.boundaryMaxScan
	}
	if len(f.boundaryChars) > 0 {
		source["boundary_chars"] = f.boundaryChars
	}
	if f.highlighterType != nil {
		source["type"] = *f.highlighterType
	}
	if f.fragmenter != nil {
		source["fragmenter"] = *f.fragmenter
	}
	if f.highlightQuery != nil {
		src, err := f.highlightQuery.Source()
		if err != nil {
			return nil, err
		}
		source["highlight_query"] = src
	}
	if f.noMatchSize != nil {
		source["no_match_size"] = *f.noMatchSize
	}
	if len(f.matchedFields) > 0 {
		source["matched_fields"] = f.matchedFields
	}
	if f.phraseLimit != nil {
		source["phrase_limit"] = *f.phraseLimit
	}
	if len(f.options) > 0 {
		source["options"] = f.options
	}
	if f.forceSource != nil {
		source["force_source"] = *f.forceSource
	}

	return source, nil
}
