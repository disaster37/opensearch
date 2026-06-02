package querydsl

// SignificantTermsAggregation is an aggregation that returns interesting
// or unusual occurrences of terms in a set.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-significantterms-aggregation.html
type SignificantTermsAggregation struct {
	FieldVal         string                          `json:"field,omitempty"`
	SubAggs          map[string]Aggregation          `json:"-"`
	Meta             map[string]any                  `json:"meta,omitempty"`
	MinDocCount      *int                            `json:"min_doc_count,omitempty"`
	ShardMinDocCount *int                            `json:"shard_min_doc_count,omitempty"`
	RequiredSize     *int                            `json:"required_size,omitempty"`
	ShardSize        *int                            `json:"shard_size,omitempty"`
	Filter           Query                           `json:"-"`
	ExecutionHint    string                          `json:"execution_hint,omitempty"`
	Heuristic        SignificanceHeuristic           `json:"-"`
	IncludeExclude   *TermsAggregationIncludeExclude `json:"-"`
}

func NewSignificantTermsAggregation() *SignificantTermsAggregation {
	return &SignificantTermsAggregation{}
}

// WithField sets the field to run significant terms on.
func (a *SignificantTermsAggregation) WithField(field string) *SignificantTermsAggregation {
	a.FieldVal = field
	return a
}

// WithMinDocCount sets the minimum document count threshold.
func (a *SignificantTermsAggregation) WithMinDocCount(minDocCount int) *SignificantTermsAggregation {
	a.MinDocCount = &minDocCount
	return a
}

// WithShardMinDocCount sets the shard-level minimum document count threshold.
func (a *SignificantTermsAggregation) WithShardMinDocCount(shardMinDocCount int) *SignificantTermsAggregation {
	a.ShardMinDocCount = &shardMinDocCount
	return a
}

// WithRequiredSize sets the number of significant terms to return.
func (a *SignificantTermsAggregation) WithRequiredSize(requiredSize int) *SignificantTermsAggregation {
	a.RequiredSize = &requiredSize
	return a
}

// WithShardSize sets the number of significant terms to fetch per shard.
func (a *SignificantTermsAggregation) WithShardSize(shardSize int) *SignificantTermsAggregation {
	a.ShardSize = &shardSize
	return a
}

// WithExecutionHint sets the execution hint for the aggregation.
func (a *SignificantTermsAggregation) WithExecutionHint(hint string) *SignificantTermsAggregation {
	a.ExecutionHint = hint
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *SignificantTermsAggregation) SubAggregation(name string, subAggregation Aggregation) *SignificantTermsAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

// WithMeta sets meta data for the aggregation.
func (a *SignificantTermsAggregation) WithMeta(metaData map[string]any) *SignificantTermsAggregation {
	a.Meta = metaData
	return a
}

// BackgroundFilter sets a background filter query.
func (a *SignificantTermsAggregation) BackgroundFilter(filter Query) *SignificantTermsAggregation {
	a.Filter = filter
	return a
}

// SignificanceHeuristic sets the significance heuristic.
func (a *SignificantTermsAggregation) SignificanceHeuristic(heuristic SignificanceHeuristic) *SignificantTermsAggregation {
	a.Heuristic = heuristic
	return a
}

// Include sets a regexp pattern for included term values.
func (a *SignificantTermsAggregation) Include(regexp string) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Include = regexp
	return a
}

// IncludeValues sets an explicit list of values to include.
func (a *SignificantTermsAggregation) IncludeValues(values ...any) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.IncludeValues = append(a.IncludeExclude.IncludeValues, values...)
	return a
}

// Exclude sets a regexp pattern for excluded term values.
func (a *SignificantTermsAggregation) Exclude(regexp string) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Exclude = regexp
	return a
}

// ExcludeValues sets an explicit list of values to exclude.
func (a *SignificantTermsAggregation) ExcludeValues(values ...any) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.ExcludeValues = append(a.IncludeExclude.ExcludeValues, values...)
	return a
}

// Partition sets the partition number for partitioned term filtering.
func (a *SignificantTermsAggregation) Partition(p int) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Partition = p
	return a
}

// NumPartitions sets the total number of partitions for partitioned term filtering.
func (a *SignificantTermsAggregation) NumPartitions(n int) *SignificantTermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.NumPartitions = n
	return a
}

// SetIncludeExclude sets the include/exclude filter directly.
func (a *SignificantTermsAggregation) SetIncludeExclude(includeExclude *TermsAggregationIncludeExclude) *SignificantTermsAggregation {
	a.IncludeExclude = includeExclude
	return a
}

func (a SignificantTermsAggregation) Source() (any, error) {
	body := make(map[string]any)

	if a.FieldVal != "" {
		body["field"] = a.FieldVal
	}
	if a.RequiredSize != nil {
		body["size"] = *a.RequiredSize
	}
	if a.ShardSize != nil {
		body["shard_size"] = *a.ShardSize
	}
	if a.MinDocCount != nil {
		body["min_doc_count"] = *a.MinDocCount
	}
	if a.ShardMinDocCount != nil {
		body["shard_min_doc_count"] = *a.ShardMinDocCount
	}
	if a.ExecutionHint != "" {
		body["execution_hint"] = a.ExecutionHint
	}
	if a.Filter != nil {
		src, err := a.Filter.Source()
		if err != nil {
			return nil, err
		}
		body["background_filter"] = src
	}
	if a.Heuristic != nil {
		name := a.Heuristic.Name()
		src, err := a.Heuristic.Source()
		if err != nil {
			return nil, err
		}
		body[name] = src
	}

	if ie := a.IncludeExclude; ie != nil {
		if err := ie.MergeInto(body); err != nil {
			return nil, err
		}
	}

	return sourceAgg("significant_terms", body, a.SubAggs, a.Meta, nil)
}

// -- Significance heuristics --

// SignificanceHeuristic is the interface for all significance heuristics.
type SignificanceHeuristic interface {
	Name() string
	Source() (any, error)
}

// -- Chi Square --

// ChiSquareSignificanceHeuristic implements Chi square as described
// in "Information Retrieval", Manning et al., Chapter 13.5.2.
type ChiSquareSignificanceHeuristic struct {
	BackgroundIsSupersetVal *bool
	IncludeNegativesVal     *bool
}

func NewChiSquareSignificanceHeuristic() *ChiSquareSignificanceHeuristic {
	return &ChiSquareSignificanceHeuristic{}
}

// WithBackgroundIsSuperset sets whether the background corpus is a superset of the foreground.
func (sh *ChiSquareSignificanceHeuristic) WithBackgroundIsSuperset(v bool) *ChiSquareSignificanceHeuristic {
	sh.BackgroundIsSupersetVal = &v
	return sh
}

// WithIncludeNegatives sets whether to include terms with negative scores.
func (sh *ChiSquareSignificanceHeuristic) WithIncludeNegatives(v bool) *ChiSquareSignificanceHeuristic {
	sh.IncludeNegativesVal = &v
	return sh
}

func (sh ChiSquareSignificanceHeuristic) Name() string {
	return "chi_square"
}

func (sh ChiSquareSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	if sh.BackgroundIsSupersetVal != nil {
		source["background_is_superset"] = *sh.BackgroundIsSupersetVal
	}
	if sh.IncludeNegativesVal != nil {
		source["include_negatives"] = *sh.IncludeNegativesVal
	}
	return source, nil
}

// -- GND --

// GNDSignificanceHeuristic implements the "Google Normalized Distance"
// as described in "The Google Similarity Distance", Cilibrasi and Vitanyi, 2007.
type GNDSignificanceHeuristic struct {
	BackgroundIsSupersetVal *bool
}

func NewGNDSignificanceHeuristic() *GNDSignificanceHeuristic {
	return &GNDSignificanceHeuristic{}
}

// WithBackgroundIsSuperset sets whether the background corpus is a superset of the foreground.
func (sh *GNDSignificanceHeuristic) WithBackgroundIsSuperset(v bool) *GNDSignificanceHeuristic {
	sh.BackgroundIsSupersetVal = &v
	return sh
}

func (sh GNDSignificanceHeuristic) Name() string {
	return "gnd"
}

func (sh GNDSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	if sh.BackgroundIsSupersetVal != nil {
		source["background_is_superset"] = *sh.BackgroundIsSupersetVal
	}
	return source, nil
}

// -- JLH Score --

// JLHScoreSignificanceHeuristic implements the JLH score.
type JLHScoreSignificanceHeuristic struct{}

func NewJLHScoreSignificanceHeuristic() *JLHScoreSignificanceHeuristic {
	return &JLHScoreSignificanceHeuristic{}
}

func (sh JLHScoreSignificanceHeuristic) Name() string {
	return "jlh"
}

func (sh JLHScoreSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	return source, nil
}

// -- Mutual Information --

// MutualInformationSignificanceHeuristic implements Mutual information
// as described in "Information Retrieval", Manning et al., Chapter 13.5.1.
type MutualInformationSignificanceHeuristic struct {
	BackgroundIsSupersetVal *bool
	IncludeNegativesVal     *bool
}

func NewMutualInformationSignificanceHeuristic() *MutualInformationSignificanceHeuristic {
	return &MutualInformationSignificanceHeuristic{}
}

// WithBackgroundIsSuperset sets whether the background corpus is a superset of the foreground.
func (sh *MutualInformationSignificanceHeuristic) WithBackgroundIsSuperset(v bool) *MutualInformationSignificanceHeuristic {
	sh.BackgroundIsSupersetVal = &v
	return sh
}

// WithIncludeNegatives sets whether to include terms with negative scores.
func (sh *MutualInformationSignificanceHeuristic) WithIncludeNegatives(v bool) *MutualInformationSignificanceHeuristic {
	sh.IncludeNegativesVal = &v
	return sh
}

func (sh MutualInformationSignificanceHeuristic) Name() string {
	return "mutual_information"
}

func (sh MutualInformationSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	if sh.BackgroundIsSupersetVal != nil {
		source["background_is_superset"] = *sh.BackgroundIsSupersetVal
	}
	if sh.IncludeNegativesVal != nil {
		source["include_negatives"] = *sh.IncludeNegativesVal
	}
	return source, nil
}

// -- Percentage Score --

// PercentageScoreSignificanceHeuristic implements the percentage score algorithm.
type PercentageScoreSignificanceHeuristic struct{}

func NewPercentageScoreSignificanceHeuristic() *PercentageScoreSignificanceHeuristic {
	return &PercentageScoreSignificanceHeuristic{}
}

func (sh PercentageScoreSignificanceHeuristic) Name() string {
	return "percentage"
}

func (sh PercentageScoreSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	return source, nil
}

// -- Script --

// ScriptSignificanceHeuristic implements a scripted significance heuristic.
type ScriptSignificanceHeuristic struct {
	ScriptVal *Script
}

func NewScriptSignificanceHeuristic() *ScriptSignificanceHeuristic {
	return &ScriptSignificanceHeuristic{}
}

// WithScript sets the script for the heuristic.
func (sh *ScriptSignificanceHeuristic) WithScript(script *Script) *ScriptSignificanceHeuristic {
	sh.ScriptVal = script
	return sh
}

func (sh ScriptSignificanceHeuristic) Name() string {
	return "script_heuristic"
}

func (sh ScriptSignificanceHeuristic) Source() (any, error) {
	source := make(map[string]any)
	if sh.ScriptVal != nil {
		source["script"], _ = sh.ScriptVal.Source()
	}
	return source, nil
}
