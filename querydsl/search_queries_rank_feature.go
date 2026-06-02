package querydsl

type RankFeatureQuery struct {
	Field         string
	ScoreFunction RankFeatureScoreFunction
	Boost         *float64
	QueryName     string
}

func NewRankFeatureQuery(field string) *RankFeatureQuery {
	return &RankFeatureQuery{Field: field}
}

// WithScoreFunction sets the score function used to compute the rank feature score.
func (q *RankFeatureQuery) WithScoreFunction(fn RankFeatureScoreFunction) *RankFeatureQuery {
	q.ScoreFunction = fn
	return q
}

// WithBoost sets the boost factor for this query.
func (q *RankFeatureQuery) WithBoost(boost float64) *RankFeatureQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *RankFeatureQuery) WithQueryName(name string) *RankFeatureQuery {
	q.QueryName = name
	return q
}

func (q RankFeatureQuery) Source() (any, error) {
	params := map[string]any{"field": q.Field}
	if q.ScoreFunction != nil {
		src, err := q.ScoreFunction.Source()
		if err != nil {
			return nil, err
		}
		params[q.ScoreFunction.Name()] = src
	}
	if q.Boost != nil {
		params["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"rank_feature": params}, nil
}

type RankFeatureScoreFunction interface {
	Name() string
	Source() (any, error)
}

type RankFeatureLogScoreFunction struct {
	ScalingFactor float64 `json:"scaling_factor"`
}

func NewRankFeatureLogScoreFunction(scalingFactor float64) RankFeatureLogScoreFunction {
	return RankFeatureLogScoreFunction{ScalingFactor: scalingFactor}
}

func (f RankFeatureLogScoreFunction) Name() string { return "log" }

func (f RankFeatureLogScoreFunction) Source() (any, error) {
	return marshalStruct(f)
}

type RankFeatureSaturationScoreFunction struct {
	Pivot *float64 `json:"pivot,omitempty"`
}

func NewRankFeatureSaturationScoreFunction() RankFeatureSaturationScoreFunction {
	return RankFeatureSaturationScoreFunction{}
}

func (f RankFeatureSaturationScoreFunction) Name() string { return "saturation" }

func (f RankFeatureSaturationScoreFunction) Source() (any, error) {
	return marshalStruct(f)
}

type RankFeatureSigmoidScoreFunction struct {
	Pivot    float64 `json:"pivot"`
	Exponent float64 `json:"exponent"`
}

func NewRankFeatureSigmoidScoreFunction(pivot, exponent float64) RankFeatureSigmoidScoreFunction {
	return RankFeatureSigmoidScoreFunction{Pivot: pivot, Exponent: exponent}
}

func (f RankFeatureSigmoidScoreFunction) Name() string { return "sigmoid" }

func (f RankFeatureSigmoidScoreFunction) Source() (any, error) {
	return marshalStruct(f)
}

type RankFeatureLinearScoreFunction struct{}

func NewRankFeatureLinearScoreFunction() RankFeatureLinearScoreFunction {
	return RankFeatureLinearScoreFunction{}
}

func (f RankFeatureLinearScoreFunction) Name() string { return "linear" }

func (f RankFeatureLinearScoreFunction) Source() (any, error) {
	return map[string]any{}, nil
}
