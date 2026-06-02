package querydsl

type MovFnAggregation struct {
	Script       *Script
	Format       string
	GapPolicy    string
	Window       int
	BucketsPaths []string
	Meta         map[string]any
}

func NewMovFnAggregation(bucketPath string, script *Script, window int) *MovFnAggregation {
	return &MovFnAggregation{BucketsPaths: []string{bucketPath}, Script: script, Window: window}
}

// WithScript sets the script for the moving function aggregation.
func (a *MovFnAggregation) WithScript(script *Script) *MovFnAggregation {
	a.Script = script
	return a
}

// WithFormat sets the format for the moving function aggregation.
func (a *MovFnAggregation) WithFormat(format string) *MovFnAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the moving function aggregation.
func (a *MovFnAggregation) WithGapPolicy(policy string) *MovFnAggregation {
	a.GapPolicy = policy
	return a
}

// WithWindow sets the window size for the moving function aggregation.
func (a *MovFnAggregation) WithWindow(window int) *MovFnAggregation {
	a.Window = window
	return a
}

// WithBucketsPaths sets the buckets paths for the moving function aggregation.
func (a *MovFnAggregation) WithBucketsPaths(paths ...string) *MovFnAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the moving function aggregation.
func (a *MovFnAggregation) WithMeta(meta map[string]any) *MovFnAggregation {
	a.Meta = meta
	return a
}

func (a MovFnAggregation) Source() (any, error) {
	body := map[string]any{"window": a.Window}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	return sourcePipeline("moving_fn", body, a.Meta, a.Script)
}

// MovAvgAggregation is deprecated in favour of MovFnAggregation.
type MovAvgAggregation struct {
	Format       string
	GapPolicy    string
	Model        MovAvgModel
	Window       *int
	Predict      *int
	Minimize     *bool
	BucketsPaths []string
	Meta         map[string]any
}

func NewMovAvgAggregation() *MovAvgAggregation { return &MovAvgAggregation{} }

// WithFormat sets the format for the moving average aggregation.
func (a *MovAvgAggregation) WithFormat(format string) *MovAvgAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the moving average aggregation.
func (a *MovAvgAggregation) WithGapPolicy(policy string) *MovAvgAggregation {
	a.GapPolicy = policy
	return a
}

// WithModel sets the model for the moving average aggregation.
func (a *MovAvgAggregation) WithModel(model MovAvgModel) *MovAvgAggregation {
	a.Model = model
	return a
}

// WithWindow sets the window size for the moving average aggregation.
func (a *MovAvgAggregation) WithWindow(window int) *MovAvgAggregation {
	a.Window = &window
	return a
}

// WithPredict sets the number of predictions for the moving average aggregation.
func (a *MovAvgAggregation) WithPredict(predict int) *MovAvgAggregation {
	a.Predict = &predict
	return a
}

// WithMinimize sets whether to minimize the moving average aggregation.
func (a *MovAvgAggregation) WithMinimize(minimize bool) *MovAvgAggregation {
	a.Minimize = &minimize
	return a
}

// WithBucketsPaths sets the buckets paths for the moving average aggregation.
func (a *MovAvgAggregation) WithBucketsPaths(paths ...string) *MovAvgAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the moving average aggregation.
func (a *MovAvgAggregation) WithMeta(meta map[string]any) *MovAvgAggregation {
	a.Meta = meta
	return a
}

func (a MovAvgAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if a.Model != nil {
		body["model"] = a.Model.Name()
		if settings := a.Model.Settings(); len(settings) > 0 {
			body["settings"] = settings
		}
	}
	if a.Window != nil {
		body["window"] = *a.Window
	}
	if a.Predict != nil {
		body["predict"] = *a.Predict
	}
	if a.Minimize != nil {
		body["minimize"] = *a.Minimize
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("moving_avg", body, a.Meta, nil)
}

type MovAvgModel interface {
	Name() string
	Settings() map[string]any
}

type EWMAMovAvgModel struct {
	Alpha *float64
}

func NewEWMAMovAvgModel() *EWMAMovAvgModel { return &EWMAMovAvgModel{} }
func (m EWMAMovAvgModel) Name() string     { return "ewma" }

// WithAlpha sets the alpha parameter for the EWMA model.
func (m *EWMAMovAvgModel) WithAlpha(alpha float64) *EWMAMovAvgModel {
	m.Alpha = &alpha
	return m
}

func (m EWMAMovAvgModel) Settings() map[string]any {
	s := map[string]any{}
	if m.Alpha != nil {
		s["alpha"] = *m.Alpha
	}
	return s
}

type HoltLinearMovAvgModel struct {
	Alpha *float64
	Beta  *float64
}

func NewHoltLinearMovAvgModel() *HoltLinearMovAvgModel { return &HoltLinearMovAvgModel{} }
func (m HoltLinearMovAvgModel) Name() string           { return "holt" }

// WithAlpha sets the alpha parameter for the Holt Linear model.
func (m *HoltLinearMovAvgModel) WithAlpha(alpha float64) *HoltLinearMovAvgModel {
	m.Alpha = &alpha
	return m
}

// WithBeta sets the beta parameter for the Holt Linear model.
func (m *HoltLinearMovAvgModel) WithBeta(beta float64) *HoltLinearMovAvgModel {
	m.Beta = &beta
	return m
}

func (m HoltLinearMovAvgModel) Settings() map[string]any {
	s := map[string]any{}
	if m.Alpha != nil {
		s["alpha"] = *m.Alpha
	}
	if m.Beta != nil {
		s["beta"] = *m.Beta
	}
	return s
}

type HoltWintersMovAvgModel struct {
	Alpha           *float64
	Beta            *float64
	Gamma           *float64
	Period          *int
	SeasonalityType string
	Pad             *bool
}

func NewHoltWintersMovAvgModel() *HoltWintersMovAvgModel { return &HoltWintersMovAvgModel{} }
func (m HoltWintersMovAvgModel) Name() string            { return "holt_winters" }

// WithAlpha sets the alpha parameter for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithAlpha(alpha float64) *HoltWintersMovAvgModel {
	m.Alpha = &alpha
	return m
}

// WithBeta sets the beta parameter for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithBeta(beta float64) *HoltWintersMovAvgModel {
	m.Beta = &beta
	return m
}

// WithGamma sets the gamma parameter for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithGamma(gamma float64) *HoltWintersMovAvgModel {
	m.Gamma = &gamma
	return m
}

// WithPeriod sets the period for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithPeriod(period int) *HoltWintersMovAvgModel {
	m.Period = &period
	return m
}

// WithSeasonalityType sets the seasonality type for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithSeasonalityType(t string) *HoltWintersMovAvgModel {
	m.SeasonalityType = t
	return m
}

// WithPad sets the pad option for the Holt-Winters model.
func (m *HoltWintersMovAvgModel) WithPad(pad bool) *HoltWintersMovAvgModel {
	m.Pad = &pad
	return m
}

func (m HoltWintersMovAvgModel) Settings() map[string]any {
	s := map[string]any{}
	if m.Alpha != nil {
		s["alpha"] = *m.Alpha
	}
	if m.Beta != nil {
		s["beta"] = *m.Beta
	}
	if m.Gamma != nil {
		s["gamma"] = *m.Gamma
	}
	if m.Period != nil {
		s["period"] = *m.Period
	}
	if m.Pad != nil {
		s["pad"] = *m.Pad
	}
	if m.SeasonalityType != "" {
		s["type"] = m.SeasonalityType
	}
	return s
}

type LinearMovAvgModel struct{}

func NewLinearMovAvgModel() *LinearMovAvgModel        { return &LinearMovAvgModel{} }
func (m LinearMovAvgModel) Name() string              { return "linear" }
func (m LinearMovAvgModel) Settings() map[string]any  { return nil }

type SimpleMovAvgModel struct{}

func NewSimpleMovAvgModel() *SimpleMovAvgModel        { return &SimpleMovAvgModel{} }
func (m SimpleMovAvgModel) Name() string              { return "simple" }
func (m SimpleMovAvgModel) Settings() map[string]any  { return nil }
