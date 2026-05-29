package querydsl

type MovFnAggregation struct {
	Script       *Script
	Format       string
	GapPolicy    string
	Window       int
	BucketsPaths []string
	Meta         map[string]any
}

func NewMovFnAggregation(bucketPath string, script *Script, window int) MovFnAggregation {
	return MovFnAggregation{BucketsPaths: []string{bucketPath}, Script: script, Window: window}
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

func NewMovAvgAggregation() MovAvgAggregation { return MovAvgAggregation{} }

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

func NewEWMAMovAvgModel() EWMAMovAvgModel { return EWMAMovAvgModel{} }
func (m EWMAMovAvgModel) Name() string    { return "ewma" }
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

func NewHoltLinearMovAvgModel() HoltLinearMovAvgModel { return HoltLinearMovAvgModel{} }
func (m HoltLinearMovAvgModel) Name() string          { return "holt" }
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

func NewHoltWintersMovAvgModel() HoltWintersMovAvgModel { return HoltWintersMovAvgModel{} }
func (m HoltWintersMovAvgModel) Name() string           { return "holt_winters" }
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

func NewLinearMovAvgModel() LinearMovAvgModel        { return LinearMovAvgModel{} }
func (m LinearMovAvgModel) Name() string             { return "linear" }
func (m LinearMovAvgModel) Settings() map[string]any { return nil }

type SimpleMovAvgModel struct{}

func NewSimpleMovAvgModel() SimpleMovAvgModel        { return SimpleMovAvgModel{} }
func (m SimpleMovAvgModel) Name() string             { return "simple" }
func (m SimpleMovAvgModel) Settings() map[string]any { return nil }
