package querydsl

type BucketSortAggregation struct {
	Sorters   []Sorter
	From      int
	Size      int
	GapPolicy string
	Meta      map[string]any
}

func NewBucketSortAggregation() *BucketSortAggregation {
	return &BucketSortAggregation{Size: -1}
}

// WithSorters sets the sorters for the bucket sort aggregation.
func (a *BucketSortAggregation) WithSorters(sorters ...Sorter) *BucketSortAggregation {
	a.Sorters = sorters
	return a
}

// WithFrom sets the from value for the bucket sort aggregation.
func (a *BucketSortAggregation) WithFrom(from int) *BucketSortAggregation {
	a.From = from
	return a
}

// WithSize sets the size for the bucket sort aggregation.
func (a *BucketSortAggregation) WithSize(size int) *BucketSortAggregation {
	a.Size = size
	return a
}

// WithGapPolicy sets the gap policy for the bucket sort aggregation.
func (a *BucketSortAggregation) WithGapPolicy(policy string) *BucketSortAggregation {
	a.GapPolicy = policy
	return a
}

// WithMeta sets the meta for the bucket sort aggregation.
func (a *BucketSortAggregation) WithMeta(meta map[string]any) *BucketSortAggregation {
	a.Meta = meta
	return a
}

func (a BucketSortAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.From != 0 {
		body["from"] = a.From
	}
	if a.Size != -1 {
		body["size"] = a.Size
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if len(a.Sorters) > 0 {
		sorters := make([]any, len(a.Sorters))
		for i, s := range a.Sorters {
			src, err := s.Source()
			if err != nil {
				return nil, err
			}
			sorters[i] = src
		}
		body["sort"] = sorters
	}
	if len(a.Meta) > 0 {
		body["meta"] = a.Meta
	}
	return map[string]any{"bucket_sort": body}, nil
}
