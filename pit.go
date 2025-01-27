// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// PointInTime is a lightweight view into the state of the data that existed
// when initiated. It can be created with OpenPointInTime API and be used
// when searching, e.g. in Search API or with SearchSource.
type PointInTime struct {
	// Id that uniquely identifies the point in time, as created with the
	// OpenPointInTime API.
	Id string `json:"id,omitempty"`
	// KeepAlive is the time for which this specific PointInTime will be
	// kept alive by Opensearch.
	KeepAlive string `json:"keep_alive,omitempty"`
}

// NewPointInTime creates a new PointInTime.
func NewPointInTime(id string) *PointInTime {
	return &PointInTime{
		Id: id,
	}
}

// NewPointInTimeWithKeepAlive creates a new PointInTime with the given
// time to keep alive.
func NewPointInTimeWithKeepAlive(id, keepAlive string) *PointInTime {
	return &PointInTime{
		Id:        id,
		KeepAlive: keepAlive,
	}
}

// Source generates the JSON serializable fragment for the PointInTime.
func (pit *PointInTime) Source() (interface{}, error) {
	if pit == nil {
		return nil, nil
	}
	m := map[string]interface{}{
		"id": pit.Id,
	}
	if pit.KeepAlive != "" {
		m["keep_alive"] = pit.KeepAlive
	}
	return m, nil
}
