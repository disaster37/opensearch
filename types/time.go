package types

import (
	json "github.com/goccy/go-json"
	"time"
)

// UnixMilliTime is a [time.Time] that serializes to/from an integer
// representing Unix milliseconds. Used by several plugin APIs (CCR, Alerting)
// whose responses report timestamps as millisecond integers rather than ISO strings.
//
// A zero value marshals to 0; an unmarshalled 0 becomes the zero time.
type UnixMilliTime struct {
	time.Time
}

// UnmarshalJSON decodes an integer millisecond timestamp (or "null", "", "0")
// into the embedded [time.Time]. Zero values are left as the Go zero time.
func (t *UnixMilliTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" || s == `""` || s == "0" {
		return nil
	}
	var ms int64
	if err := json.Unmarshal(b, &ms); err != nil {
		return err
	}
	if ms == 0 {
		return nil
	}
	t.Time = time.Unix(0, ms*int64(time.Millisecond))
	return nil
}

// MarshalJSON encodes the time as an integer millisecond count, or 0 if
// the time is the zero value.
func (t UnixMilliTime) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte("0"), nil
	}
	return json.Marshal(t.Time.UnixMilli())
}
