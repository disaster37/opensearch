package opensearch

import (
	"encoding/json"
	"fmt"
	"time"
)

// UnixMilliTime is a time.Time that marshals to and from Unix milliseconds.
type UnixMilliTime struct {
	time.Time
}

func (u *UnixMilliTime) UnmarshalJSON(b []byte) error {
	var timestamp int64
	err := json.Unmarshal(b, &timestamp)
	if err != nil {
		return err
	}
	u.Time = time.UnixMilli(timestamp)
	return nil
}

func (u UnixMilliTime) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", u.UnixMilli())), nil
}
