package api

// AdDetector represents an anomaly detector definition.
type AdDetector struct {
	Name               string           `json:"name"`
	Description        *string          `json:"description,omitempty"`
	TimeField          string           `json:"time_field,omitempty"`
	Indices            []string         `json:"indices,omitempty"`
	FeatureAttributes  []map[string]any `json:"feature_attributes,omitempty"`
	DetectionInterval  map[string]any   `json:"detection_interval,omitempty"`
	DetectionDateRange map[string]any   `json:"detection_date_range,omitempty"`
	FilterQuery        map[string]any   `json:"filter_query,omitempty"`
	UiMetadata         map[string]any   `json:"ui_metadata,omitempty"`
	SchemaVersion      *int64           `json:"schema_version,omitempty"`
	LastUpdateTime     *int64           `json:"last_update_time,omitempty"`
	CurState           *string          `json:"cur_state,omitempty"`
	StateError         *string          `json:"state_error,omitempty"`
}

// AdIndexDetectorResponse represents the response from creating, updating, or
// getting an anomaly detector. The same envelope shape is returned by both
// the index/update and the get endpoints, so it is reused for GetDetector.
type AdIndexDetectorResponse struct {
	Id             string     `json:"_id,omitempty"`
	Version        int64      `json:"_version,omitempty"`
	SequenceNumber int64      `json:"_seq_no,omitempty"`
	PrimaryTerm    int64      `json:"_primary_term,omitempty"`
	Detector       AdDetector `json:"anomaly_detector,omitempty"`
}

// AdGetDetectorResponse represents the response from getting an anomaly detector.
type AdGetDetectorResponse struct {
	Id             string     `json:"_id,omitempty"`
	Version        int64      `json:"_version,omitempty"`
	SequenceNumber int64      `json:"_seq_no,omitempty"`
	PrimaryTerm    int64      `json:"_primary_term,omitempty"`
	Detector       AdDetector `json:"anomaly_detector,omitempty"`
}

// AdDeleteDetectorResponse represents the response from deleting an anomaly detector.
type AdDeleteDetectorResponse struct {
	Index   *string `json:"_index,omitempty"`
	ID      *string `json:"_id,omitempty"`
	Version *int64  `json:"_version,omitempty"`
	Result  *string `json:"result,omitempty"`
}

// AdExecuteDetectorResponse represents the response from executing an anomaly detector.
type AdExecuteDetectorResponse struct {
	AnomalyGrade  float64          `json:"anomaly_grade,omitempty"`
	Confidence    float64          `json:"confidence,omitempty"`
	DataStartTime string           `json:"data_start_time,omitempty"`
	DataEndTime   string           `json:"data_end_time,omitempty"`
	Features      []map[string]any `json:"features,omitempty"`
}

// AdPreviewDetectorResponse represents the response from previewing an anomaly detector.
type AdPreviewDetectorResponse struct {
	AnomalyResult []map[string]any `json:"anomaly_result,omitempty"`
}

// AdSearchDetectorsResponse wraps the search detectors hits.
type AdSearchDetectorsResponse struct {
	Total     int64         `json:"total_anomaly_detectors"`
	Detectors []AdSearchHit `json:"anomaly_detectors"`
}

// AdSearchHit represents a single detector search hit.
type AdSearchHit struct {
	Id     string     `json:"_id"`
	Source AdDetector `json:"detector"`
}

// AdSearchResultsResponse wraps the search anomaly results. The same envelope
// shape (`total_results` + `results`) is returned by both the regular and the
// top-anomalies endpoints, so SearchTopResults also returns this type.
type AdSearchResultsResponse struct {
	Total   int64            `json:"total_results"`
	Results []map[string]any `json:"results"`
}

// AdStatsResponse represents anomalies detection stats.
type AdStatsResponse map[string]any

// AdValidateResponse represents the result of validating an anomaly detector.
type AdValidateResponse struct {
	Error   *string `json:"error,omitempty"`
	Message *string `json:"message,omitempty"`
}
