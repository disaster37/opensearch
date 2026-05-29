package api

// IngestDeletePipelineResponse represents the acknowledgment response from
// deleting an ingest pipeline. The Acknowledged field indicates whether the
// delete was acknowledged by the cluster.
type IngestDeletePipelineResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index,omitempty"`
}

// IngestGetPipelineResponse maps pipeline IDs to their definitions.
// Each value is an IngestGetPipeline containing the processors, description,
// and optional version of the pipeline.
type IngestGetPipelineResponse map[string]*IngestGetPipeline

// IngestGetPipeline represents the definition of a single ingest pipeline,
// including its description, ordered list of processors, optional version,
// and optional on-failure processor chain.
type IngestGetPipeline struct {
	Description string           `json:"description"`
	Processors  []map[string]any `json:"processors"`
	Version     int64            `json:"version,omitempty"`
	OnFailure   []map[string]any `json:"on_failure,omitempty"`
}

// IngestPutPipelineResponse represents the acknowledgment response from
// creating or updating an ingest pipeline. The Acknowledged field indicates
// whether the operation was acknowledged by the cluster.
type IngestPutPipelineResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index,omitempty"`
}

// IngestSimulatePipelineResponse represents the result of simulating an
// ingest pipeline. It contains the per-document simulation results showing
// how each processor transforms the document.
type IngestSimulatePipelineResponse struct {
	Docs []*IngestSimulateDocumentResult `json:"docs"`
}

// IngestSimulateDocumentResult represents the simulation result for a single
// document, including the final document state and the output of each processor.
type IngestSimulateDocumentResult struct {
	Doc              map[string]any                   `json:"doc"`
	ProcessorResults []*IngestSimulateProcessorResult `json:"processor_results"`
}

// IngestSimulateProcessorResult represents the output of a single processor
// during pipeline simulation, including the processor tag and resulting document.
type IngestSimulateProcessorResult struct {
	ProcessorTag string         `json:"tag"`
	Doc          map[string]any `json:"doc"`
}

// IngestProcessorGrokResponse is returned by the grok processor endpoint.
// The API returns {"patterns": {"BAC": "...", ...}} where each key is a
// grok pattern name and each value is its regex definition.
type IngestProcessorGrokResponse struct {
	Patterns map[string]string `json:"patterns"`
}
