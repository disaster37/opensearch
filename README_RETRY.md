# Retry and Backoff Configuration

This document explains how to configure retry and backoff settings for the OpenSearch client to handle transient errors like connection resets, timeouts, and server errors.

## Configuration Options

The `Config` struct now includes the following retry-related fields:

- `RetryCount`: Maximum number of retry attempts (default: 0 = no retries)
- `RetryWaitTime`: Minimum wait time between retries (default: 100ms)
- `RetryMaxWaitTime`: Maximum wait time between retries (default: 2s)
- `RetryConditions`: Custom retry conditions (optional)

## Default Retry Behavior

When `RetryCount > 0`, the client automatically retries on:

1. **Network errors** including:
   - "connection reset by peer" (your specific error case)
   - "connection refused"
   - "timeout" / "i/o timeout"
   - "network is unreachable"
   - "broken pipe"
   - "EOF"

2. **HTTP status codes**:
   - 429 Too Many Requests
   - 5xx server errors (except 501 Not Implemented)

## Usage Examples

### Basic Retry Configuration

```go
client, err := opensearch.New(&opensearch.Config{
    URL:      "https://localhost:9200",
    Username: "admin",
    Password: "admin",
    RetryCount: 3,
    RetryWaitTime: 500 * time.Millisecond,
    RetryMaxWaitTime: 5 * time.Second,
}, logger)
```

### Advanced Retry with Custom Conditions

```go
client, err := opensearch.New(&opensearch.Config{
    URL:      "https://localhost:9200",
    Username: "admin",
    Password: "admin",
    RetryCount: 3,
    RetryWaitTime: 500 * time.Millisecond,
    RetryMaxWaitTime: 5 * time.Second,
    // Use built-in conditions optimized for Point-in-Time searches
    RetryConditions: opensearch.PITSearchRetryConditions(),
}, logger)
```

### Custom Retry Conditions

```go
customConditions := []resty.RetryConditionFunc{
    // Add your custom logic here
    func(res *resty.Response, err error) bool {
        if err != nil && strings.Contains(err.Error(), "specific error") {
            return true
        }
        return false
    },
}

client, err := opensearch.New(&opensearch.Config{
    URL:              "https://localhost:9200",
    RetryCount:       2,
    RetryConditions:  customConditions,
}, logger)
```

## Handling Your Specific Error

For the error you mentioned:
```
ERROR HTTP request failed error=read tcp 172.18.0.2:54492->10.221.84.160:443: read: connection reset by peer service=search
```

This will be automatically handled by the default retry conditions when you set `RetryCount > 0`. The client will retry the request up to the specified number of times with exponential backoff.

## Built-in Helper Functions

- `opensearch.DefaultRetryConditions()`: Returns standard retry conditions
- `opensearch.PITSearchRetryConditions()`: Returns conditions optimized for long-running Point-in-Time searches, including OpenSearch-specific transient errors like:
  - `search_phase_execution_exception`
  - `too_many_buckets_exception`
  - `circuit_breaking_exception`

## Backoff Strategy

The client uses capped exponential backoff with jitter (based on AWS recommendations):
- Wait time doubles with each retry attempt
- Random jitter is added to prevent thundering herd
- Wait time is capped at `RetryMaxWaitTime`