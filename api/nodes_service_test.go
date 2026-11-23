// nodes_service_test.go contains unit tests for DefaultNodesService.
// Tests use httptest.NewServer to mock the OpenSearch nodes endpoints, covering
// success paths, nil-request handling, query-parameter handling, error branches,
// and the resource_usage_stats model helpers (FloatString and the utilization
// accessors).
package api

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitNodesService_Info(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/_all/_all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/node1/os", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","roles":["cluster_manager"]}}}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes all metrics", func(t *testing.T) {
		resp, err := svc.Info(ctx, &NodesInfoRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})

	t.Run("specific node with metric", func(t *testing.T) {
		resp, err := svc.Info(ctx, &NodesInfoRequest{NodeIds: []string{"node1"}, Metrics: []string{"os"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
		assert.Equal(t, "node1", resp.Nodes["n1"].Name)
	})

	t.Run("nil request uses defaults", func(t *testing.T) {
		resp, err := svc.Info(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})
}

func TestUnitNodesService_Stats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/node1/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","roles":["data"]}}}`))
	})
	mux.HandleFunc("/_nodes/stats/os", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/stats/file_cache", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Echo the detailed query param so the test can assert it.
		detailed := r.URL.Query().Get("detailed")
		body := `{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","file_cache":{"store_size":"1mb"},"native_memory":{"total_estimated_bytes":1024,"analytics_backend":{"allocated_bytes":512,"resident_bytes":256}}}}}`
		if detailed != "" {
			body = `{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","file_cache":{"store_size":"1mb","block_cache":{"evictions":0},"detailed":true},"native_memory":{"total_estimated_bytes":1024,"analytics_backend":{"allocated_bytes":512,"resident_bytes":256}}}}}`
		}
		_, _ = w.Write([]byte(body))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{})
		require.NoError(t, err)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{NodeIds: []string{"node1"}})
		require.NoError(t, err)
		assert.Contains(t, resp.Nodes, "n1")
	})

	t.Run("specific metric", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"os"}})
		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("detailed=true sends query param", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"file_cache"}, Detailed: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.Nodes, "n1")
		fc := resp.Nodes["n1"].FileCache
		require.NotNil(t, fc)
		assert.Equal(t, true, fc["detailed"])
		require.NotNil(t, resp.Nodes["n1"].NativeMemory)
		assert.Equal(t, int64(1024), resp.Nodes["n1"].NativeMemory.TotalEstimatedBytes)
		require.NotNil(t, resp.Nodes["n1"].NativeMemory.AnalyticsBackend)
		assert.Equal(t, int64(512), resp.Nodes["n1"].NativeMemory.AnalyticsBackend.AllocatedBytes)
	})

	t.Run("detailed=false omits query param", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"file_cache"}, Detailed: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.Nodes, "n1")
		fc := resp.Nodes["n1"].FileCache
		require.NotNil(t, fc)
		_, hasDetailed := fc["detailed"]
		assert.False(t, hasDetailed)
	})

	t.Run("nil request uses defaults", func(t *testing.T) {
		resp, err := svc.Stats(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})
}

// nodesStatsResourceUsageFixture is the exact wire shape returned by
// GET /_nodes/stats/resource_usage_stats on OpenSearch 3.10.0: percentages are
// JSON strings and the tracker entry is nested under the node ID.
const nodesStatsResourceUsageFixture = `{
  "_nodes": {"total": 1, "successful": 1, "failed": 0},
  "cluster_name": "docker-cluster",
  "nodes": {
    "node-1": {
      "timestamp": 1795453806252,
      "name": "node-1",
      "roles": ["cluster_manager", "data", "ingest"],
      "resource_usage_stats": {
        "node-1": {
          "timestamp": 1795453805391,
          "cpu_utilization_percent": "0.2",
          "memory_utilization_percent": "44.2",
          "native_memory_utilization_percent": "1.0",
          "io_usage_stats": {"max_io_utilization_percent": "0.0"}
        }
      },
      "native_memory": {"total_estimated_bytes": 854810624}
    }
  }
}`

// nodesStatsResourceUsageCombinedFixture mirrors a containerized node where
// os.cpu.percent and process.cpu.percent report the -1 unavailable sentinel
// while resource_usage_stats still exposes the cgroup-aware CPU percentage.
const nodesStatsResourceUsageCombinedFixture = `{
  "_nodes": {"total": 1, "successful": 1, "failed": 0},
  "cluster_name": "docker-cluster",
  "nodes": {
    "node-1": {
      "timestamp": 1795453806252,
      "name": "node-1",
      "os": {"timestamp": 1795453806252, "cpu": {"percent": -1}},
      "process": {"timestamp": 1795453806252, "cpu": {"percent": -1}},
      "resource_usage_stats": {
        "node-1": {
          "timestamp": 1795453805391,
          "cpu_utilization_percent": "0.2",
          "memory_utilization_percent": "44.2",
          "native_memory_utilization_percent": "1.0",
          "io_usage_stats": {"max_io_utilization_percent": "0.0"}
        }
      }
    }
  }
}`

// floatString returns a *FloatString holding the raw wire value.
func floatString(s string) *FloatString {
	f := FloatString(s)
	return &f
}

func TestUnitNodesService_StatsResourceUsage(t *testing.T) {
	const nodeID = "node-1"

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/stats/resource_usage_stats", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, nodesStatsResourceUsageFixture)
	})
	mux.HandleFunc("/_nodes/node-1/stats/resource_usage_stats", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, nodesStatsResourceUsageFixture)
	})
	mux.HandleFunc("/_nodes/stats/os,process,resource_usage_stats", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, http.StatusOK, nodesStatsResourceUsageCombinedFixture)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("3.10.0 payload", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{NodesStatsMetricResourceUsageStats}})
		require.NoError(t, err)
		require.NotNil(t, resp)

		require.NotNil(t, resp.NodesStats)
		assert.Equal(t, 1, resp.NodesStats.Total)
		assert.Equal(t, 1, resp.NodesStats.Successful)
		assert.Equal(t, 0, resp.NodesStats.Failed)
		assert.Equal(t, "docker-cluster", resp.ClusterName)

		node := resp.Nodes[nodeID]
		require.NotNil(t, node)
		require.Contains(t, node.ResourceUsageStats, nodeID)

		usage := node.ResourceUsage(nodeID)
		require.NotNil(t, usage)
		require.NotNil(t, usage.Timestamp)
		assert.Equal(t, int64(1795453805391), *usage.Timestamp)
		assert.Equal(t, "0.2", usage.CPUUtilizationPercent.String())
		assert.Equal(t, "44.2", usage.MemoryUtilizationPercent.String())

		cpuRaw, err := usage.CPUUtilizationPercent.Float64()
		require.NoError(t, err)
		assert.InDelta(t, 0.2, cpuRaw, 1e-9)

		cpu := node.ResourceCPUUtilization(nodeID)
		require.NotNil(t, cpu)
		assert.InDelta(t, 0.2, *cpu, 1e-9)

		mem := usage.MemoryUtilization()
		require.NotNil(t, mem)
		assert.InDelta(t, 44.2, *mem, 1e-9)

		native := usage.NativeMemoryUtilization()
		require.NotNil(t, native)
		assert.InDelta(t, 1.0, *native, 1e-9)

		require.NotNil(t, usage.IOUsageStats)
		assert.Equal(t, "0.0", usage.IOUsageStats.MaxIOUtilizationPercent.String())
		io := usage.MaxIOUtilization()
		require.NotNil(t, io)
		assert.Zero(t, *io)

		require.NotNil(t, node.NativeMemory)
		assert.Equal(t, int64(854810624), node.NativeMemory.TotalEstimatedBytes)

		assert.Nil(t, node.ResourceCPUUtilization("unknown-node"))
	})

	t.Run("combined os process resource usage with -1 cpu sentinels", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{
			Metrics: []string{"os", "process", NodesStatsMetricResourceUsageStats},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		node := resp.Nodes[nodeID]
		require.NotNil(t, node)
		require.NotNil(t, node.OS)
		require.NotNil(t, node.OS.CPU)
		assert.Equal(t, -1, node.OS.CPU.Percent)
		require.NotNil(t, node.Process)
		assert.Equal(t, -1, node.Process.CPU.Percent)

		cpu := node.ResourceCPUUtilization(nodeID)
		require.NotNil(t, cpu)
		assert.InDelta(t, 0.2, *cpu, 1e-9)
	})

	t.Run("single node route", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{
			NodeIds: []string{nodeID},
			Metrics: []string{NodesStatsMetricResourceUsageStats},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.Nodes, nodeID)
		cpu := resp.Nodes[nodeID].ResourceCPUUtilization(nodeID)
		require.NotNil(t, cpu)
		assert.InDelta(t, 0.2, *cpu, 1e-9)
	})
}

func TestUnitFloatString_JSONAndParse(t *testing.T) {
	t.Run("unmarshal", func(t *testing.T) {
		cases := []struct {
			name    string
			input   string
			wantRaw string
			wantErr bool
		}{
			{"quoted string", `"0.2"`, "0.2", false},
			{"empty string", `""`, "", false},
			{"whitespace string", `"   "`, "   ", false},
			{"negative sentinel", `"-1"`, "-1", false},
			{"negative zero string", `"-0.0"`, "-0.0", false},
			{"nan string", `"NaN"`, "NaN", false},
			{"infinity string", `"Infinity"`, "Infinity", false},
			{"negative infinity string", `"-Infinity"`, "-Infinity", false},
			{"malformed string", `"unavailable"`, "unavailable", false},
			{"integer number", `100`, "100", false},
			{"negative number", `-1`, "-1", false},
			{"fractional number", `0.2`, "0.2", false},
			{"scientific number", `1.5e+2`, "1.5e+2", false},
			{"uppercase exponent number", `2E-3`, "2E-3", false},
			{"huge number", `1e1000`, "1e1000", false},
			{"null", `null`, "", false},
			{"empty bytes", ``, "", true},
			{"unterminated string", `"unterminated`, "", true},
			{"bare minus", `-`, "", true},
			{"leading zero", `01`, "", true},
			{"plus sign", `+1`, "", true},
			{"leading dot", `.5`, "", true},
			{"trailing dot", `1.`, "", true},
			{"missing exponent", `1e`, "", true},
			{"incomplete exponent", `1e+`, "", true},
			{"boolean", `true`, "", true},
			{"object", `{}`, "", true},
			{"array", `[]`, "", true},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var f FloatString
				err := f.UnmarshalJSON([]byte(tc.input))
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.wantRaw, f.String())
			})
		}
	})

	t.Run("unquoted number through struct field", func(t *testing.T) {
		var decoded struct {
			CPU *FloatString `json:"cpu_utilization_percent"`
		}
		require.NoError(t, json.Unmarshal([]byte(`{"cpu_utilization_percent":0.2}`), &decoded))
		require.NotNil(t, decoded.CPU)
		assert.Equal(t, "0.2", decoded.CPU.String())
	})

	t.Run("marshal emits a JSON string", func(t *testing.T) {
		b, err := json.Marshal(floatString("0.2"))
		require.NoError(t, err)
		assert.Equal(t, `"0.2"`, string(b))

		usage := &NodesStatsResourceUsage{
			CPUUtilizationPercent: floatString("0.2"),
		}
		b, err = json.Marshal(usage)
		require.NoError(t, err)
		assert.Equal(t, `{"cpu_utilization_percent":"0.2"}`, string(b))
	})

	t.Run("zero value omits new response fields", func(t *testing.T) {
		b, err := json.Marshal(&NodesStatsNode{})
		require.NoError(t, err)
		assert.NotContains(t, string(b), "resource_usage_stats")

		b, err = json.Marshal(&NodesStatsResponse{})
		require.NoError(t, err)
		assert.NotContains(t, string(b), `"_nodes"`)
	})

	t.Run("nil String", func(t *testing.T) {
		var f *FloatString
		assert.Equal(t, "", f.String())
	})

	t.Run("Float64", func(t *testing.T) {
		cases := []struct {
			name    string
			raw     string
			want    float64
			wantErr bool
			isRange bool
		}{
			{"positive", "0.2", 0.2, false, false},
			{"integer", "100", 100, false, false},
			{"scientific", "1.5e+2", 150, false, false},
			{"negative sentinel preserved", "-1", -1, false, false},
			{"whitespace trimmed", "  44.2  ", 44.2, false, false},
			{"empty", "", 0, true, false},
			{"whitespace only", "   ", 0, true, false},
			{"malformed", "abc", 0, true, false},
			{"nan", "NaN", 0, true, false},
			{"inf", "Inf", 0, true, false},
			{"infinity", "Infinity", 0, true, false},
			{"negative infinity", "-Infinity", 0, true, false},
			{"overflow", "1e1000", 0, true, true},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := floatString(tc.raw).Float64()
				if tc.wantErr {
					require.Error(t, err)
					if tc.isRange {
						assert.ErrorIs(t, err, strconv.ErrRange)
					}
					return
				}
				require.NoError(t, err)
				assert.InDelta(t, tc.want, got, 1e-9)
			})
		}
	})

	t.Run("negative zero normalized to positive zero", func(t *testing.T) {
		got, err := floatString("-0.0").Float64()
		require.NoError(t, err)
		assert.Zero(t, got)
		assert.False(t, math.Signbit(got))
	})

	t.Run("nil receiver", func(t *testing.T) {
		var f *FloatString
		_, err := f.Float64()
		require.Error(t, err)
	})
}

func TestUnitNodesStatsResourceUsage_Accessors(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var r *NodesStatsResourceUsage
		assert.Nil(t, r.CPUUtilization())
		assert.Nil(t, r.MemoryUtilization())
		assert.Nil(t, r.NativeMemoryUtilization())
		assert.Nil(t, r.MaxIOUtilization())
	})

	t.Run("real values", func(t *testing.T) {
		r := &NodesStatsResourceUsage{
			CPUUtilizationPercent:          floatString("0.2"),
			MemoryUtilizationPercent:       floatString("44.2"),
			NativeMemoryUtilizationPercent: floatString("1.0"),
			IOUsageStats:                   &NodesStatsResourceUsageIO{MaxIOUtilizationPercent: floatString("0.0")},
		}

		cpu := r.CPUUtilization()
		require.NotNil(t, cpu)
		assert.InDelta(t, 0.2, *cpu, 1e-9)

		mem := r.MemoryUtilization()
		require.NotNil(t, mem)
		assert.InDelta(t, 44.2, *mem, 1e-9)

		native := r.NativeMemoryUtilization()
		require.NotNil(t, native)
		assert.InDelta(t, 1.0, *native, 1e-9)

		io := r.MaxIOUtilization()
		require.NotNil(t, io)
		assert.Zero(t, *io)
	})

	t.Run("pre-3.7.0 omits native memory and io", func(t *testing.T) {
		r := &NodesStatsResourceUsage{
			CPUUtilizationPercent:    floatString("2.5"),
			MemoryUtilizationPercent: floatString("10.0"),
		}
		cpu := r.CPUUtilization()
		require.NotNil(t, cpu)
		assert.InDelta(t, 2.5, *cpu, 1e-9)
		assert.Nil(t, r.NativeMemoryUtilization())
		assert.Nil(t, r.MaxIOUtilization())
	})

	t.Run("sentinels and malformed values are sanitized", func(t *testing.T) {
		for _, raw := range []string{"-1", "-1.0", "NaN", "Infinity", "abc", ""} {
			r := &NodesStatsResourceUsage{
				CPUUtilizationPercent:    floatString(raw),
				MemoryUtilizationPercent: floatString(raw),
			}
			assert.Nil(t, r.CPUUtilization(), raw)
			assert.Nil(t, r.MemoryUtilization(), raw)
		}
	})

	t.Run("io present without max value", func(t *testing.T) {
		r := &NodesStatsResourceUsage{IOUsageStats: &NodesStatsResourceUsageIO{}}
		assert.Nil(t, r.MaxIOUtilization())
	})

	t.Run("negative zero is zero", func(t *testing.T) {
		r := &NodesStatsResourceUsage{CPUUtilizationPercent: floatString("-0.0")}
		cpu := r.CPUUtilization()
		require.NotNil(t, cpu)
		assert.Zero(t, *cpu)
		assert.False(t, math.Signbit(*cpu))
	})

	t.Run("burst over 100 percent is not clamped", func(t *testing.T) {
		r := &NodesStatsResourceUsage{CPUUtilizationPercent: floatString("125.4")}
		cpu := r.CPUUtilization()
		require.NotNil(t, cpu)
		assert.InDelta(t, 125.4, *cpu, 1e-9)
	})
}

func TestUnitNodesStatsNode_ResourceUsageLookup(t *testing.T) {
	usage := func(cpu string) *NodesStatsResourceUsage {
		return &NodesStatsResourceUsage{CPUUtilizationPercent: floatString(cpu)}
	}

	node := &NodesStatsNode{
		ResourceUsageStats: map[string]*NodesStatsResourceUsage{
			"n1": usage("1.5"),
			"n2": usage("2.5"),
		},
	}

	t.Run("selects matching node", func(t *testing.T) {
		u := node.ResourceUsage("n1")
		require.NotNil(t, u)
		assert.Equal(t, "1.5", u.CPUUtilizationPercent.String())

		cpu := node.ResourceCPUUtilization("n2")
		require.NotNil(t, cpu)
		assert.InDelta(t, 2.5, *cpu, 1e-9)
	})

	t.Run("node ID mismatch returns nil", func(t *testing.T) {
		assert.Nil(t, node.ResourceUsage("n3"))
		assert.Nil(t, node.ResourceCPUUtilization("n3"))
	})

	t.Run("empty node ID returns nil", func(t *testing.T) {
		assert.Nil(t, node.ResourceUsage(""))
		assert.Nil(t, node.ResourceCPUUtilization(""))
	})

	t.Run("nil node", func(t *testing.T) {
		var n *NodesStatsNode
		assert.Nil(t, n.ResourceUsage("n1"))
		assert.Nil(t, n.ResourceCPUUtilization("n1"))
	})

	t.Run("nil map", func(t *testing.T) {
		n := &NodesStatsNode{}
		assert.Nil(t, n.ResourceUsage("n1"))
		assert.Nil(t, n.ResourceCPUUtilization("n1"))
	})

	t.Run("null entry", func(t *testing.T) {
		n := &NodesStatsNode{
			ResourceUsageStats: map[string]*NodesStatsResourceUsage{"n1": nil},
		}
		assert.Nil(t, n.ResourceCPUUtilization("n1"))
	})

	t.Run("sentinel CPU is sanitized", func(t *testing.T) {
		n := &NodesStatsNode{
			ResourceUsageStats: map[string]*NodesStatsResourceUsage{"n1": usage("-1")},
		}
		assert.Nil(t, n.ResourceCPUUtilization("n1"))
	})
}

func TestUnitNodesService_HotThreads(t *testing.T) {
	threadDump := "::: {node-1}[abcdef][127.0.0.1][9300]\n  Hot threads at 2026-01-01T00:00:00.000Z"

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/hot_threads", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(threadDump))
	})
	mux.HandleFunc("/_nodes/node1/hot_threads", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(threadDump))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.HotThreads(ctx, nil)
		require.NoError(t, err)
		assert.Contains(t, resp, "Hot threads")
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.HotThreads(ctx, []string{"node1"})
		require.NoError(t, err)
		assert.Contains(t, resp, "Hot threads")
	})
}

func TestUnitNodesService_ReloadSecureSettings(t *testing.T) {
	respJSON := `{"cluster_name":"test","nodes":{"n1":{"name":"node1"}}}`

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/reload_secure_settings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.ReloadSecureSettings(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
		assert.Equal(t, "node1", resp.Nodes["n1"].Name)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"secure_settings_password": "test"}
		resp, err := svc.ReloadSecureSettings(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitNodesService_Usage(t *testing.T) {
	respJSON := `{"cluster_name":"test","nodes":{"n1":{"timestamp":1234567890,"since":1234567880,"rest_actions":{"_count":100}}}}`

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})
	mux.HandleFunc("/_nodes/node1/usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})
	mux.HandleFunc("/_nodes/node1/usage/rest_actions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{NodeIds: []string{"node1"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(1234567890), resp.Nodes["n1"].Timestamp)
		assert.Equal(t, int64(1234567880), resp.Nodes["n1"].Since)
	})

	t.Run("specific node with metric", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{NodeIds: []string{"node1"}, Metrics: []string{"rest_actions"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("nil request uses defaults", func(t *testing.T) {
		resp, err := svc.Usage(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test", resp.ClusterName)
	})
}

func TestUnitNodesService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("HotThreads server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.HotThreads(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Usage server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
	})

	t.Run("Usage unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Info server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
	})

	t.Run("Info unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Stats server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
	})

	t.Run("Stats unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitNodesService_NetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewNodesService(deadClient(), testLogger())

	t.Run("Info network error", func(t *testing.T) {
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
	})

	t.Run("Stats network error", func(t *testing.T) {
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
	})

	t.Run("HotThreads network error", func(t *testing.T) {
		_, err := s.HotThreads(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings network error", func(t *testing.T) {
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Usage network error", func(t *testing.T) {
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
	})
}
