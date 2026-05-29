# OpenSearch Go Client - API Implementation Plan

Generated from analysis of:
- Client: `github.com/disaster37/opensearch/v3` (146 implemented methods)
- Server: [opensearch-project/OpenSearch](https://github.com/opensearch-project/OpenSearch) main branch REST API specs
- Plugins: security, alerting, index-management, CCR, anomaly-detection, SQL, ml-commons, neural-search, async-search, K-NN, performance-analyzer, geospatial, flow-framework, job-scheduler

---

## 1. Elasticsearch-Only APIs (in client but NOT in OpenSearch)

These APIs should be **deprecated or removed** from the client as they do not exist on OpenSearch.

| Service | Method | Issue |
|---------|--------|-------|
| Indices | `Freeze` | POST `/{index}/_freeze` - removed from OpenSearch, ES-only |
| Indices | `Unfreeze` | POST `/{index}/_unfreeze` - removed from OpenSearch, ES-only |

---

## 2. Core OpenSearch APIs to Implement

### 2.1 Document Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 1 | `Create` | PUT/POST | `/{index}/_create/{id}` | wait_for_active_shards, refresh, routing, timeout, version, version_type, pipeline | required (doc) | `*CreateResponse` (same as IndexResponse) |
| 2 | `GetSource` | GET | `/{index}/_source/{id}` | preference, realtime, refresh, routing, _source, _source_excludes, _source_includes, version, version_type | none | `json.RawMessage` (raw source) |
| 3 | `ExistsSource` | HEAD | `/{index}/_source/{id}` | preference, realtime, refresh, routing, _source, _source_excludes, _source_includes, version, version_type | none | `bool` |
| 4 | `ReindexRethrottle` | POST | `/_reindex/{task_id}/_rethrottle` | requests_per_second (required) | none | `*TasksListResponse` |
| 5 | `DeleteByQueryRethrottle` | POST | `/_delete_by_query/{task_id}/_rethrottle` | requests_per_second (required) | none | `*TasksListResponse` |
| 6 | `UpdateByQueryRethrottle` | POST | `/_update_by_query/{task_id}/_rethrottle` | requests_per_second (required) | none | `*TasksListResponse` |

### 2.2 Search Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 7 | `SearchTemplate` | GET/POST | `/_search/template`, `/{index}/_search/template` | ignore_unavailable, ignore_throttled, allow_no_indices, expand_wildcards, preference, routing, scroll, search_type, explain, profile, typed_keys, rest_total_hits_as_int, ccs_minimize_roundtrips | required (template definition) | `*querydsl.SearchResult` |
| 8 | `MultiSearchTemplate` | GET/POST | `/_msearch/template`, `/{index}/_msearch/template` | search_type, typed_keys, max_concurrent_searches, rest_total_hits_as_int, ccs_minimize_roundtrips | required (NDJSON) | `*querydsl.MultiSearchResult` |
| 9 | `RenderSearchTemplate` | GET/POST | `/_render/template`, `/_render/template/{id}` | none | optional (template + params) | `*RenderSearchTemplateResponse` |
| 10 | `RankEval` | GET/POST | `/_rank_eval`, `/{index}/_rank_eval` | ignore_unavailable, allow_no_indices, expand_wildcards, search_type | required (ranking eval body) | `*RankEvalResponse` |
| 11 | `CreatePIT` | POST | `/{index}/_search/point_in_time` | allow_partial_pit_creation, keep_alive, preference, routing | none | `*CreatePITResponse` {pit_id, creation_time, shards} |
| 12 | `DeletePIT` | DELETE | `/_search/point_in_time` | none | required (pit_id body) | `*DeletePITResponse` {pits: [{pit_id, successful}]} |
| 13 | `GetAllPITs` | GET | `/_search/point_in_time/_all` | none | none | `*GetAllPITsResponse` {pits: [{pit_id, creation_time, keep_alive}]} |
| 14 | `DeleteAllPITs` | DELETE | `/_search/point_in_time/_all` | none | none | `*DeletePITResponse` |
| 15 | `ScriptsPainlessExecute` | GET/POST | `/_scripts/painless/_execute` | none | optional (script definition) | `*PainlessExecuteResponse` |

### 2.3 Indices Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 16 | `AddBlock` | PUT | `/{index}/_block/{block}` | cluster_manager_timeout, timeout, ignore_unavailable, allow_no_indices, expand_wildcards | none | `*IndicesBlockResponse` {acknowledged, shards_acknowledged} |
| 17 | `Clone` | PUT/POST | `/{index}/_clone/{target}` | cluster_manager_timeout, timeout, wait_for_active_shards, wait_for_completion, task_execution_timeout | optional (settings, aliases) | `*types.AcknowledgedResponse` |
| 18 | `Split` | PUT/POST | `/{index}/_split/{target}` | copy_settings, cluster_manager_timeout, timeout, wait_for_active_shards, wait_for_completion, task_execution_timeout | optional (settings, aliases) | `*types.AcknowledgedResponse` |
| 19 | `DeleteAlias` | DELETE | `/{index}/_alias/{name}`, `/{index}/_aliases/{name}` | cluster_manager_timeout, timeout | none | `*types.AcknowledgedResponse` |
| 20 | `ExistsAlias` | HEAD | `/_alias/{name}`, `/{index}/_alias/{name}` | ignore_unavailable, allow_no_indices, expand_wildcards, local | none | `bool` |
| 21 | `ExistsIndexTemplate` | HEAD | `/_index_template/{name}` | flat_settings, master_timeout, local | none | `bool` |
| ~~22~~ | ~~`ExistsTemplate`~~ | — | — | — | — | **ALREADY IMPLEMENTED** at `indices_service.go:706` |
| 23 | `Recovery` | GET | `/_recovery`, `/{index}/_recovery` | detailed, active_only | none | `map[string]*IndicesRecoveryResponse` |
| 24 | `ShardStores` | GET | `/_shard_stores`, `/{index}/_shard_stores` | status, ignore_unavailable, allow_no_indices, expand_wildcards | none | `*IndicesShardStoresResponse` |
| 25 | `UpdateAliases` | POST | `/_aliases` | cluster_manager_timeout, timeout | required (actions) | `*types.AcknowledgedResponse` |
| 26 | `ResolveIndex` | GET | `/_resolve/index/{name}` | expand_wildcards | none | `*IndicesResolveIndexResponse` {indices, aliases, data_streams} |
| 27 | `SimulateIndexTemplate` | POST | `/_index_template/_simulate_index/{name}` | create, cause, cluster_manager_timeout | optional (template body) | `*IndicesSimulateTemplateResponse` |
| 28 | `SimulateTemplate` | POST | `/_index_template/_simulate`, `/_index_template/_simulate/{name}` | create, cause, cluster_manager_timeout | optional (template body) | `*IndicesSimulateTemplateResponse` |
| 29 | `DataStreamsStats` | GET | `/_data_stream/_stats`, `/_data_stream/{name}/_stats` | none | none | `*IndicesDataStreamsStatsResponse` |

Note: `Upgrade` and `GetUpgrade` are present in specs but documented as "no longer useful and will be removed" — skip.

### 2.4 Cluster Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 30 | `AllocationExplain` | GET/POST | `/_cluster/allocation/explain` | include_yes_decisions, include_disk_info | optional (index, shard, primary) | `*ClusterAllocationExplainResponse` |
| 31 | `PendingTasks` | GET | `/_cluster/pending_tasks` | local, cluster_manager_timeout | none | `*ClusterPendingTasksResponse` |
| 32 | `RemoteInfo` | GET | `/_remote/info` | none | none | `map[string]*ClusterRemoteInfoResponse` |
| 33 | `ExistsComponentTemplate` | HEAD | `/_component_template/{name}` | master_timeout, local | none | `bool` |
| 34 | `PutDecommissionAwareness` | PUT | `/_cluster/decommission/awareness/{attr}/{value}` | none | none | `*types.AcknowledgedResponse` |
| 35 | `GetDecommissionAwareness` | GET | `/_cluster/decommission/awareness/{attr}/_status` | none | none | `*ClusterDecommissionAwarenessResponse` |
| 36 | `DeleteDecommissionAwareness` | DELETE | `/_cluster/decommission/awareness/` | none | none | `*types.AcknowledgedResponse` |
| 37 | `PutWeightedRouting` | PUT | `/_cluster/routing/awareness/{attribute}/weights` | none | none | `*ClusterWeightedRoutingResponse` |
| 38 | `GetWeightedRouting` | GET | `/_cluster/routing/awareness/{attribute}/weights` | none | none | `*ClusterWeightedRoutingResponse` |
| 39 | `DeleteWeightedRouting` | DELETE | `/_cluster/routing/awareness/weights` | none | none | `*types.AcknowledgedResponse` |
| 40 | `PostVotingConfigExclusions` | POST | `/_cluster/voting_config_exclusions` | node_ids, node_names, timeout | none | `*types.AcknowledgedResponse` |
| 41 | `DeleteVotingConfigExclusions` | DELETE | `/_cluster/voting_config_exclusions` | wait_for_removal | none | `*types.AcknowledgedResponse` |

### 2.5 Nodes Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 42 | `HotThreads` | GET | `/_nodes/hot_threads`, `/_nodes/{node_id}/hot_threads` | interval, snapshots, threads, ignore_idle_threads, type, timeout | none | `string` (plain text) |
| 43 | `ReloadSecureSettings` | POST | `/_nodes/reload_secure_settings`, `/_nodes/{node_id}/reload_secure_settings` | timeout | optional (keystore password) | `*NodesReloadSecureSettingsResponse` |
| 44 | `Usage` | GET | `/_nodes/usage`, `/_nodes/{node_id}/usage`, `/_nodes/usage/{metric}` | timeout | none | `*NodesUsageResponse` |

### 2.6 Cat Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 45 | `Help` | GET | `/_cat` | help, s | none | `CatHelpResponse` (list of available endpoints) |
| 46 | `NodeAttrs` | GET | `/_cat/nodeattrs` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatNodeAttrsResponse` |
| 47 | `Nodes` | GET | `/_cat/nodes` | bytes, format, full_id, local, cluster_manager_timeout, h, help, s, time, v | none | `CatNodesResponse` |
| 48 | `PendingTasks` | GET | `/_cat/pending_tasks` | format, local, cluster_manager_timeout, h, help, s, time, v | none | `CatPendingTasksResponse` |
| 49 | `Plugins` | GET | `/_cat/plugins` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatPluginsResponse` |
| 50 | `CatRecovery` | GET | `/_cat/recovery`, `/_cat/recovery/{index}` | format, active_only, bytes, detailed, h, help, s, time, v | none | `CatRecoveryResponse` |
| 51 | `Repositories` | GET | `/_cat/repositories` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatRepositoriesResponse` |
| 52 | `Segments` | GET | `/_cat/segments`, `/_cat/segments/{index}` | format, bytes, cluster_manager_timeout, h, help, s, v | none | `CatSegmentsResponse` |
| 53 | `SegmentReplication` | GET | `/_cat/segment_replication`, `/_cat/segment_replication/{index}` | format, active_only, bytes, detailed, shards, h, help, s, time, v | none | `CatSegmentReplicationResponse` |
| 54 | `CatTasks` | GET | `/_cat/tasks` | format, nodes, actions, detailed, parent_task_id, h, help, s, time, v | none | `CatTasksResponse` |
| 55 | `Templates` | GET | `/_cat/templates`, `/_cat/templates/{name}` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatTemplatesResponse` |
| 56 | `ThreadPool` | GET | `/_cat/thread_pool`, `/_cat/thread_pool/{thread_pool_patterns}` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatThreadPoolResponse` |
| 57 | `ClusterManager` | GET | `/_cat/cluster_manager` | format, local, cluster_manager_timeout, h, help, s, v | none | `CatClusterManagerResponse` — NOTE: `Master()` already exists using deprecated `/_cat/master` path; this is just an alias/rename |

### 2.7 Ingest Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 58 | `ProcessorGrok` | GET | `/_ingest/processor/grok` | none | none | `map[string][]string` (builtin patterns) |

### 2.8 Snapshot Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 59 | `CleanupRepository` | POST | `/_snapshot/{repository}/_cleanup` | cluster_manager_timeout, timeout | none | `*SnapshotCleanupRepositoryResponse` {results: {deleted_bytes, deleted_blobs}} |
| 60 | `Clone` | PUT | `/_snapshot/{repository}/{snapshot}/_clone/{target}` | cluster_manager_timeout | required (indices) | `*types.AcknowledgedResponse` |

### 2.9 Script Service - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 61 | `GetScriptContext` | GET | `/_script_context` | none | none | `*ScriptContextResponse` {contexts: [...]} |
| 62 | `GetScriptLanguages` | GET | `/_script_language` | none | none | `*ScriptLanguagesResponse` {language_contexts: [...]} |

### 2.10 Client-Level - Missing APIs

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 63 | `Info` | GET | `/` | none | none | `*InfoResponse` {name, cluster_name, cluster_uuid, version, tagline} |
| 64 | `Ping` | HEAD | `/` | none | none | `bool` |

### 2.11 New Services - Missing Entirely

#### Dangling Indices Service (new service)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 65 | `List` | GET | `/_dangling` | none | none | `*DanglingIndicesListResponse` {dangling_indices: [{index_name, index_uuid, ...}]} |
| 66 | `Import` | POST | `/_dangling/{index_uuid}` | accept_data_loss, timeout, cluster_manager_timeout | none | `*types.AcknowledgedResponse` |
| 67 | `Delete` | DELETE | `/_dangling/{index_uuid}` | accept_data_loss, timeout, cluster_manager_timeout | none | `*types.AcknowledgedResponse` |

#### Search Pipeline Service (new service)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 68 | `Get` | GET | `/_search/pipeline`, `/_search/pipeline/{id}` | cluster_manager_timeout | none | `map[string]*SearchPipelineResponse` |
| 69 | `Put` | PUT | `/_search/pipeline/{id}` | cluster_manager_timeout, timeout | required (pipeline definition) | `*types.AcknowledgedResponse` |
| 70 | `Delete` | DELETE | `/_search/pipeline/{id}` | cluster_manager_timeout, timeout | none | `*types.AcknowledgedResponse` |

#### Remote Store Service (new service, experimental)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 71 | `Restore` | POST | `/_remotestore/_restore` | cluster_manager_timeout, wait_for_completion | required (index IDs) | `*RemoteStoreRestoreResponse` |
| 72 | `Stats` | GET | `/_remotestore/stats/{index}`, `/_remotestore/stats/{index}/{shard_id}` | timeout | none | `*RemoteStoreStatsResponse` |

#### WLM Service (new service, experimental)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 73 | `ListStats` | GET | `/_list/wlm_stats` | size, next_token, sort, order, v | none | `*WlmStatsListResponse` |

---

## 3. OpenSearch Plugin APIs to Implement

### 3.1 Security Plugin - Missing APIs

**File**: `api/security_service.go`

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 74 | `Health` | GET | `/_plugins/_security/health` | none | none | `*SecurityHealthResponse` {status, message} |
| 75 | `WhoAmI` | GET | `/_plugins/_security/whoami` | none | none | `*SecurityWhoAmIResponse` {dn, is_admin, is_node_certificate_request} |
| 76 | `TenantInfo` | GET | `/_plugins/_security/tenantinfo` | none | none | `*SecurityTenantInfoResponse` |
| 77 | `DashboardsInfo` | GET | `/_plugins/_security/dashboardsinfo` | none | none | `*SecurityDashboardsInfoResponse` |
| 78 | `ConfigUpdate` | PUT | `/_plugins/_security/configupdate` | none | required (config_type) | `*SecurityConfigUpdateResponse` {node_size, updated_node, ...} |
| 79 | `SSLInfo` | GET | `/_opendistro/_security/sslinfo` | none | none | `*SecuritySSLInfoResponse` |
| 80 | `PermissionsInfo` | GET | `/_plugins/_security/api/permissionsinfo` | none | none | `*SecurityPermissionsInfoResponse` |
| 81 | `Account` (GET) | GET | `/_plugins/_security/api/account` | none | none | `*SecurityAccountResponse` |
| 82 | `Account` (PUT) | PUT | `/_plugins/_security/api/account` | none | required (password change) | `*SecurityResponse` |
| 83 | `AuthToken` | POST | `/_plugins/_security/api/authtoken` | none | none | `*SecurityAuthTokenResponse` |
| 84 | `GetAllowlist` | GET | `/_plugins/_security/api/allowlist` | none | none | `map[string]*SecurityAllowlist` |
| 85 | `PutAllowlist` | PUT | `/_plugins/_security/api/allowlist` | none | required | `*SecurityResponse` |
| 86 | `GetCertificates` | GET | `/_plugins/_security/api/certificates` | none | none | `*SecurityCertificatesResponse` |
| 87 | `GetMultiTenancyConfig` | GET | `/_plugins/_security/api/tenancy/config` | none | none | `*SecurityMultiTenancyConfigResponse` |
| 88 | `PutMultiTenancyConfig` | PUT | `/_plugins/_security/api/tenancy/config` | none | required | `*SecurityResponse` |
| 89 | `GetRateLimiters` | GET | `/_plugins/_security/api/authfailurelisteners` | none | none | `map[string]*SecurityRateLimiter` |
| 90 | `PutRateLimiter` | PUT | `/_plugins/_security/api/authfailurelisteners/{name}` | none | required | `*SecurityResponse` |
| 91 | `DeleteRateLimiter` | DELETE | `/_plugins/_security/api/authfailurelisteners/{name}` | none | none | `*SecurityResponse` |
| 92 | `ListRoles` (bulk GET) | GET | `/_plugins/_security/api/roles` | none | none | `map[string]SecurityRole` |
| 93 | `ListUsers` (bulk GET) | GET | `/_plugins/_security/api/internalusers` | none | none | `map[string]SecurityUser` |
| 94 | `ListRoleMappings` (bulk GET) | GET | `/_plugins/_security/api/rolesmapping` | none | none | `map[string]SecurityRoleMapping` |
| 95 | `ListActionGroups` (bulk GET) | GET | `/_plugins/_security/api/actiongroups` | none | none | `map[string]SecurityActionGroup` |
| 96 | `ListTenants` (bulk GET) | GET | `/_plugins/_security/api/tenants` | none | none | `map[string]SecurityTenant` |
| 97 | `ListNodesDN` (bulk GET) | GET | `/_plugins/_security/api/nodesdn` | none | none | `map[string]SecurityDistinguishedName` |

### 3.2 Alerting Plugin - Missing APIs

**File**: `api/alerting_service.go`

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 98 | `ExecuteMonitor` | POST | `/_plugins/_alerting/monitors/{monitorId}/_execute` | none | optional (trigger overrides) | `*AlertingExecuteMonitorResponse` |
| 99 | `AcknowledgeAlert` | POST | `/_plugins/_alerting/monitors/{monitorId}/_acknowledge/alerts` | none | required (alert IDs) | `*AlertingAcknowledgeAlertResponse` |
| 100 | `GetAlerts` | GET | `/_plugins/_alerting/monitors/alerts` | sortOrder, size, startIndex, searchString, severityLevel, alertState, monitorId, workflowIds, alertIds, associatedAlertIds | none | `*AlertingGetAlertsResponse` {alerts, totalAlerts} |
| 101 | `GetFindings` | GET | `/_plugins/_alerting/monitors/findings/_search` | sortOrder, size, startIndex, searchString, findingId, monitorId | none | `*AlertingGetFindingsResponse` {findings, totalFindings} |
| 102 | `GetDestinations` | GET | `/_plugins/_alerting/destinations/{destinationId}` + list | none | none | `*AlertingGetDestinationsResponse` |
| 103 | `IndexWorkflow` | POST/PUT | `/_plugins/_alerting/workflows`, `/_plugins/_alerting/workflows/{workflowId}` | none | required | `*AlertingGetWorkflowResponse` |
| 104 | `GetWorkflow` | GET | `/_plugins/_alerting/workflows/{workflowId}` | none | none | `*AlertingGetWorkflowResponse` |
| 105 | `DeleteWorkflow` | DELETE | `/_plugins/_alerting/workflows/{workflowId}` | none | none | `*AlertingDeleteWorkflowResponse` |
| 106 | `ExecuteWorkflow` | POST | `/_plugins/_alerting/workflows/{workflowId}/_execute` | none | optional | `*AlertingExecuteWorkflowResponse` |
| 107 | `GetWorkflowAlerts` | GET | `/_plugins/_alerting/workflows/alerts` | similar to GetAlerts | none | `*AlertingGetAlertsResponse` |
| 108 | `AcknowledgeChainedAlerts` | POST | `/_plugins/_alerting/workflows/{workflowId}/_acknowledge/alerts` | none | required (alert IDs) | `*AlertingAcknowledgeAlertResponse` |

### 3.3 ISM Plugin - Missing APIs

**File**: `api/ism_service.go`

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 109 | `AddPolicy` | POST | `/_plugins/_ism/add/{index}` | none | required (policy_id) | `*IsmAddPolicyResponse` {updated_indices, failed_indices} |
| 110 | `RemovePolicy` | POST | `/_plugins/_ism/remove/{index}` | none | none | `*IsmRemovePolicyResponse` {updated_indices, failed_indices} |
| 111 | `ChangePolicy` | POST | `/_plugins/_ism/change_policy/{index}` | none | required (policy_id, state, include) | `*IsmChangePolicyResponse` |
| 112 | `RetryFailedIndex` | POST | `/_plugins/_ism/retry/{index}` | none | optional (state) | `*IsmRetryResponse` {updated_indices, failed_indices} |
| 113 | `ListPolicies` | GET | `/_plugins/_ism/policies` | none | none | `*IsmListPoliciesResponse` {policies, total_policies} |

### 3.4 ISM Rollup Plugin - New sub-service

**File**: `api/rollup_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 114 | `GetRollup` | GET | `/_plugins/_rollup/jobs/{rollupId}` | none | none | `*RollupGetResponse` |
| 115 | `PutRollup` | PUT | `/_plugins/_rollup/jobs/{rollupId}` | none | required (rollup config) | `*RollupGetResponse` |
| 116 | `DeleteRollup` | DELETE | `/_plugins/_rollup/jobs/{rollupId}` | none | none | `*types.AcknowledgedResponse` |
| 117 | `StartRollup` | POST | `/_plugins/_rollup/jobs/{rollupId}/_start` | none | none | `*RollupStartStopResponse` |
| 118 | `StopRollup` | POST | `/_plugins/_rollup/jobs/{rollupId}/_stop` | none | none | `*RollupStartStopResponse` |
| 119 | `ExplainRollup` | GET | `/_plugins/_rollup/jobs/{rollupId}/_explain` | none | none | `map[string]*RollupExplainResponse` |

### 3.5 Snapshot Management (SM) - Missing APIs

**File**: `api/sm_service.go`

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 120 | `StartPolicy` | POST | `/_plugins/_sm/policies/{policyName}/_start` | none | none | `*SmStartStopResponse` |
| 121 | `StopPolicy` | POST | `/_plugins/_sm/policies/{policyName}/_stop` | none | none | `*SmStartStopResponse` |
| 122 | `ExplainPolicy` (detailed) | GET | `/_plugins/_sm/policies/{policyName}/_explain` | none | none | `*SmExplainPolicyResponse` (already exists but verify) |
| 123 | `ListPolicies` | GET | `/_plugins/_sm/policies` | none | none | `*SmListPoliciesResponse` |

### 3.6 CCR Plugin - Missing APIs

**File**: `api/ccr_service.go`

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 124 | `UpdateRule` | PUT | `/_plugins/_replication/{name}/_update` | none | required (update body) | `*CcrUpdateRuleResponse` |

---

## 4. Entirely New Plugin Services to Add

### 4.1 Anomaly Detection Plugin (new service `AD`)

**File**: `api/ad_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 125 | `IndexDetector` | POST/PUT | `/_plugins/_anomaly_detection/detectors`, `/{detectorId}` | none | required | `*AdIndexDetectorResponse` |
| 126 | `GetDetector` | GET | `/_plugins/_anomaly_detection/detectors/{detectorId}` | none | none | `*AdGetDetectorResponse` |
| 127 | `DeleteDetector` | DELETE | `/_plugins/_anomaly_detection/detectors/{detectorId}` | none | none | `*AdDeleteDetectorResponse` |
| 128 | `ExecuteDetector` | POST | `/_plugins/_anomaly_detection/detectors/{detectorId}/_run` | none | optional | `*AdExecuteDetectorResponse` |
| 129 | `PreviewDetector` | POST | `/_plugins/_anomaly_detection/detectors/_preview`, `/{detectorId}/_preview` | none | required | `*AdPreviewDetectorResponse` |
| 130 | `SearchDetectors` | POST | `/_plugins/_anomaly_detection/detectors/_search` | none | required (query) | `*AdSearchDetectorsResponse` |
| 131 | `SearchResults` | POST | `/_plugins/_anomaly_detection/detectors/results/_search` | none | required (query) | `*AdSearchResultsResponse` |
| 132 | `SearchTopResults` | POST | `/_plugins/_anomaly_detection/detectors/{detectorId}/results/_topAnomalies` | none | required | `*AdSearchTopResultsResponse` |
| 133 | `Stats` | GET | `/_plugins/_anomaly_detection/stats`, `/{stat}` | none | none | `*AdStatsResponse` |
| 134 | `ValidateDetector` | POST | `/_plugins/_anomaly_detection/detectors/_validate` | none | required | `*AdValidateResponse` |

### 4.2 Forecaster Plugin (new service, part of AD plugin)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 135 | `IndexForecaster` | POST/PUT | `/_plugins/_forecast/forecasters`, `/{forecasterId}` | none | required | `*ForecastIndexResponse` |
| 136 | `GetForecaster` | GET | `/_plugins/_forecast/forecasters/{forecasterId}` | none | none | `*ForecastGetResponse` |
| 137 | `DeleteForecaster` | DELETE | `/_plugins/_forecast/forecasters/{forecasterId}` | none | none | `*types.AcknowledgedResponse` |
| 138 | `StartForecaster` | POST | `/_plugins/_forecast/forecasters/{forecasterId}/_start` | none | none | `*ForecastStartStopResponse` |
| 139 | `StopForecaster` | POST | `/_plugins/_forecast/forecasters/{forecasterId}/_stop` | none | none | `*ForecastStartStopResponse` |
| 140 | `RunOnceForecaster` | POST | `/_plugins/_forecast/forecasters/{forecasterId}/_run` | none | none | `*ForecastRunOnceResponse` |

### 4.3 SQL/PPL Plugin (new service `SQL`)

**File**: `api/sql_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 141 | `PPLQuery` | POST | `/_plugins/_ppl` | none | required ({query: "..."}) | `*SQLResponse` (columns, datarows, schema, etc.) |
| 142 | `PPLExplain` | POST | `/_plugins/_ppl/_explain` | none | required ({query: "..."}) | `*SQLExplainResponse` |
| 143 | `SQLQuery` | POST | `/_plugins/_sql` | format=json/jdbc/csv/raw | required ({query: "..."}) | `*SQLResponse` |
| 144 | `SQLExplain` | POST | `/_plugins/_sql/_explain` | none | required | `*SQLExplainResponse` |
| 145 | `SQLCloseCursor` | POST | `/_plugins/_sql/close` | none | required ({cursor: "..."}) | `*SQLCloseResponse` |

### 4.4 ML Commons Plugin (new service `ML`)

**File**: `api/ml_service.go` (new) — large surface area, core methods:

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 146 | `RegisterModel` | POST | `/_plugins/_ml/models/_register` | none | required (model config) | `*MlRegisterModelResponse` {task_id, task_type, status} |
| 147 | `DeployModel` | POST | `/_plugins/_ml/models/{modelId}/_deploy` | none | none | `*MlDeployModelResponse` {task_id} |
| 148 | `UndeployModel` | POST | `/_plugins/_ml/models/{modelId}/_undeploy` | none | none | `*MlUndeployModelResponse` |
| 149 | `GetModel` | GET | `/_plugins/_ml/models/{modelId}` | none | none | `*MlGetModelResponse` |
| 150 | `SearchModels` | POST | `/_plugins/_ml/models/_search` | none | required (query) | `*MlSearchModelsResponse` |
| 151 | `UpdateModel` | PUT | `/_plugins/_ml/models/{modelId}` | none | required | `*MlUpdateModelResponse` |
| 152 | `DeleteModel` | DELETE | `/_plugins/_ml/models/{modelId}` | none | none | `*types.AcknowledgedResponse` |
| 153 | `Predict` | POST | `/_plugins/_ml/models/{modelId}/_predict` | none | required (input) | `*MlPredictResponse` {inference_results} |
| 154 | `Train` | POST | `/_plugins/_ml/_train/{algorithm}` | none | required | `*MlTrainResponse` {task_id} |
| 155 | `TrainAndPredict` | POST | `/_plugins/_ml/_train_predict/{algorithm}` | none | required | `*MlPredictResponse` |
| 156 | `Execute` | POST | `/_plugins/_ml/_execute/{algorithm}` | none | required | `*MlExecuteResponse` |
| 157 | `Stats` | GET | `/_plugins/_ml/stats`, `/{stat}` | none | none | `*MlStatsResponse` |
| 158 | `GetTask` | GET | `/_plugins/_ml/tasks/{taskId}` | none | none | `*MlGetTaskResponse` |
| 159 | `DeleteTask` | DELETE | `/_plugins/_ml/tasks/{taskId}` | none | none | `*types.AcknowledgedResponse` |
| 160 | `CreateConnector` | POST | `/_plugins/_ml/connectors/_create` | none | required | `*MlCreateConnectorResponse` {connector_id} |
| 161 | `GetConnector` | GET | `/_plugins/_ml/connectors/{connectorId}` | none | none | `*MlGetConnectorResponse` |
| 162 | `DeleteConnector` | DELETE | `/_plugins/_ml/connectors/{connectorId}` | none | none | `*types.AcknowledgedResponse` |
| 163 | `SearchConnectors` | POST | `/_plugins/_ml/connectors/_search` | none | required | `*MlSearchConnectorsResponse` |
| 164 | `RegisterAgent` | POST | `/_plugins/_ml/agents/_register` | none | required | `*MlRegisterAgentResponse` {agent_id} |
| 165 | `GetAgent` | GET | `/_plugins/_ml/agents/{agentId}` | none | none | `*MlGetAgentResponse` |
| 166 | `DeleteAgent` | DELETE | `/_plugins/_ml/agents/{agentId}` | none | none | `*types.AcknowledgedResponse` |
| 167 | `SearchAgents` | POST | `/_plugins/_ml/agents/_search` | none | required | `*MlSearchAgentsResponse` |
| 168 | `DeleteModelGroup` | DELETE | `/_plugins/_ml/model_groups/{modelGroupId}` | none | none | `*types.AcknowledgedResponse` |
| 169 | `RegisterModelGroup` | POST | `/_plugins/_ml/model_groups/_register` | none | required | `*MlRegisterModelGroupResponse` |
| 170 | `Profile` | GET | `/_plugins/_ml/profile`, `/models/{modelId}`, `/tasks/{taskId}` | none | none | `*MlProfileResponse` |
| 171 | `ListTools` | GET | `/_plugins/_ml/tools` | none | none | `*MlListToolsResponse` |
| 172 | `ExecuteTool` | POST | `/_plugins/_ml/tools/_execute/{toolName}` | none | required | `*MlExecuteToolResponse` |

### 4.5 Asynchronous Search Plugin (new service `AsyncSearch`)

**File**: `api/async_search_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 173 | `Submit` | POST | `/_plugins/_asynchronous_search` | wait_for_completion_timeout, keep_on_completion, keep_alive | required (search body) | `*AsyncSearchSubmitResponse` {id, response, ...} |
| 174 | `Get` | GET | `/_plugins/_asynchronous_search/{id}` | none | none | `*AsyncSearchGetResponse` |
| 175 | `Delete` | DELETE | `/_plugins/_asynchronous_search/{id}` | none | none | `*types.AcknowledgedResponse` |
| 176 | `Stats` | GET | `/_plugins/_asynchronous_search/stats` | none | none | `*AsyncSearchStatsResponse` |

### 4.6 K-NN Plugin (new service `KNN`)

**File**: `api/knn_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 177 | `Stats` | GET | `/_plugins/_knn/stats`, `/{stat}` | none | none | `*KnnStatsResponse` |
| 178 | `Warmup` | GET | `/_plugins/_knn/warmup/{index}` | none | none | `*KnnWarmupResponse` |
| 179 | `ClearCache` | POST | `/_plugins/_knn/clear_cache/{index}` | none | none | `*types.AcknowledgedResponse` |
| 180 | `TrainModel` | POST | `/_plugins/_knn/models/_train`, `/_plugins/_knn/models/{modelId}/_train` | none | required | `*KnnTrainModelResponse` |
| 181 | `GetModel` | GET | `/_plugins/_knn/models/{modelId}` | none | none | `*KnnGetModelResponse` |
| 182 | `SearchModels` | POST | `/_plugins/_knn/models/_search` | none | required | `*KnnSearchModelsResponse` |
| 183 | `DeleteModel` | DELETE | `/_plugins/_knn/models/{modelId}` | none | none | `*types.AcknowledgedResponse` |

### 4.7 Neural Search Plugin (new service `Neural`)

**File**: `api/neural_service.go` (new)

| # | Method | HTTP | Path | URL Params | Body | Return |
|---|--------|------|------|-----------|------|--------|
| 184 | `Stats` | GET | `/_plugins/_neural/stats`, `/{stat}` | none | none | `*NeuralStatsResponse` |
| 185 | `Warmup` | POST | `/_plugins/_neural/warmup/{index}` | none | none | `*NeuralWarmupResponse` |
| 186 | `ClearCache` | POST | `/_plugins/_neural/clear_cache/{index}` | none | none | `*types.AcknowledgedResponse` |

---

## 5. Summary

### Implementation Priority

| Priority | Count | Description |
|----------|-------|-------------|
| P0 - Critical | 2 | Deprecate Elasticsearch-only Freeze/Unfreeze |
| P1 - Core APIs | 72 | Core OpenSearch REST APIs (#1-#73 minus #22 already implemented) |
| P2 - Plugin extensions | 51 | Extend existing plugin services (#74-#124) |
| P3 - New plugins | 62 | Entirely new plugin services (#125-#186) |
| **Total** | **185 to implement + 2 deprecations** | |

### Suggested Implementation Order

1. **Phase 1**: Core Document & Search missing APIs (#1-#15)
2. **Phase 2**: Indices missing APIs (#16-#21, #23-#29) — note: #22 ExistsTemplate already implemented
3. **Phase 3**: Cluster & Nodes missing APIs (#30-#44)
4. **Phase 4**: Cat complete coverage (#45-#57)
5. **Phase 5**: Small services — Ingest, Snapshot, Script, Dangling, SearchPipeline, RemoteStore, WLM (#58-#73)
6. **Phase 6**: Info/Ping + deprecate Freeze/Unfreeze (#63-#64 + #0)
7. **Phase 7**: Security plugin completion (#74-#97)
8. **Phase 8**: Alerting plugin completion (#98-#108)
9. **Phase 9**: ISM + Rollup + SM + CCR completion (#109-#124)
10. **Phase 10**: New plugin: Anomaly Detection + Forecasting (#125-#140)
11. **Phase 11**: New plugin: SQL/PPL (#141-#145)
12. **Phase 12**: New plugin: ML Commons (#146-#172)
13. **Phase 13**: New plugin: Async Search, KNN, Neural (#173-#186)

### Return Code Reference

All API methods should return `*types.OpenSearchError` on error (already implemented). Key HTTP status codes:

| Code | Meaning | Client Helper |
|------|---------|---------------|
| 200 | OK | — |
| 201 | Created | — |
| 204 | No Content | — |
| 400 | Bad Request | — |
| 404 | Not Found | `IsNotFound(err)` |
| 409 | Conflict | `IsConflict(err)` |
| 429 | Too Many Requests (rate limit) | — |
| 500 | Internal Server Error | — |

### File Structure for New Services

```
api/
├── ad_service.go          # Anomaly Detection
├── ad_model.go
├── ad_service_test.go
├── async_search_service.go # Asynchronous Search
├── async_search_model.go
├── async_search_service_test.go
├── knn_service.go         # K-NN
├── knn_model.go
├── knn_service_test.go
├── ml_service.go          # ML Commons
├── ml_model.go
├── ml_service_test.go
├── neural_service.go      # Neural Search
├── neural_model.go
├── neural_service_test.go
├── rollup_service.go      # ISM Rollup
├── rollup_model.go
├── rollup_service_test.go
├── sql_service.go         # SQL/PPL
├── sql_model.go
├── sql_service_test.go
├── dangling_service.go    # Dangling Indices
├── dangling_model.go
├── dangling_service_test.go
├── search_pipeline_service.go # Search Pipeline
├── search_pipeline_model.go
├── search_pipeline_service_test.go
├── remote_store_service.go    # Remote Store
├── remote_store_model.go
├── remote_store_service_test.go
├── wlm_service.go         # WLM Stats
├── wlm_model.go
├── wlm_service_test.go
```
