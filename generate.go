package opensearch

//go:generate mockgen -source=client.go -destination=mock/client.go -package=mock
//go:generate mockgen -source=api/document_service.go -destination=mock/document_service.go -package=mock
//go:generate mockgen -source=api/search_service.go -destination=mock/search_service.go -package=mock
//go:generate mockgen -source=api/indices_service.go -destination=mock/indices_service.go -package=mock
//go:generate mockgen -source=api/cluster_service.go -destination=mock/cluster_service.go -package=mock
//go:generate mockgen -source=api/nodes_service.go -destination=mock/nodes_service.go -package=mock
//go:generate mockgen -source=api/cat_service.go -destination=mock/cat_service.go -package=mock
//go:generate mockgen -source=api/ingest_service.go -destination=mock/ingest_service.go -package=mock
//go:generate mockgen -source=api/snapshot_service.go -destination=mock/snapshot_service.go -package=mock
//go:generate mockgen -source=api/tasks_service.go -destination=mock/tasks_service.go -package=mock
//go:generate mockgen -source=api/script_service.go -destination=mock/script_service.go -package=mock
//go:generate mockgen -source=api/security_service.go -destination=mock/security_service.go -package=mock
//go:generate mockgen -source=api/ism_service.go -destination=mock/ism_service.go -package=mock
//go:generate mockgen -source=api/sm_service.go -destination=mock/sm_service.go -package=mock
//go:generate mockgen -source=api/alerting_service.go -destination=mock/alerting_service.go -package=mock
//go:generate mockgen -source=api/transform_service.go -destination=mock/transform_service.go -package=mock
//go:generate mockgen -source=api/ccr_service.go -destination=mock/ccr_service.go -package=mock
//go:generate mockgen -source=api/rollup_service.go -destination=mock/rollup_service.go -package=mock
//go:generate mockgen -source=api/ml_service.go -destination=mock/ml_service.go -package=mock
//go:generate mockgen -source=api/async_search_service.go -destination=mock/async_search_service.go -package=mock
//go:generate mockgen -source=api/knn_service.go -destination=mock/knn_service.go -package=mock
//go:generate mockgen -source=api/neural_service.go -destination=mock/neural_service.go -package=mock
//go:generate mockgen -source=api/sql_service.go -destination=mock/sql_service.go -package=mock
//go:generate mockgen -source=api/ad_service.go -destination=mock/ad_service.go -package=mock
