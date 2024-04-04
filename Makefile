.PHONY: start
start:
	docker-compose up -d
	sleep 30
	curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz "https://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=500s"
	curl --fail -XPUT -k -u admin:vLPeJYa8.3RqtZCcAK6jNz -H 'Content-Type: application/json' "https://localhost:9200/_index_template/socle" -d '{"index_patterns":["*"],"priority":500,"template":{"settings":{"number_of_shards":1,"number_of_replicas":0}}}'
    

.PHONY: test
test:
	go test -covermode=atomic -coverprofile cover.out -deprecations -strict-decoder -v . ./config/... ./trace/... ./uritemplates/...

