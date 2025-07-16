# Use goreman to run `go install github.com/mattn/goreman@latest`
# exec `goreman start`
deimos1: bin/deimos -name node1 -listen-client-urls http://127.0.0.1:4001 -advertise-client-urls http://127.0.0.1:4001 -listen-peer-urls http://127.0.0.1:7001 -advertise-peer-urls http://127.0.0.1:7001 -bootstrap-config 'node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003'
deimos2: bin/deimos -name node2 -listen-client-urls http://127.0.0.1:4002 -advertise-client-urls http://127.0.0.1:4002 -listen-peer-urls http://127.0.0.1:7002 -advertise-peer-urls http://127.0.0.1:7002 -bootstrap-config 'node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003'
deimos3: bin/deimos -name node3 -listen-client-urls http://127.0.0.1:4003 -advertise-client-urls http://127.0.0.1:4003 -listen-peer-urls http://127.0.0.1:7003 -advertise-peer-urls http://127.0.0.1:7003 -bootstrap-config 'node1=http://localhost:7001,node2=http://localhost:7002,node3=http://localhost:7003'
#proxy: bin/deimos -proxy=on -bind-addr 127.0.0.1:8080 -peers 'localhost:7001,localhost:7002,localhost:7003'
