# Use goreman to run `go install github.com/mattn/goreman@latest`
# exec `goreman start`
deimos1: bin/deimos -name node1 -bind-addr 127.0.0.1:4001 -peer-bind-addr :7001 -bootstrap-config 'node1=localhost:7001,node2=localhost:7002,node3=localhost:7003'
 deimos2: bin/deimos -name node2 -bind-addr 127.0.0.1:4002 -peer-bind-addr :7002 -bootstrap-config 'node1=localhost:7001,node2=localhost:7002,node3=localhost:7003'
 deimos3: bin/deimos -name node3 -bind-addr 127.0.0.1:4003 -peer-bind-addr :7003 -bootstrap-config 'node1=localhost:7001,node2=localhost:7002,node3=localhost:7003'
 #proxy: bin/deimos -proxy=on -bind-addr 127.0.0.1:8080 -peers 'localhost:7001,localhost:7002,localhost:7003'
