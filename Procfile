# Use goreman to run `go install github.com/mattn/goreman@latest`
# exec `goreman start`
Deimos1: bin/deimos -id 0x1 -bind-addr 127.0.0.1:9927 -peer-bind-addr :7001 -peers '0x1=localhost:7001&0x2=localhost:7002&0x3=localhost:7003'
Deimos2: bin/deimos -id 0x2 -bind-addr 127.0.0.1:9928 -peer-bind-addr :7002 -peers '0x1=localhost:7001&0x2=localhost:7002&0x3=localhost:7003'
Deimos3: bin/deimos -id 0x3 -bind-addr 127.0.0.1:9929 -peer-bind-addr :7003 -peers '0x1=localhost:7001&0x2=localhost:7002&0x3=localhost:7003'
proxy:   bin/deimos -proxy=on -bind-addr 127.0.0.1:10000 -peers '0x1=localhost:7001&0x2=localhost:7002&0x3=localhost:7003'
