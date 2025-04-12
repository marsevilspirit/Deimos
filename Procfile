# Use goreman to run `go install github.com/mattn/goreman@latest`
# exec `goreman start`
Deimos1: ./deimos -id 0x1 -l :9927 -peers '0x1=localhost:9927&0x2=localhost:9928&0x3=localhost:9929'
Deimos2: ./deimos -id 0x2 -l :9928 -peers '0x1=localhost:9927&0x2=localhost:9928&0x3=localhost:9929'
Deimos3: ./deimos -id 0x3 -l :9929 -peers '0x1=localhost:9927&0x2=localhost:9928&0x3=localhost:9929'
proxy:   ./deimos -proxy-mode -l :10000 -peers '0x1=localhost:9927&0x2=localhost:9928&0x3=localhost:9929'
