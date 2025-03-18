# Use goreman to run `go install github.com/mattn/goreman@latest`
# exec `goreman start`
mars1: ./marstore -id 0x1 -l :6618 -peers '0x0=localhost:6618&0x1=localhost:6619&0x2=localhost:6620'
mars2: ./marstore -id 0x2 -l :6619 -peers '0x0=localhost:6618&0x1=localhost:6619&0x2=localhost:6620'
mars3: ./marstore -id 0x3 -l :6620 -peers '0x0=localhost:6618&0x1=localhost:6619&0x2=localhost:6620'
