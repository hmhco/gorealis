## Update Aurora job resources
### Build Client
`go build /examples/client.go`
### Run Client
#### Example
```
./client -cmd update \
    -url $CLIENT_AURORA_URL \
    -username $CLIENT_AURORA_USER \
    -password $CLIENT_AURORA_PASS \
    -role hmheng-demo -stage devel \
    -name hello-world-web-app \
    -cpu 0.5 \
    -ram 1024
```
