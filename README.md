To build:
```bash
go build -o puls -trimpath -ldflags "-s -w"
```

To build for linux on mac
```bash
docker run --rm \
  -v "$PWD":/app \
  -w /app \
  golang:1.23 \
  bash -c 'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o puls -trimpath -ldflags "-s -w" ./cmd/puls'
```

To set up context:
```bash
./puls context set \
  --name stage \
  --url http://your-pulsar-url:8080/admin/v2 \
  --tenant project \
  --namespace dev \
  --prefix stand1
```

List all topics
```bash
./puls list --full
```

List topics
```bash
./puls list
```

List topics with partitioned
```bash
./puls list --with-partitioned
```

List topics with verbose logs
```bash
./puls list --verbose
```

Delete empty topics
```bash
./puls delete-empty-topics --verbose
```
