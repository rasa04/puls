To build:
```bash
go build -o puls -trimpath -ldflags "-s -w"
#To build for linux on mac
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

List topics
```bash
# List with backlog > 0
./puls list
# List all topics
./puls list --full
# List topics with partitioned
./puls list --with-partitioned
# List topics with verbose logs
./puls list --verbose
```

Show full info about certain topic
```bash
# You can pass full name:
./puls show persistent://tenant/ns/topic
# Or just topic name (tenant/namespace will be taken from current context):
./puls show topic
```

Delete topic(s)
```bash
# Dry-run (recommended first):
./puls delete --dry-run topic1 topic2

# Delete single topic (auto-detect partitioned/non-partitioned):
./puls delete topic

# Delete by full name:
./puls delete persistent://tenant/ns/topic

# Force kind if you know it:
./puls delete --kind=partitioned topic
./puls delete --kind=non-partitioned topic

# Override tenant/namespace from context:
./puls delete --tenant project --namespace dev topic
```

Delete empty topics
```bash
# Default is DRY-RUN (safe): prints what would be deleted
./puls delete-empty-topics --verbose

# Actually delete (danger!):
./puls delete-empty-topics --verbose --dry-run=false

# With prefix filter override:
./puls delete-empty-topics --prefix stand1 --dry-run=false
```
