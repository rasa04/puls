To build:
```bash
go build -o puls -trimpath -ldflags "-s -w"
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



Delete empty topics
```bash
./puls delete-empty-topics --verbose
```
