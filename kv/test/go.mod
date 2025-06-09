module tests

go 1.23.5

require (
	github.com/nats-io/nats-server/v2 v2.11.3
	github.com/nats-io/nats.go v1.42.0
	github.com/synadia-io/orbit.go/kv v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/go-tpm v0.9.3 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.4 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/time v0.11.0 // indirect
)

replace (
	github.com/nats-io/nats.go => ../../../nats.go
	github.com/synadia-io/orbit.go/kv => ..
)
