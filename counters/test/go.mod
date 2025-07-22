module test

go 1.23.5

require (
	github.com/nats-io/nats-server/v2 v2.11.4-0.20250721141514-425a7e52c079
	github.com/nats-io/nats.go v1.43.1-0.20250722080352-3b61443e4534
	github.com/synadia-io/orbit.go/counters v0.0.0-00010101000000-000000000000
)

replace github.com/synadia-io/orbit.go/counters => ../

require (
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.4 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/jetstreamext v0.1.0 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.1 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/time v0.12.0 // indirect
)
