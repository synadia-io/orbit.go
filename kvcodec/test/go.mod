module tests

go 1.23.0

toolchain go1.23.5

require (
	github.com/nats-io/nats-server/v2 v2.11.4
	github.com/nats-io/nats.go v1.43.0
	github.com/synadia-io/orbit.go/kvcodec v0.0.0
)

require (
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.4 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	golang.org/x/time v0.12.0 // indirect
)

replace github.com/synadia-io/orbit.go/kvcodec => ..
