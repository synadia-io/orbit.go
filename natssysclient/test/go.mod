module tests

go 1.23.2

require (
	github.com/nats-io/nats-server/v2 v2.10.22
	github.com/nats-io/nats.go v1.38.0
	github.com/synadia-io/orbit.go/natssysclient v0.1.0
)

require (
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.3 // indirect
	github.com/nats-io/nkeys v0.4.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/time v0.7.0 // indirect
)

replace github.com/synadia-io/orbit.go/natssysclient => ..
