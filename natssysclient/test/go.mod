module tests

go 1.25.0

require (
	github.com/nats-io/nats-server/v2 v2.12.6
	github.com/nats-io/nats.go v1.50.0
	github.com/synadia-io/orbit.go/natssysclient v0.1.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.6.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/minio/highwayhash v1.0.4-0.20251030100505-070ab1a87a76 // indirect
	github.com/nats-io/jwt/v2 v2.8.1 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.2 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/synadia-io/orbit.go/natssysclient => ..
