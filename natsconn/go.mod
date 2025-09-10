module github.com/synadia-io/orbit.go/natsconn

go 1.24.0

replace github.com/synadia-io/orbit.go/natscontext => ../natscontext

require (
	github.com/nats-io/nats.go v1.45.0
	github.com/nats-io/nkeys v0.4.11
	github.com/synadia-io/orbit.go/natscontext v0.0.0-00010101000000-000000000000
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.4.3-default-no-op // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.4 // indirect
	github.com/nats-io/nats-server/v2 v2.11.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/time v0.13.0 // indirect
)
