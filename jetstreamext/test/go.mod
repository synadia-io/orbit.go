module tests

go 1.23.5

require (
	github.com/nats-io/nats-server/v2 v2.11.0-dev.0.20250131152735-9f9638745def
	github.com/nats-io/nats.go v1.38.1-0.20250201220135-660781ffac83
	github.com/synadia-io/orbit.go/jetstreamext v0.1.0
)

require (
	github.com/google/go-tpm v0.9.3 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/minio/highwayhash v1.0.3 // indirect
	github.com/nats-io/jwt/v2 v2.7.3 // indirect
	github.com/nats-io/nkeys v0.4.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.1 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/time v0.9.0 // indirect
)

replace github.com/synadia-io/orbit.go/jetstreamext => ..
