module tests

go 1.25.0

require (
	github.com/nats-io/nats-server/v2 v2.14.0-RC.1
	github.com/nats-io/nats.go v1.51.0
	github.com/synadia-io/orbit.go/jetstreamext v0.1.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.7.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.1 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.2 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

replace github.com/synadia-io/orbit.go/jetstreamext => ..

// Pin nats.go to the v2.14-dev branch for the AllowBatchPublish stream config field.
// Remove once nats.go with v2.14 is released.
replace github.com/nats-io/nats.go => github.com/nats-io/nats.go v1.50.1-0.20260401165322-1730e36511c5
