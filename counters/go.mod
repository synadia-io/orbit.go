module github.com/synadia-io/orbit.go/counters

go 1.23.5

require (
	github.com/nats-io/nats.go v1.43.1-0.20250115000000-000000000000
	github.com/synadia-io/orbit.go/jetstreamext v0.1.0
)

replace github.com/nats-io/nats.go => github.com/nats-io/nats.go v1.43.1-0.20250715134023-41c7d362ea9a

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/synadia-io/orbit.go/natsext v0.1.1 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
)
