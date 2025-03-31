# NATS JetStream Store

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/natsstore
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/natsstore
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/natsstore.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/natsstore.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natsstore
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natsstore.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

Nats JetStream Store library provides a client to make it easier to perform common operations on a JetStream KV bucket.

## Installation

```bash
go get github.com/synadia-io/orbit.go/natsstore
```

## Usage

The client serves as a wrapper around a KV bucket. However, it also requires access to the underlying
JetStream stream to support low-level purging. Therefor, the client will look for the bucket and underlying stream.

```go
// connect to nats
nc, err := nats.Connect(nats.DefaultURL)
if err != nil {
    // handle error
}
defer nc.Close()

// get the jetstream client
js, err := jetstream.New(nc)
if err != nil {
    // handle error
}

// get a store wrapping an existing JetStream KV bucket, using a json 
// codec to persist entries
store, err = natsstore.NewKeyValueStore(js, "myBucket", natsstore.NewJsonCodec())
if err != nil {
    // handle error
}

// add a new entry to the store but fail if it already exists
_, err := store.Apply(context.Background(), "entries.entry_one", func(entry natsstore.Entry) ([]byte, error) {
    if !entry.IsNew() {
        return nil, fmt.Errorf("entry already exists") 
    }   
    
    return entry.Encode("Hello World")
})
if err != nil {
    // handle error
}

// update an entry in the store, but fail if it does not exist
_, err := store.Apply(context.Background(), "entries.entry_one", func(entry natsstore.Entry) ([]byte, error) {
    if entry.IsNew() {
        return nil, fmt.Errorf("entry does not exist")
    }
    
    return entry.Encode("Goodbye, cruel world")
})
if err != nil {
    // handle error
}

// list the entries in the store
fmt.Println("Entries:")
err := store.List(context.Background(), "entries.>", func(entry natsstore.Entry, hasMore bool) error {
    fmt.Printf(" - %s\n", entry.Key())
    return nil
})
if err != nil {
    // handle error
}
```

### Codecs
The store entries are stored as byte slices, but the store can be configured with a codec to encode and decode the 
entries. The library provides a `Json` codec which can optionally be wrapped by a `Secured` codec. The latter will
encrypt/decrypt all entry data written/read to/from the store. You can also implement your own codec by implementing 
the `Codec` interface and passing it when constructing the store.
