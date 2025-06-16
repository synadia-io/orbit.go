# KV Codecs for NATS JetStream

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/kvcodec
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/kvcodec
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/kvcodec.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/kvcodec.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/kvcodec
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/kvcodec.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

This module provides codec capabilities for NATS JetStream KeyValue stores, allowing transparent encoding/decoding of keys and values while maintaining full KV API compatibility.

## Overview

The kvcodec module wraps existing JetStream KeyValue stores to add encoding/decoding capabilities with separate key and value codecs. This enables scenarios like:

- **Character escaping** - Use characters in keys that would normally be invalid
- **Path notation** - Use familiar `/path/style` keys while storing as NATS subjects
- **Value encoding** - Encode values independently from keys (e.g., Base64 encoding)
- **Custom transformations** - Implement your own codecs for specific needs
- **Codec chaining** - Chain multiple codecs together for complex transformations

## Installation

```bash
go get github.com/synadia-io/orbit.go/kvcodec
```

## Usage

### Basic Example

```go
// Create JetStream context
js, _ := jetstream.New(nc)

// Create a regular KV bucket
kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
    Bucket: "my-bucket",
})

// Wrap with separate key and value codecs
// Use Base64 for both keys and values to escape special characters
base64Codec := kvcodec.Base64Codec()
codecKV := kvcodec.New(kv, base64Codec, base64Codec)

// Use it like a regular KV - the codecs are transparent
codecKV.Put(ctx, "Acme Inc.contact", []byte("info@acme.com"))

// Keys with special characters work seamlessly
entry, _ := codecKV.Get(ctx, "Acme Inc.contact")
log.Printf("Key: %s, Value: %s", entry.Key(), entry.Value())
```

### Constructor Functions

#### kvcodec.New(kv, keyCodec, valueCodec)

Creates a new codec-enabled KeyValue wrapper with separate key and value codecs:

```go
base64Codec := kvcodec.Base64Codec()
noOpCodec := kvcodec.NoOpCodec()

// Encode both keys and values with Base64
codecKV := kvcodec.New(kv, base64Codec, base64Codec)

// Only encode keys, leave values unchanged
codecKV = kvcodec.New(kv, base64Codec, noOpCodec)
```

#### kvcodec.NewForKey(kv, keyCodec)

Creates a wrapper with only key encoding enabled (values use NoOpCodec):

```go
pathCodec := kvcodec.PathCodec()
codecKV := kvcodec.NewForKey(kv, pathCodec)

// Keys are path-converted, values are unchanged
codecKV.Put(ctx, "config/app/database", []byte("postgres://localhost"))
```

#### kvcodec.NewForValue(kv, valueCodec)

Creates a wrapper with only value encoding enabled (keys use NoOpCodec):

```go
base64Codec := kvcodec.Base64Codec()
codecKV := kvcodec.NewForValue(kv, base64Codec)

// Keys are unchanged, values are Base64-encoded
codecKV.Put(ctx, "config.database", []byte("postgres://localhost"))
```

### Available Codecs

#### NoOpCodec

A no-op codec that passes data through unchanged. Implements both KeyCodec and ValueCodec interfaces.

```go
noOpCodec := kvcodec.NoOpCodec()
codecKV := kvcodec.New(kv, noOpCodec, noOpCodec)
```

#### Base64Codec

Encodes keys/values using base64 encoding. Useful for escaping special characters. Supports wildcard patterns for keys.

```go
base64Codec := kvcodec.Base64Codec()

// Use for keys, values, or both
codecKV := kvcodec.New(kv, base64Codec, base64Codec)

// "Acme Inc.contact" becomes base64-encoded
codecKV.Put(ctx, "Acme Inc.contact", []byte("info@acme.com"))
```

#### PathCodec

Translates between path-style keys (`/foo/bar`) and NATS-style keys (`foo.bar`). Only implements KeyCodec interface.

```go
pathCodec := kvcodec.PathCodec()
codecKV := kvcodec.NewForKey(kv, pathCodec)

// Use familiar path notation
codecKV.Put(ctx, "/config/app/database", []byte("postgres://localhost"))

// Internally stored as "config.app.database"
```

### Advanced Usage

#### Wildcards and Watching

Filterable codecs preserve wildcard functionality by encoding tokens individually:

```go
base64Codec := kvcodec.Base64Codec()
codecKV := kvcodec.New(kv, base64Codec, base64Codec)

// Watch with wildcards - tokens are encoded individually
watcher, _ := codecKV.Watch(ctx, "orders.>")
defer watcher.Stop()

// Put some encoded data
codecKV.Put(ctx, "orders.12345", []byte("Order details"))
codecKV.Put(ctx, "orders.67890", []byte("Another order"))

// Receive decoded updates
for entry := range watcher.Updates() {
    log.Printf("Order %s: %s", entry.Key(), entry.Value())
}
```

#### Chaining Codecs

Chain multiple codecs together for complex transformations using separate chain constructors:

```go
// Create individual codecs
pathCodec := kvcodec.PathCodec()
base64Codec := kvcodec.Base64Codec()

// Chain key codecs: Path conversion first, then Base64 encoding
keyChain, _ := kvcodec.NewKeyChainCodec(pathCodec, base64Codec)

// Chain value codecs: Base64 encoding only
valueChain, _ := kvcodec.NewValueChainCodec(base64Codec)

// Use the chained codecs
codecKV := kvcodec.New(kv, keyChain, valueChain)

// Keys are path-converted AND base64-encoded, values are base64-encoded
codecKV.Put(ctx, "/config/app/setting", []byte("configuration data"))
```

**Chain Constructors:**

- `NewKeyChainCodec(codecs ...KeyCodec)` - Chain key codecs
- `NewValueChainCodec(codecs ...ValueCodec)` - Chain value codecs

#### Custom Codec Implementation

Implement the `KeyCodec` and/or `ValueCodec` interfaces to create custom transformations:

```go
// Key-only codec
type UppercaseKeyCodec struct{}

func (u *UppercaseKeyCodec) EncodeKey(key string) (string, error) {
    return strings.ToUpper(key), nil
}

func (u *UppercaseKeyCodec) DecodeKey(key string) (string, error) {
    return strings.ToLower(key), nil
}

// Value-only codec  
type UppercaseValueCodec struct{}

func (u *UppercaseValueCodec) EncodeValue(value []byte) ([]byte, error) {
    return bytes.ToUpper(value), nil
}

func (u *UppercaseValueCodec) DecodeValue(value []byte) ([]byte, error) {
    return bytes.ToLower(value), nil
}

// Use your custom codecs
codecKV := kvcodec.New(kv, &UppercaseKeyCodec{}, &UppercaseValueCodec{})

// Or implement both interfaces in one struct
type UppercaseCodec struct{}

// ... implement both KeyCodec and ValueCodec methods ...

codecKV := kvcodec.New(kv, &UppercaseCodec{}, &UppercaseCodec{})
```

**Optional FilterableKeyCodec Interface:**

For wildcard support, implement `FilterableKeyCodec`:

```go
func (u *UppercaseKeyCodec) EncodeFilter(filter string) (string, error) {
    // Handle wildcard patterns specially
    return encodeFilterPreservingWildcards(filter), nil
}
```
