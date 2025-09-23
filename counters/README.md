# Distributed Counters for NATS JetStream

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/counters
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/counters
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/counters.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/counters.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/counters
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/counters.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

This module provides distributed counter functionality built on NATS JetStream streams, enabling high-performance counter operations with arbitrary precision and source tracking capabilities.

## Overview

The counters module wraps JetStream streams configured with `AllowMsgCounter` to provide distributed counters. Each subject in the stream represents a separate counter.

Counters are tracked across multiple sources, allowing for aggregation and source history. The module supports operations like incrementing/decrementing counters, loading current values, and retrieving source contributions.

## Installation

```bash
go get github.com/synadia-io/orbit.go/counters
```

## Usage

### Basic Example

```go
// Create JetStream context
js, _ := jetstream.New(nc)

// Create a stream configured for counters
stream, _ := js.CreateStream(ctx, jetstream.StreamConfig{
    Name:            "COUNTERS",
    Subjects:        []string{"events.>"},
    AllowMsgCounter: true,  // Enable counter functionality
    AllowDirect:     true,  // Required for reading multiple subjects
})

// Wrap the stream as a counter
counter, _ := counters.NewCounterFromStream(js, stream)

// Increment a counter
newValue, _ := counter.Add(ctx, "events.orders", big.NewInt(1))
log.Printf("Orders count: %s", newValue)

// Load current value
value, _ := counter.Load(ctx, "events.orders")
log.Printf("Current orders: %s", value.Val)
```

### Counter Operations

#### Add - Increment/Decrement Counters

Adds a value to the counter and returns the new total:

```go
// Increment by 1
newValue, _ := counter.Add(ctx, "events.clicks", big.NewInt(1))

// Increment by 100
newValue, _ := counter.Add(ctx, "events.purchases", big.NewInt(100))

// Decrement by 5
newValue, _ := counter.Add(ctx, "inventory.items", big.NewInt(-5))

// Handle very large numbers
largeValue := new(big.Int)
largeValue.SetString("9999999999999999999999999999", 10)
newValue, _ := counter.Add(ctx, "events.large", largeValue)
```

#### Load - Get Current Value

Retrieves the current value of a single counter:

```go
value, _ := counter.Load(ctx, "events.orders")
log.Printf("Orders: %s", value.Val)
log.Printf("Subject: %s", value.Subject)
```

#### Get - Get Counter entry with Source Tracking

Retrieves the counter entry along with its source tracking information:

```go
// Get single entry with source tracking
entry, _ := counter.GetEntry(ctx, "aggregated.total")
log.Printf("Total value: %s", entry.Value)

// Show contributions from each source stream
for sourceID, subjects := range entry.Sources {
    log.Printf("Source %s contributions:", sourceID)
    for subject, value := range subjects {
        log.Printf("  %s: %s", subject, value)
    }
}

```

#### GetMultiple - Get Multiple Entries

Loads multiple counter entries with wildcard support:

```go
// Get multiple entries with wildcards
for entry, err := range counter.GetEntries(ctx, []string{"aggregated.>"}) {
    if err != nil {
        log.Printf("Error: %v", err)
        continue
    }
    log.Printf("%s: %s (from %d sources)", 
        entry.Subject, entry.Value, len(entry.Sources))
}
```

### Error Handling

The module provides specific error types for different scenarios:

```go
// Stream not configured for counters
counter, err := counters.NewCounterFromStream(js, stream)
if errors.Is(err, counters.ErrCounterNotEnabled) {
    log.Fatal("Stream must have AllowMsgCounter enabled")
}

// Counter doesn't exist
value, err := counter.Load(ctx, "nonexistent.counter")
if errors.Is(err, counters.ErrCounterNotFound) {
    log.Println("Counter not found, initializing...")
    counter.Add(ctx, "nonexistent.counter", big.NewInt(0))
}

// Invalid counter value
_, err = counter.Add(ctx, "events.orders", nil)
if errors.Is(err, counters.ErrInvalidCounterValue) {
    log.Println("Value cannot be nil")
}
```
