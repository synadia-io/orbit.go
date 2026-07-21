// Copyright 2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("new jetstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Atomic batch publishing requires the stream to be created with
	// AllowAtomicPublish enabled (and nats-server v2.12.0 or later).
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:               "ORDERS",
		Subjects:           []string{"orders.>"},
		AllowAtomicPublish: true,
	}); err != nil {
		log.Fatalf("create stream: %v", err)
	}

	// NATS-DOC-START
	// Publish the three line items of order ORD-42 as one atomic batch:
	// all three land in the ORDERS stream, or none do.
	batch, err := jetstreamext.NewBatchPublisher(js)
	if err != nil {
		log.Fatalf("create batch publisher: %v", err)
	}
	if err := batch.Add("orders.created", []byte(`{"order":"ORD-42","sku":"COFFEE-1KG","qty":2}`)); err != nil {
		log.Fatalf("add line item: %v", err)
	}
	if err := batch.Add("orders.created", []byte(`{"order":"ORD-42","sku":"FILTER-100","qty":1}`)); err != nil {
		log.Fatalf("add line item: %v", err)
	}
	// Commit sends the final line item and atomically commits the batch.
	ack, err := batch.Commit(ctx, "orders.created", []byte(`{"order":"ORD-42","sku":"MUG-WHITE","qty":4}`))
	if err != nil {
		log.Fatalf("commit batch: %v", err)
	}
	log.Printf("batch %q committed: %d line items stored", ack.BatchID, ack.BatchSize)
	// NATS-DOC-END
}
