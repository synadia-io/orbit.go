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
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:4222"
	}

	nc, err := nats.Connect(url)
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

	// Batch Direct Get reads straight from the stream's storage, so the
	// stream must be created with AllowDirect enabled.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        "ORDERS",
		Subjects:    []string{"orders.>"},
		AllowDirect: true,
	}); err != nil {
		log.Fatalf("create stream: %v", err)
	}

	// Seed the stream so there is something to read back.
	for _, payload := range []string{
		`{"order":"ORD-42","sku":"COFFEE-1KG","qty":2}`,
		`{"order":"ORD-42","sku":"FILTER-100","qty":1}`,
		`{"order":"ORD-42","sku":"MUG-WHITE","qty":4}`,
	} {
		if _, err := js.Publish(ctx, "orders.created", []byte(payload)); err != nil {
			log.Fatalf("publish: %v", err)
		}
	}

	// NATS-DOC-START
	// Fetch up to 3 messages from ORDERS in a single Direct Get request,
	// starting at stream sequence 1, and iterate them in order.
	msgs, err := jetstreamext.GetBatch(ctx, js, "ORDERS", 3, jetstreamext.GetBatchSeq(1))
	if err != nil {
		log.Fatalf("get batch: %v", err)
	}
	for msg, err := range msgs {
		if err != nil {
			log.Fatalf("read message: %v", err)
		}
		fmt.Printf("seq %d on %s\n", msg.Sequence, msg.Subject)
	}
	// NATS-DOC-END
}
