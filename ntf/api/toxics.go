// Copyright 2026 Synadia Communications Inc.
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

package api

// ToxicUnit is the unit of a toxic attribute's value, written out.
type ToxicUnit string

const (
	// ToxicUnitMilliseconds is a duration in milliseconds.
	ToxicUnitMilliseconds ToxicUnit = "milliseconds"
	// ToxicUnitMicroseconds is a duration in microseconds.
	ToxicUnitMicroseconds ToxicUnit = "microseconds"
	// ToxicUnitKilobytesPerSecond is a rate in kilobytes per second.
	ToxicUnitKilobytesPerSecond ToxicUnit = "kilobytes per second"
	// ToxicUnitBytes is a size in bytes.
	ToxicUnitBytes ToxicUnit = "bytes"
	// ToxicUnitNone is a value without a unit, such as a fraction.
	ToxicUnitNone ToxicUnit = "none"
)

const (
	// toxicMaxMillis bounds every attribute in milliseconds, a day, well short
	// of where toxiproxy's arithmetic overflows.
	toxicMaxMillis = 24 * 60 * 60 * 1000

	// toxicMaxSlicerDelay bounds the slicer's delay, in microseconds, a minute.
	toxicMaxSlicerDelay = 60 * 1000 * 1000

	// toxicMaxSlicerSize bounds the slicer's sizes, a MiB, more than a proxy
	// reads at once.
	toxicMaxSlicerSize = 1 << 20

	// toxicMaxBandwidthRate bounds the bandwidth toxic's rate, in kilobytes per
	// second.
	toxicMaxBandwidthRate = 1_000_000_000

	// toxicMaxLimitBytes bounds limit_data's bytes, which JSON carries as a
	// double.
	toxicMaxLimitBytes = 1 << 50
)

// ToxicAttribute is one attribute a toxic type accepts in Toxic.Attributes.
//
// An attribute that is not a fraction is an integer from Minimum to Maximum,
// both included. A fraction is a number above Minimum, 0, and below Maximum,
// 1, both excluded.
//
// Default is the value the attribute takes when the caller leaves it out. It
// is nil for an attribute that has none, which the caller must give.
type ToxicAttribute struct {
	// Name is the attribute's key in Toxic.Attributes.
	Name string
	// Minimum is the smallest value accepted, excluded for a fraction.
	Minimum int64
	// Maximum is the largest value accepted, excluded for a fraction.
	Maximum int64
	// Default is the value used when the attribute is left out, nil when it is
	// required.
	Default *int64
	// Fraction is set for a number strictly between 0 and 1.
	Fraction bool
	// Unit is the unit of the value.
	Unit ToxicUnit
	// Description says what the attribute does.
	Description string
}

// ToxicAttributes holds the attributes each toxic type accepts, keyed by the
// Toxic* constants, by toxiproxy's names and in its units, and for ToxicLossy
// the agent's own. A cut takes none.
//
// It holds the rules of single attributes only. The agent also checks rules no
// single attribute expresses: a slicer's size_variation must be below its
// average_size, a lossy's max_rto at least its rto, a toxic's name must be 1 to
// 64 letters, digits, '.', '_' or '-', its toxicity in [0, 1], and a cut takes
// no toxicity other than 1. Toxicity is Toxic.Toxicity, never an attribute, and
// the agent refuses toxicity, agent and stream as attributes, since it sets
// them itself.
var ToxicAttributes = map[string][]ToxicAttribute{
	ToxicCut: nil,
	ToxicLatency: {
		{Name: "latency", Minimum: 0, Maximum: toxicMaxMillis, Default: new(int64(0)), Unit: ToxicUnitMilliseconds, Description: "How long data is delayed."},
		{Name: "jitter", Minimum: 0, Maximum: toxicMaxMillis, Default: new(int64(0)), Unit: ToxicUnitMilliseconds, Description: "How much the delay varies either way."},
	},
	ToxicBandwidth: {
		{Name: "rate", Minimum: 1, Maximum: toxicMaxBandwidthRate, Unit: ToxicUnitKilobytesPerSecond, Description: "The most data passed each second."},
	},
	ToxicSlowClose: {
		{Name: "delay", Minimum: 0, Maximum: toxicMaxMillis, Default: new(int64(0)), Unit: ToxicUnitMilliseconds, Description: "How long the close of a connection is delayed."},
	},
	ToxicTimeout: {
		{Name: "timeout", Minimum: 0, Maximum: toxicMaxMillis, Default: new(int64(0)), Unit: ToxicUnitMilliseconds, Description: "How long data is dropped before the connection is closed, never closed when 0."},
	},
	ToxicResetPeer: {
		{Name: "timeout", Minimum: 0, Maximum: toxicMaxMillis, Default: new(int64(0)), Unit: ToxicUnitMilliseconds, Description: "How long after data arrives the connection is reset."},
	},
	ToxicSlicer: {
		{Name: "average_size", Minimum: 1, Maximum: toxicMaxSlicerSize, Unit: ToxicUnitBytes, Description: "The average size of a piece the data is cut into."},
		{Name: "size_variation", Minimum: 0, Maximum: toxicMaxSlicerSize, Default: new(int64(0)), Unit: ToxicUnitBytes, Description: "How much the size of a piece varies either way."},
		{Name: "delay", Minimum: 0, Maximum: toxicMaxSlicerDelay, Default: new(int64(0)), Unit: ToxicUnitMicroseconds, Description: "How long apart the pieces are sent."},
	},
	ToxicLimitData: {
		{Name: "bytes", Minimum: 0, Maximum: toxicMaxLimitBytes, Default: new(int64(0)), Unit: ToxicUnitBytes, Description: "How many bytes pass before the connection is closed."},
	},
	ToxicLossy: {
		{Name: "loss", Minimum: 0, Maximum: 1, Fraction: true, Unit: ToxicUnitNone, Description: "The chance that a segment is lost."},
		{Name: "rto", Minimum: 1, Maximum: toxicMaxMillis, Default: new(int64(200)), Unit: ToxicUnitMilliseconds, Description: "How long a lost segment stalls the stream."},
		{Name: "max_rto", Minimum: 1, Maximum: toxicMaxMillis, Default: new(int64(3200)), Unit: ToxicUnitMilliseconds, Description: "The longest stall of one retransmission, which doubles each time it is lost again."},
	},
}
