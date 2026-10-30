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

import (
	"slices"
	"testing"
)

// toxicTypes are the Toxic* type constants.
var toxicTypes = []string{
	ToxicCut,
	ToxicLatency,
	ToxicBandwidth,
	ToxicSlowClose,
	ToxicTimeout,
	ToxicResetPeer,
	ToxicSlicer,
	ToxicLimitData,
	ToxicLossy,
}

// TestToxicAttributesCoverTypes proves every toxic type has an entry and every
// entry is a known type.
func TestToxicAttributesCoverTypes(t *testing.T) {
	for _, typ := range toxicTypes {
		_, ok := ToxicAttributes[typ]
		if !ok {
			t.Errorf("toxic type %q has no entry", typ)
		}
	}

	for typ := range ToxicAttributes {
		if !slices.Contains(toxicTypes, typ) {
			t.Errorf("entry %q is not a known toxic type", typ)
		}
	}
}

// TestToxicAttributesOmitAgentAttributes proves no entry lists an attribute
// the agent sets itself.
func TestToxicAttributesOmitAgentAttributes(t *testing.T) {
	for typ, attrs := range ToxicAttributes {
		for _, attr := range attrs {
			switch attr.Name {
			case "toxicity", "agent", "stream":
				t.Errorf("toxic type %q lists attribute %q, which the agent sets", typ, attr.Name)
			}
		}
	}
}
