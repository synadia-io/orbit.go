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

package chaos

import (
	"bytes"
	_ "embed"
)

// definitionSchema is the hand-written JSON Schema of a Definition.
//
//go:embed schemas/ntf/chaos/v1/definition.json
var definitionSchema []byte

// DefinitionSchema returns the JSON Schema of a run definition. It covers the
// definition's own structure and leaves each driver's and action's params
// open; those are checked against the schemas the drivers and kinds publish,
// KindInfo.Schema and DriverInfo.Schema, in a second stage when a run starts.
func DefinitionSchema() []byte {
	return bytes.Clone(definitionSchema)
}
