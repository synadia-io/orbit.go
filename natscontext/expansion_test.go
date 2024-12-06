// Copyright 2022 The NATS Authors
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

package natscontext

import "testing"

func TestContext(t *testing.T) {
	// Do nothing if the string does not start with ~
	t1 := expandHomedir("foo")
	if t1 != "foo" {
		t.Fatalf("failed to expand home directory for string 'foo': %s", t1)
	}

	// Expand ~ to HOMEDIR if string starts with ~
	t2 := expandHomedir("~/foo")
	if t2 == "~/foo" {
		t.Fatalf("failed to expand home directory for string 'foo': %s", t2)
	}

	// Do nothing if there is a ~ but it is not the first character of the string
	t3 := expandHomedir("/~/foo")
	if t3 != "/~/foo" {
		t.Fatalf("failed to expand home directory for string 'foo': %s", t3)
	}
}
