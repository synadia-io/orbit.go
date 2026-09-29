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

//go:build !darwin

package ntf

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// linuxPortRangeFile holds the low and high ends of the Linux ephemeral port range.
const linuxPortRangeFile = "/proc/sys/net/ipv4/ip_local_port_range"

// ephemeralPortRange returns the low and high ends of the range the OS draws
// ports for outgoing connections from. Only Linux is supported here.
func ephemeralPortRange() (int, int, error) {
	if runtime.GOOS != "linux" {
		return 0, 0, fmt.Errorf("%w: reading the ephemeral port range on %s", errors.ErrUnsupported, runtime.GOOS)
	}

	b, err := os.ReadFile(linuxPortRangeFile)
	if err != nil {
		return 0, 0, err
	}

	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("%s does not hold a range: %q", linuxPortRangeFile, b)
	}

	low, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, err
	}

	high, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, err
	}

	return low, high, nil
}
