<p align="center">
  <img src="orbit.png">
</p>

Orbit is a set of independent utilities around NATS ecosystem that aims to boost
productivity and provide higher abstraction layer for NATS clients.

Note that these libraries will evolve rapidly and API guarantees are not made
until the specific project has a v1.0.0 version.

You can use the library as a whole, or pick just what you need.

# Utilities

| Module                      | Description                                                  | Docs                                 | Version                                                   |
|-----------------------------|--------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------|
| Core NATS Extensions        | Core NATS extensions                                         | [README.md](natsext/README.md)       | [![Go Reference][natsext-image]][natsext-url]             |
| JetStream Extensions        | JetStream extensions                                         | [README.md](jetstreamext/README.md)  | [![Go Reference][jetstreamext-image]][jetstreamext-url]   |
| `natscontext`               | Allow connecting to NATS using NATS Contexts                 | [README.md](natscontext/README.md)   | [![Go Reference][natscontext-image]][natscontext-url]     |
| NATS System Client          | NATS client for NATS monitoring APIs                         | [README.md](natssysclient/README.md) | [![Go Reference][natssysclient-image]][natssysclient-url] |
| Partitioned consumer groups | Client side implementation of partitioned 'consumer groups'  | [README.md](pcgroups/README.md)      | [![Go Reference][pcgroups-image]][pcgroups-url]           |
| KV Codecs                   | Transparent encoding/decoding for JetStream KeyValue stores  | [README.md](kvcodec/README.md)       | [![Go Reference][kvcodec-image]][kvcodec-url]             |
| Counters                    | Distributed counter functionality built on JetStream streams | [README.md](counters/README.md)      | [![Go Reference][counters-image]][counters-url]           |

[natsext-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natsext
[natsext-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natsext.svg
[jetstreamext-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/jetstreamext
[jetstreamext-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/jetstreamext.svg
[natscontext-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natscontext
[natscontext-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natscontext.svg
[natssysclient-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natssysclient
[natssysclient-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natssysclient.svg
[pcgroups-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/pcgroups
[pcgroups-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/pcgroups.svg
[kvcodec-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/kvcodec
[kvcodec-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/kvcodec.svg
[counters-url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/counters
[counters-image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/counters.svg
