# NATS Context Connection Helper

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/natscontext
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/natscontext
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/natscontext.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/natscontext.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natscontext
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natscontext.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

This is a package that helps Go developers connect to NATS using a NATS Context as featured in the `nats` Command Line
Tool.

## Installation

```bash
go get github.com/synadia-io/orbit.go/natscontext
```

## Usage

Using the `nats` command line create a Context that can connect to your server, here we use a credential in a file:

```bash
nats context add staging --creds /home/user/staging.creds --js-domain STAGING
```

We can now use the context called `staging` from Go:

```go
nc, settings, err := natscontext.Connect("staging", nats.Name("my application"))
if err != nil {
	// handle error
}

// Get a JetStream handle using the domain in the context
js, err := jetstream.NewWithDomain(nc, settings.JSDomain)
if err != nil {
	// handle error
}
```

If the full path to a context JSON file is given instead of the friendly name then that file will be used.

All context settings are supported except Windows Certificate Store related settings.