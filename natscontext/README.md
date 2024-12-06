# NATS Context Connection Helper

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