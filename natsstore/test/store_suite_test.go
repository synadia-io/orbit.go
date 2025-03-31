package test

import (
    "github.com/nats-io/nats-server/v2/server"
    "github.com/nats-io/nats-server/v2/test"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"
    "os"
    "testing"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"
)

var jsDir string
var srv *server.Server
var nc *nats.Conn
var js jetstream.JetStream

func TestStorage(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "Storage Suite")
}

var _ = BeforeSuite(func() {
    var err error

    jsDir := os.TempDir()

    opts := test.DefaultTestOptions
    opts.Port = -1
    opts.JetStream = true
    opts.StoreDir = jsDir

    srv := test.RunServer(&opts)
    nc, err := nats.Connect(srv.ClientURL())
    Expect(err).NotTo(HaveOccurred())

    js, err = jetstream.New(nc)
    Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
    defer os.RemoveAll(jsDir)

    if nc != nil {
        nc.Close()
    }

    if srv != nil {
        srv.Shutdown()
    }
})
