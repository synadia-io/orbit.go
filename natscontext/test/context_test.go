package test

import (
	"encoding/json"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natscontext"
	"os"
	"testing"
	"time"
)

func TestContext(t *testing.T) {
	srv, _ := RunServerWithConfig("testdata/server.conf")
	defer srv.Shutdown()

	nctx := natscontext.Settings{
		URL:      srv.ClientURL(),
		User:     "test",
		Password: "s3cret",
	}

	j, _ := json.Marshal(nctx)
	tf, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("creating temp file failed: %v", err)
	}
	defer os.Remove(tf.Name())

	_, err = tf.Write(j)
	if err != nil {
		t.Fatalf("writing temp file failed: %v", err)
	}
	tf.Close()

	nc, _, err := natscontext.Connect(tf.Name(), nats.Name("unit tests"))
	if err != nil {
		t.Fatalf("connecting to nats server failed: %v", err)
	}

	resp, err := nc.Request("$SYS.REQ.SERVER.PING.CONNZ", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("user info failed: %v", err)
	}

	type connz struct {
		Data struct {
			Connections server.ConnInfos `json:"connections"`
		} `json:"data"`
	}

	var nfo connz
	err = json.Unmarshal(resp.Data, &nfo)
	if err != nil {
		t.Fatalf("invalid user info: %v", err)
	}

	if len(nfo.Data.Connections) != 1 {
		t.Fatalf("invalid number of connections: %d", len(nfo.Data.Connections))
	}

	if nfo.Data.Connections[0].Name != "unit tests" {
		t.Fatalf("invalid connection name: %q", nfo.Data.Connections[0].Name)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Running nats server in separate Go routines
////////////////////////////////////////////////////////////////////////////////

// RunDefaultServer will run a server on the default port.
func RunDefaultServer() *server.Server {
	return RunServerOnPort(nats.DefaultPort)
}

// RunServerOnPort will run a server on the given port.
func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	return RunServerWithOptions(&opts)
}

// RunServerWithOptions will run a server with the given options.
func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

// RunServerWithConfig will run a server with the given configuration file.
func RunServerWithConfig(configFile string) (*server.Server, *server.Options) {
	return natsserver.RunServerWithConfig(configFile)
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(&opts)
}
