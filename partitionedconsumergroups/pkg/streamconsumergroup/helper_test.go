package streamconsumergroup

import (
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"os"
	"testing"
)

//func require_True(t testing.TB, b bool) {
//	t.Helper()
//	if !b {
//		t.Fatalf("require true, but got false")
//	}
//}

//func require_False(t testing.TB, b bool) {
//	t.Helper()
//	if b {
//		t.Fatalf("require false, but got true")
//	}
//}

func require_NoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("require no error, but got: %v", err)
	}
}

//func require_NotNil(t testing.TB, v any) {
//	t.Helper()
//	if v == nil {
//		t.Fatalf("require not nil, but got: %v", v)
//	}
//}

//func require_Contains(t *testing.T, s string, subStrs ...string) {
//	t.Helper()
//	for _, subStr := range subStrs {
//		if !strings.Contains(s, subStr) {
//			t.Fatalf("require %q to be contained in %q", subStr, s)
//		}
//	}
//}

//func require_Error(t testing.TB, err error, expected ...error) {
//	t.Helper()
//	if err == nil {
//		t.Fatalf("require error, but got none")
//	}
//	if len(expected) == 0 {
//		return
//	}
//	// Try to strip nats prefix from Go library if present.
//	const natsErrPre = "nats: "
//	eStr := err.Error()
//	if strings.HasPrefix(eStr, natsErrPre) {
//		eStr = strings.Replace(eStr, natsErrPre, "", 1)
//	}
//
//	for _, e := range expected {
//		if err == e || strings.Contains(eStr, e.Error()) || strings.Contains(e.Error(), eStr) {
//			return
//		}
//	}
//	t.Fatalf("Expected one of %v, got '%v'", expected, err)
//}

func require_Equal[T comparable](t testing.TB, a, b T) {
	t.Helper()
	if a != b {
		t.Fatalf("require %T equal, but got: %v != %v", a, a, b)
	}
}

//func require_NotEqual[T comparable](t testing.TB, a, b T) {
//	t.Helper()
//	if a == b {
//		t.Fatalf("require %T not equal, but got: %v == %v", a, a, b)
//	}
//}

//func require_Len(t testing.TB, a, b int) {
//	t.Helper()
//	if a != b {
//		t.Fatalf("require len, but got: %v != %v", a, b)
//	}
//}

//func require_LessThan[T cmp.Ordered](t *testing.T, a, b T) {
//	t.Helper()
//	if a >= b {
//		t.Fatalf("require %v to be less than %v", a, b)
//	}
//}

//func require_ChanRead[T any](t *testing.T, ch chan T, timeout time.Duration) T {
//	t.Helper()
//	select {
//	case v := <-ch:
//		return v
//	case <-time.After(timeout):
//		t.Fatalf("require read from channel within %v but didn't get anything", timeout)
//	}
//	panic("this shouldn't be possible")
//}

//func require_NoChanRead[T any](t *testing.T, ch chan T, timeout time.Duration) {
//	t.Helper()
//	select {
//	case <-ch:
//		t.Fatalf("require no read from channel within %v but got something", timeout)
//	case <-time.After(timeout):
//	}
//}

// RunServerWithOptions will run a server with the given options.
func RunServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(opts)
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}
