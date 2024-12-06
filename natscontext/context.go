// Copyright 2022-2024 The NATS Authors
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

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"
)

type context struct {
	Name   string `json:"-"`
	config *Settings
	path   string
}

type nscCreds struct {
	UserCreds string `json:"user_creds"`
	Operator  struct {
		Service []string `json:"service"`
	} `json:"operator"`
}

type Settings struct {
	Name                string `json:"name,omitempty"`
	Description         string `json:"description"`
	URL                 string `json:"url"`
	nscUrl              string
	SocksProxy          string `json:"socks_proxy"`
	Token               string `json:"token"`
	User                string `json:"user"`
	Password            string `json:"password"`
	Creds               string `json:"creds"`
	nscCreds            string
	NKey                string   `json:"nkey"`
	Cert                string   `json:"cert"`
	Key                 string   `json:"key"`
	CA                  string   `json:"ca"`
	NSCLookup           string   `json:"nsc"`
	JSDomain            string   `json:"jetstream_domain"`
	JSAPIPrefix         string   `json:"jetstream_api_prefix"`
	JSEventPrefix       string   `json:"jetstream_event_prefix"`
	InboxPrefix         string   `json:"inbox_prefix"`
	UserJwt             string   `json:"user_jwt"`
	ColorScheme         string   `json:"color_scheme"`
	TLSFirst            bool     `json:"tls_first"`
	WinCertStoreType    string   `json:"windows_cert_store"`
	WinCertStoreMatchBy string   `json:"windows_cert_match_by"`
	WinCertStoreMatch   string   `json:"windows_cert_match"`
	WinCertStoreCaMatch []string `json:"windows_ca_certs_match"`
}

// Connect connects to the NATS server configured by the named context, empty name connects to selected context.
// If the name is a absolute name to a context configuration that will be used. The settings that are returned
// will include all the values from the Context so values like the JetStream Domain etc can be used later
func Connect(name string, opts ...nats.Option) (*nats.Conn, Settings, error) {
	var nctx *context
	var err error

	absName, _ := filepath.Abs(name)
	if name != "" && absName == name {
		nctx, err = newFromFile(name)
	} else {
		nctx, err = newFromName(name)
	}
	if err != nil {
		return nil, Settings{}, err
	}

	nc, err := nctx.connect(opts...)
	if err != nil {
		return nil, Settings{}, err
	}

	return nc, *nctx.config, nil
}

func (c *context) connect(opts ...nats.Option) (*nats.Conn, error) {
	nopts, err := c.natsOptions(opts...)
	if err != nil {
		return nil, err
	}

	return nats.Connect(c.config.URL, nopts...)
}

func (c *context) natsOptions(opts ...nats.Option) ([]nats.Option, error) {
	var nopts []nats.Option

	switch {
	case c.config.User != "":
		nopts = append(nopts, nats.UserInfo(c.config.User, c.config.Password))
	case c.config.Creds != "":
		nopts = append(nopts, nats.UserCredentials(expandHomedir(c.config.Creds)))

	case c.config.NKey != "":
		nko, err := nats.NkeyOptionFromSeed(expandHomedir(c.config.NKey))
		if err != nil {
			return nil, err
		}

		nopts = append(nopts, nko)
	}

	if c.config.Token != "" {
		nopts = append(nopts, nats.Token(expandHomedir(c.config.Token)))
	}

	if c.config.Cert != "" && c.config.Key != "" {
		nopts = append(nopts, nats.ClientCert(expandHomedir(c.config.Cert), expandHomedir(c.config.Key)))
	}

	if c.config.CA != "" {
		nopts = append(nopts, nats.RootCAs(expandHomedir(c.config.CA)))
	}

	if c.config.SocksProxy != "" {
		nopts = append(nopts, nats.SetCustomDialer(c.SOCKSDialer()))
	}

	if c.config.InboxPrefix != "" {
		nopts = append(nopts, nats.CustomInboxPrefix(c.config.InboxPrefix))
	}

	if c.config.TLSFirst {
		nopts = append(nopts, nats.TLSHandshakeFirst())
	}

	if c.config.WinCertStoreType != "" {
		return nil, fmt.Errorf("windows cert stores are not supported")
	}

	nopts = append(nopts, opts...)

	return nopts, nil
}

func expandHomedir(path string) string {
	if path[0] != '~' {
		return path
	}

	usr, err := user.Current()
	if err != nil {
		return path
	}

	return strings.Replace(path, "~", usr.HomeDir, 1)
}

func newFromName(name string) (*context, error) {
	c := &context{
		Name:   name,
		config: &Settings{},
	}

	err := c.loadActiveContext()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func newFromFile(filename string) (*context, error) {
	c := &context{
		Name:   strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename)),
		config: &Settings{},
		path:   filename,
	}

	err := c.loadActiveContext()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func readCtxFromFile(file string) string {
	parent, err := parentDir()
	if err != nil {
		return ""
	}

	currentFile := filepath.Join(parent, "nats", file)

	_, err = os.Stat(currentFile)
	if os.IsNotExist(err) {
		return ""
	}

	fc, err := os.ReadFile(currentFile)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(fc))
}

func selectedContext() string {
	return readCtxFromFile("context.txt")
}

func validName(name string) bool {
	return name != "" && !strings.Contains(name, "..") && !strings.Contains(name, string(os.PathSeparator))
}

func ctxDir(parent string) string {
	return filepath.Join(parent, "nats", "context")
}

func knownContext(parent string, name string) bool {
	if !validName(name) {
		return false
	}

	_, err := os.Stat(filepath.Join(ctxDir(parent), name+".json"))
	return !os.IsNotExist(err)
}

func (c *context) loadActiveContext() error {
	if c.path == "" {
		parent, err := parentDir()
		if err != nil {
			return err
		}

		// none given, lets try to find it via the fs
		if c.Name == "" {
			c.Name = selectedContext()
			if c.Name == "" {
				return nil
			}
		}

		if !validName(c.Name) {
			return fmt.Errorf("invalid context name %s", c.Name)
		}

		if !knownContext(parent, c.Name) {
			return fmt.Errorf("unknown context %q", c.Name)
		}

		c.path = filepath.Join(parent, "nats", "context", c.Name+".json")
	}

	ctxContent, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(ctxContent, c.config)
	if err != nil {
		return err
	}

	// performing environment variable expansion for the path of the cerds.
	c.config.Creds = os.ExpandEnv(c.config.Creds)

	if c.config.NSCLookup != "" {
		err := c.resolveNscLookup()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *context) resolveNscLookup() error {
	if c.config.NSCLookup == "" {
		return nil
	}

	path, err := exec.LookPath("nsc")
	if err != nil {
		return fmt.Errorf("cannot find 'nsc' in user path")
	}

	cmd := exec.Command(path, "generate", "profile", c.config.NSCLookup)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nsc invoke failed: %s", string(out))
	}

	var result nscCreds
	err = json.Unmarshal(out, &result)
	if err != nil {
		return fmt.Errorf("could not parse nsc output: %s", err)
	}

	if result.UserCreds != "" {
		c.config.nscCreds = result.UserCreds
	}

	if len(result.Operator.Service) > 0 {
		c.config.nscUrl = strings.Join(result.Operator.Service, ",")
	}

	return nil
}

func parentDir() (string, error) {
	parent := os.Getenv("XDG_CONFIG_HOME")
	if parent != "" {
		return parent, nil
	}

	u, err := user.Current()
	if err != nil {
		return "", err
	}

	if u.HomeDir == "" {
		return "", fmt.Errorf("cannot determine home directory")
	}

	return filepath.Join(u.HomeDir, parent, ".config"), nil
}
