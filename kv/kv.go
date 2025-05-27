// Copyright 2025 Synadia Communications Inc.
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

package kv

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type (
	// KeyValue contains methods to operate on a KeyValue store.
	// Using the KeyValue interface, it is possible to:
	//
	// - Get, Put, Create, Update, Delete and Purge a key
	// - Watch for updates to keys
	// - List all keys
	// - Retrieve historical values for a key
	// - Retrieve status and configuration of a key value bucket
	// - Purge all delete markers
	KeyValue interface {
		// Get returns the latest value for the key. If the key does not exist,
		// ErrKeyNotFound will be returned.
		Get(ctx context.Context, key string) (*KeyValueEntry, error)

		// GetRevision returns a specific revision value for the key. If the key
		// does not exist or the provided revision does not exists,
		// ErrKeyNotFound will be returned.
		GetRevision(ctx context.Context, key string, revision uint64) (*KeyValueEntry, error)

		// Put will place the new value for the key into the store. If the key
		// does not exist, it will be created. If the key exists, the value will
		// be updated.
		//
		// A key has to consist of alphanumeric characters, dashes, underscores,
		// equal signs, and dots.
		Put(ctx context.Context, key string, value []byte) (uint64, error)

		// PutString will place the string for the key into the store. If the
		// key does not exist, it will be created. If the key exists, the value
		// will be updated.
		//
		// A key has to consist of alphanumeric characters, dashes, underscores,
		// equal signs, and dots.
		PutString(ctx context.Context, key, value string) (uint64, error)

		// Create will add the key/value pair if it does not exist. If the key
		// already exists, ErrKeyExists will be returned.
		//
		// A key has to consist of alphanumeric characters, dashes, underscores,
		// equal signs, and dots.
		Create(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (uint64, error)

		// Update will update the value if the latest revision matches.
		// If the provided revision is not the latest, Update will return an error.
		// Update also resets the TTL associated with the key (if any).
		Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)

		// Delete will place a delete marker and leave all revisions. A history
		// of a deleted key can still be retrieved by using the History method
		// or a watch on the key. [Delete] is a non-destructive operation and
		// will not remove any previous revisions from the underlying stream.
		//
		// [LastRevision] option can be specified to only perform delete if the
		// latest revision the provided one.
		Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error

		// Purge will place a delete marker and remove all previous revisions.
		// Only the latest revision will be preserved (with a delete marker).
		// Unlike [Delete], Purge is a destructive operation and will remove all
		// previous revisions from the underlying streams.
		//
		// [LastRevision] option can be specified to only perform purge if the
		// latest revision the provided one.
		Purge(ctx context.Context, key string, opts ...KVPurgeOpt) error

		// Watch for any updates to keys that match the keys argument which
		// could include wildcards. By default, the watcher will send the latest
		// value for each key and all future updates. Watch will send a nil
		// entry when it has received all initial values. There are a few ways
		// to configure the watcher:
		//
		// - IncludeHistory will have the key watcher send all historical values
		// for each key (up to KeyValueMaxHistory).
		// - IgnoreDeletes will have the key watcher not pass any keys with
		// delete markers.
		// - UpdatesOnly will have the key watcher only pass updates on values
		// (without latest values when started).
		// - MetaOnly will have the key watcher retrieve only the entry meta
		// data, not the entry value.
		// - ResumeFromRevision instructs the key watcher to resume from a
		// specific revision number.
		Watch(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error)

		// WatchAll will watch for any updates to all keys. It can be configured
		// with the same options as Watch.
		WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error)

		// ListKeys returns an iterator for filtered keys in the bucket.
		ListKeys(ctx context.Context, filters ...string) (iter.Seq2[string, error], error)

		// History will return all historical values for the key (up to
		// KeyValueMaxHistory).
		History(ctx context.Context, key string) ([]KeyValueEntry, error)

		// Bucket returns the KV store name.
		Bucket() string

		// PurgeDeletes will remove all current delete markers. It can be
		// configured using DeleteMarkersOlderThan option to only remove delete
		// markers older than a certain duration.
		//
		// PurgeDeletes is a destructive operation and will remove all entries
		// with delete markers from the underlying stream.
		PurgeDeletes(ctx context.Context, opts ...KVPurgeDeletesOpt) error

		// Status retrieves the status and configuration of a bucket.
		Status(ctx context.Context) (KeyValueStatus, error)
	}

	// KeyValueConfig is the configuration for a KeyValue store.
	KeyValueConfig struct {
		// Bucket is the name of the KeyValue store. Bucket name has to be
		// unique and can only contain alphanumeric characters, dashes, and
		// underscores.
		Bucket string `json:"bucket"`

		// Description is an optional description for the KeyValue store.
		Description string `json:"description,omitempty"`

		// MaxValueSize is the maximum size of a value in bytes. If not
		// specified, the default is -1 (unlimited).
		MaxValueSize int32 `json:"max_value_size,omitempty"`

		// History is the number of historical values to keep per key. If not
		// specified, the default is 1. Max is 64.
		History uint8 `json:"history,omitempty"`

		// TTL is the expiry time for keys. By default, keys do not expire.
		TTL time.Duration `json:"ttl,omitempty"`

		// MaxBytes is the maximum size in bytes of the KeyValue store. If not
		// specified, the default is -1 (unlimited).
		MaxBytes int64 `json:"max_bytes,omitempty"`

		// Storage is the type of storage to use for the KeyValue store. If not
		// specified, the default is FileStorage.
		Storage jetstream.StorageType `json:"storage,omitempty"`

		// Replicas is the number of replicas to keep for the KeyValue store in
		// clustered jetstream. Defaults to 1, maximum is 5.
		Replicas int `json:"num_replicas,omitempty"`

		// Placement is used to declare where the stream should be placed via
		// tags and/or an explicit cluster name.
		Placement *jetstream.Placement `json:"placement,omitempty"`

		// RePublish allows immediate republishing a message to the configured
		// subject after it's stored.
		RePublish *jetstream.RePublish `json:"republish,omitempty"`

		// Mirror defines the consiguration for mirroring another KeyValue
		// store.
		Mirror *jetstream.StreamSource `json:"mirror,omitempty"`

		// Sources defines the configuration for sources of a KeyValue store.
		Sources []*jetstream.StreamSource `json:"sources,omitempty"`

		// Compression sets the underlying stream compression.
		// NOTE: Compression is supported for nats-server 2.10.0+
		Compression bool `json:"compression,omitempty"`

		// LimitMarkerTTL is how long the bucket keeps markers when keys are
		// removed by the TTL setting.
		LimitMarkerTTL time.Duration
	}

	// KeyValueStatus is run-time status about a Key-Value bucket.
	KeyValueStatus struct {
		// Bucket returns the name of the KeyValue store.
		Bucket string

		// Values is how many messages are in the bucket, including historical values.
		Values uint64

		// History returns the configured history kept per key.
		History int64

		// TTL returns the duration for which keys are kept in the bucket.
		TTL time.Duration

		// BackingStore indicates what technology is used for storage of the bucket.
		// Currently only JetStream is supported.
		BackingStore string

		// Bytes returns the size of the bucket in bytes.
		Bytes uint64

		// IsCompressed indicates if the data is compressed on disk.
		IsCompressed bool

		// LimitMarkerTTL is how long the bucket keeps markers when keys are
		// removed by the TTL setting, 0 meaning markers are not supported.
		LimitMarkerTTL time.Duration

		// StreamInfo is the underlying stream information.
		StreamInfo *jetstream.StreamInfo
	}

	// KeyWatcher is what is returned when doing a watch. It can be used to
	// retrieve updates to keys. If not using UpdatesOnly option, it will also
	// send the latest value for each key. After all initial values have been
	// sent, a nil entry will be sent. Stop can be used to stop the watcher and
	// close the underlying channel. Watcher will not close the channel until
	// Stop is called or connection is closed.
	KeyWatcher interface {
		Updates() <-chan *KeyValueEntry
		Stop() error
	}

	// KeyValueEntry is a retrieved entry for Get, List or Watch.
	KeyValueEntry struct {
		// Bucket is the bucket the data was loaded from.
		Bucket string

		// Key is the name of the key that was retrieved.
		Key string

		// Value is the retrieved value.
		Value []byte

		// Revision is a unique sequence for this value.
		Revision uint64

		// Created is the time the data was put in the bucket.
		Created time.Time

		// Delta is distance from the latest value (how far the current sequence
		// is from the latest).
		Delta uint64

		// Operation returns Put or Delete or Purge, depending on the manner in
		// which the current revision was created.
		Operation KeyValueOp
	}
)

type (
	WatchOpt func(opts *watchOpts) error

	watchOpts struct {
		// Do not send delete markers to the update channel.
		ignoreDeletes bool
		// Include all history per subject, not just last one.
		includeHistory bool
		// Include only updates for keys.
		updatesOnly bool
		// retrieve only the meta data of the entry
		metaOnly bool
		// resumeFromRevision is the revision to resume from.
		resumeFromRevision uint64
	}

	// KVDeleteOpt is used to configure delete operation.
	KVDeleteOpt func(opts *deleteOpts) error

	// KVPurgeOpt is used to configure purge operation.

	deleteOpts struct {
		// Delete only if the latest revision matches.
		revision uint64
	}

	KVPurgeOpt func(opts *purgeOpts) error

	purgeOpts struct {
		// Delete only if the latest revision matches.
		revision uint64

		// purge ttl
		ttl time.Duration
	}

	// KVCreateOpt is used to configure Create.
	KVCreateOpt func(opts *createOpts) error

	createOpts struct {
		ttl time.Duration // TTL for the key
	}

	// KVPurgeDeletesOpt is used to configure PurgeDeletes.
	KVPurgeDeletesOpt func(opts *purgeDeletesOpts) error

	purgeDeletesOpts struct {
		dmthr time.Duration // Delete markers threshold
	}
)

// kvs is the implementation of KeyValue
type kvs struct {
	name       string
	streamName string
	pre        string
	putPre     string
	pushJS     nats.JetStreamContext
	js         jetstream.JetStream
	stream     jetstream.Stream
	// If true, it means that APIPrefix/Domain was set in the context
	// and we need to add something to some of our high level protocols
	// (such as Put, etc..)
	useJSPfx bool
	// To know if we can use the stream direct get API
	useDirect bool
}

// KeyValueOp represents the type of KV operation (Put, Delete, Purge). It is a
// part of KeyValueEntry.
type KeyValueOp uint8

// Available KeyValueOp values.
const (
	// KeyValuePut is a set on a revision which creates or updates a value for a
	// key.
	KeyValuePut KeyValueOp = iota

	// KeyValueDelete is a set on a revision which adds a delete marker for a
	// key.
	KeyValueDelete

	// KeyValuePurge is a set on a revision which removes all previous revisions
	// for a key.
	KeyValuePurge
)

func (op KeyValueOp) String() string {
	switch op {
	case KeyValuePut:
		return "KeyValuePutOp"
	case KeyValueDelete:
		return "KeyValueDeleteOp"
	case KeyValuePurge:
		return "KeyValuePurgeOp"
	default:
		return "Unknown Operation"
	}
}

const (
	kvBucketNamePre         = "KV_"
	kvBucketNameTmpl        = "KV_%s"
	kvSubjectsTmpl          = "$KV.%s.>"
	kvSubjectsPreTmpl       = "$KV.%s."
	kvSubjectsPreDomainTmpl = "%s.$KV.%s."
	kvNoPending             = "0"
)

const (
	KeyValueMaxHistory = 64
	AllKeys            = ">"
	kvLatestRevision   = 0
	kvop               = "KV-Operation"
	kvdel              = "DEL"
	kvpurge            = "PURGE"
)

// Regex for valid keys and buckets.
var (
	validBucketRe    = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	validKeyRe       = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)
	validSearchKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9*]*[>]?$`)
)

func GetKeyValue(ctx context.Context, js jetstream.JetStream, bucket string) (KeyValue, error) {
	if !bucketValid(bucket) {
		return nil, ErrInvalidBucketName
	}
	streamName := fmt.Sprintf(kvBucketNameTmpl, bucket)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			err = ErrBucketNotFound
		}
		return nil, err
	}
	// Do some quick sanity checks that this is a correctly formed stream for KV.
	// Max msgs per subject should be > 0.
	if stream.CachedInfo().Config.MaxMsgsPerSubject < 1 {
		return nil, ErrBadBucket
	}
	pushJS, err := legacyJetStream(js)
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

func CreateKeyValue(ctx context.Context, js jetstream.JetStream, cfg KeyValueConfig) (KeyValue, error) {
	scfg, err := prepareKeyValueConfig(ctx, js, cfg)
	if err != nil {
		return nil, err
	}

	stream, err := js.CreateStream(ctx, scfg)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			// errors are joined so that backwards compatibility is retained
			// and previous checks for ErrStreamNameAlreadyInUse will still work.
			err = errors.Join(fmt.Errorf("%w: %s", ErrBucketExists, cfg.Bucket), err)

			// If we have a failure to add, it could be because we have
			// a config change if the KV was created against before a bug fix
			// that changed the value of discard policy.
			// We will check if the stream exists and if the only difference
			// is the discard policy, we will update the stream.
			// The same logic applies for KVs created pre 2.9.x and
			// the AllowDirect setting.
			if stream, _ = js.Stream(ctx, scfg.Name); stream != nil {
				cfg := stream.CachedInfo().Config
				cfg.Discard = scfg.Discard
				cfg.AllowDirect = scfg.AllowDirect
				if reflect.DeepEqual(cfg, scfg) {
					stream, err = js.UpdateStream(ctx, scfg)
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	pushJS, err := legacyJetStream(js)
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

func UpdateKeyValue(ctx context.Context, js jetstream.JetStream, cfg KeyValueConfig) (KeyValue, error) {
	scfg, err := prepareKeyValueConfig(ctx, js, cfg)
	if err != nil {
		return nil, err
	}

	stream, err := js.UpdateStream(ctx, scfg)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			err = fmt.Errorf("%w: %s", ErrBucketNotFound, cfg.Bucket)
		}
		return nil, err
	}
	pushJS, err := legacyJetStream(js)
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

func CreateOrUpdateKeyValue(ctx context.Context, js jetstream.JetStream, cfg KeyValueConfig) (KeyValue, error) {
	scfg, err := prepareKeyValueConfig(ctx, js, cfg)
	if err != nil {
		return nil, err
	}

	stream, err := js.CreateOrUpdateStream(ctx, scfg)
	if err != nil {
		return nil, err
	}
	pushJS, err := legacyJetStream(js)
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

func prepareKeyValueConfig(ctx context.Context, js jetstream.JetStream, cfg KeyValueConfig) (jetstream.StreamConfig, error) {
	if !bucketValid(cfg.Bucket) {
		return jetstream.StreamConfig{}, ErrInvalidBucketName
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		return jetstream.StreamConfig{}, err
	}

	// Default to 1 for history. Max is 64 for now.
	history := int64(1)
	if cfg.History > 0 {
		if cfg.History > KeyValueMaxHistory {
			return jetstream.StreamConfig{}, ErrHistoryTooLarge
		}
		history = int64(cfg.History)
	}

	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// We will set explicitly some values so that we can do comparison
	// if we get an "already in use" error and need to check if it is same.
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}
	maxMsgSize := cfg.MaxValueSize
	if maxMsgSize == 0 {
		maxMsgSize = -1
	}
	// When stream's MaxAge is not set, server uses 2 minutes as the default
	// for the duplicate window. If MaxAge is set, and lower than 2 minutes,
	// then the duplicate window will be set to that. If MaxAge is greater,
	// we will cap the duplicate window to 2 minutes (to be consistent with
	// previous behavior).
	duplicateWindow := 2 * time.Minute
	if cfg.TTL > 0 && cfg.TTL < duplicateWindow {
		duplicateWindow = cfg.TTL
	}
	var compression jetstream.StoreCompression
	if cfg.Compression {
		compression = jetstream.S2Compression
	}
	var allowMsgTTL bool
	var subjectDeleteMarkerTTL time.Duration
	if cfg.LimitMarkerTTL != 0 {
		info, err := js.AccountInfo(ctx)
		if err != nil {
			return jetstream.StreamConfig{}, err
		}
		if info.API.Level < 1 {
			return jetstream.StreamConfig{}, ErrLimitMarkerTTLNotSupported
		}
		allowMsgTTL = true
		subjectDeleteMarkerTTL = cfg.LimitMarkerTTL
	}
	scfg := jetstream.StreamConfig{
		Name:                   fmt.Sprintf(kvBucketNameTmpl, cfg.Bucket),
		Description:            cfg.Description,
		MaxMsgsPerSubject:      history,
		MaxBytes:               maxBytes,
		MaxAge:                 cfg.TTL,
		MaxMsgSize:             maxMsgSize,
		Storage:                cfg.Storage,
		Replicas:               replicas,
		Placement:              cfg.Placement,
		AllowRollup:            true,
		DenyDelete:             true,
		Duplicates:             duplicateWindow,
		MaxMsgs:                -1,
		MaxConsumers:           -1,
		AllowDirect:            true,
		RePublish:              cfg.RePublish,
		Compression:            compression,
		Discard:                jetstream.DiscardNew,
		AllowMsgTTL:            allowMsgTTL,
		SubjectDeleteMarkerTTL: subjectDeleteMarkerTTL,
	}
	if cfg.Mirror != nil {
		// Copy in case we need to make changes so we do not change caller's version.
		m := copyStreamSource(cfg.Mirror)
		if !strings.HasPrefix(m.Name, kvBucketNamePre) {
			m.Name = fmt.Sprintf(kvBucketNameTmpl, m.Name)
		}
		scfg.Mirror = m
		scfg.MirrorDirect = true
	} else if len(cfg.Sources) > 0 {
		// For now we do not allow direct subjects for sources. If that is desired a user could use stream API directly.
		for _, ss := range cfg.Sources {
			var sourceBucketName string
			if strings.HasPrefix(ss.Name, kvBucketNamePre) {
				sourceBucketName = ss.Name[len(kvBucketNamePre):]
			} else {
				sourceBucketName = ss.Name
				ss.Name = fmt.Sprintf(kvBucketNameTmpl, ss.Name)
			}

			if ss.External == nil || sourceBucketName != cfg.Bucket {
				ss.SubjectTransforms = []jetstream.SubjectTransformConfig{{Source: fmt.Sprintf(kvSubjectsTmpl, sourceBucketName), Destination: fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}}
			}
			scfg.Sources = append(scfg.Sources, ss)
		}
		scfg.Subjects = []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}
	} else {
		scfg.Subjects = []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}
	}

	return scfg, nil
}

// DeleteKeyValue will delete this KeyValue store (JetStream stream).
func DeleteKeyValue(ctx context.Context, js jetstream.JetStream, bucket string) error {
	if !bucketValid(bucket) {
		return ErrInvalidBucketName
	}
	stream := fmt.Sprintf(kvBucketNameTmpl, bucket)
	if err := js.DeleteStream(ctx, stream); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			err = errors.Join(fmt.Errorf("%w: %s", ErrBucketNotFound, bucket), err)
		}
		return err
	}
	return nil
}

// KeyValueStoreNames is used to retrieve a list of key value store names as an iterator
func KeyValueStoreNames(ctx context.Context, js jetstream.JetStream) iter.Seq2[string, error] {
	listSubj := fmt.Sprintf(kvSubjectsTmpl, "*")
	streams := js.StreamNames(ctx, jetstream.WithStreamListSubject(listSubj))

	return func(yield func(string, error) bool) {
		for name := range streams.Name() {
			if !strings.HasPrefix(name, kvBucketNamePre) {
				continue
			}
			if !yield(strings.TrimPrefix(name, kvBucketNamePre), nil) {
				return
			}
		}
		if err := streams.Err(); err != nil {
			yield("", err)
		}
	}
}

// KeyValueStores is used to retrieve a list of key value store statuses as an iterator
func KeyValueStores(ctx context.Context, js jetstream.JetStream) iter.Seq2[KeyValueStatus, error] {
	listSubj := fmt.Sprintf(kvSubjectsTmpl, "*")
	streams := js.ListStreams(ctx, jetstream.WithStreamListSubject(listSubj))

	return func(yield func(KeyValueStatus, error) bool) {
		for info := range streams.Info() {
			if !strings.HasPrefix(info.Config.Name, kvBucketNamePre) {
				continue
			}
			status := KeyValueStatus{
				Bucket:         strings.TrimPrefix(info.Config.Name, kvBucketNamePre),
				Values:         info.State.Msgs,
				History:        info.Config.MaxMsgsPerSubject,
				TTL:            info.Config.MaxAge,
				BackingStore:   "JetStream",
				Bytes:          info.State.Bytes,
				IsCompressed:   info.Config.Compression != jetstream.NoCompression,
				LimitMarkerTTL: info.Config.SubjectDeleteMarkerTTL,
				StreamInfo:     info,
			}
			if !yield(status, nil) {
				return
			}
		}
		if err := streams.Err(); err != nil {
			yield(KeyValueStatus{}, err)
		}
	}
}

func legacyJetStream(js jetstream.JetStream) (nats.JetStreamContext, error) {
	opts := make([]nats.JSOpt, 0)
	if js.Options().APIPrefix != "" {
		opts = append(opts, nats.APIPrefix(js.Options().APIPrefix))
	}
	return js.Conn().JetStream(opts...)
}

func bucketValid(bucket string) bool {
	if len(bucket) == 0 {
		return false
	}
	return validBucketRe.MatchString(bucket)
}

func keyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validKeyRe.MatchString(key)
}

func searchKeyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validSearchKeyRe.MatchString(key)
}

func (kv *kvs) get(ctx context.Context, key string, revision uint64) (*KeyValueEntry, error) {
	if !keyValid(key) {
		return nil, ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	var m *jetstream.RawStreamMsg
	var err error

	if revision == kvLatestRevision {
		m, err = kv.stream.GetLastMsgForSubject(ctx, b.String())
	} else {
		m, err = kv.stream.GetMsg(ctx, revision)
		// If a sequence was provided, just make sure that the retrieved
		// message subject matches the request.
		if err == nil && m.Subject != b.String() {
			return nil, ErrKeyNotFound
		}
	}
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			err = ErrKeyNotFound
		}
		return nil, err
	}

	entry := &KeyValueEntry{
		Bucket:   kv.name,
		Key:      key,
		Value:    m.Data,
		Revision: m.Sequence,
		Created:  m.Time,
	}

	// Double check here that this is not a DEL Operation marker.
	if len(m.Header) > 0 {
		if m.Header.Get(kvop) != "" {
			switch m.Header.Get(kvop) {
			case kvdel:
				entry.Operation = KeyValueDelete
			case kvpurge:
				entry.Operation = KeyValuePurge
			}
		} else if m.Header.Get(jetstream.MarkerReasonHeader) != "" {
			switch m.Header.Get(jetstream.MarkerReasonHeader) {
			case "MaxAge", "Purge":
				entry.Operation = KeyValuePurge
			case "Remove":
				entry.Operation = KeyValueDelete
			}
		}
		if entry.Operation != KeyValuePut {
			return entry, ErrKeyDeleted
		}
	}

	return entry, nil
}

// Get returns the latest value for the key.
func (kv *kvs) Get(ctx context.Context, key string) (*KeyValueEntry, error) {
	e, err := kv.get(ctx, key, kvLatestRevision)
	if err != nil {
		if errors.Is(err, ErrKeyDeleted) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return e, nil
}

// GetRevision returns a specific revision value for the key.
func (kv *kvs) GetRevision(ctx context.Context, key string, revision uint64) (*KeyValueEntry, error) {
	e, err := kv.get(ctx, key, revision)
	if err != nil {
		if errors.Is(err, ErrKeyDeleted) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return e, nil
}

// Put will place the new value for the key into the store.
func (kv *kvs) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	subj := kv.buildKeySubject(key)
	pa, err := kv.js.Publish(ctx, subj, value)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// PutString will place the string for the key into the store.
func (kv *kvs) PutString(ctx context.Context, key string, value string) (uint64, error) {
	return kv.Put(ctx, key, []byte(value))
}

// Create will add the key/value pair if it does not exist.
func (kv *kvs) Create(ctx context.Context, key string, value []byte, opts ...KVCreateOpt) (revision uint64, err error) {
	var o createOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return 0, err
			}
		}
	}

	v, err := kv.updateRevision(ctx, key, value, 0, o.ttl)
	if err == nil {
		return v, nil
	}

	if e, err := kv.get(ctx, key, kvLatestRevision); errors.Is(err, ErrKeyDeleted) {
		return kv.updateRevision(ctx, key, value, e.Revision, o.ttl)
	}

	// Check if the expected last subject sequence is not zero which implies
	// the key already exists.
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		if aerr := jsErr.APIError(); aerr != nil && int(aerr.ErrorCode) == int(jetstream.JSErrCodeStreamWrongLastSequence) {
			return 0, ErrKeyExists
		}
	}

	return 0, err
}

// Update will update the value if the latest revision matches.
func (kv *kvs) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return kv.updateRevision(ctx, key, value, revision, 0)
}

func (kv *kvs) updateRevision(ctx context.Context, key string, value []byte, revision uint64, ttl time.Duration) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	subj := kv.buildKeySubject(key)

	opts := []jetstream.PublishOpt{
		jetstream.WithExpectLastSequencePerSubject(revision),
	}
	if ttl > 0 {
		opts = append(opts, jetstream.WithMsgTTL(ttl))
	}

	pa, err := kv.js.Publish(ctx, subj, value, opts...)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Delete will place a delete marker and leave all revisions.
func (kv *kvs) Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error {
	if !keyValid(key) {
		return ErrInvalidKey
	}

	subj := kv.buildKeySubject(key)

	// DEL op marker. For watch functionality.
	m := nats.NewMsg(subj)

	var o deleteOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return err
			}
		}
	}

	m.Header.Set(kvop, kvdel)
	pubOpts := make([]jetstream.PublishOpt, 0)

	if o.revision != 0 {
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(o.revision, 10))
	}

	_, err := kv.js.PublishMsg(ctx, m, pubOpts...)
	return err
}

// Purge will place a delete marker and remove all previous revisions.
func (kv *kvs) Purge(ctx context.Context, key string, opts ...KVPurgeOpt) error {
	if !keyValid(key) {
		return ErrInvalidKey
	}

	subj := kv.buildKeySubject(key)

	// PURGE op marker. For watch functionality.
	m := nats.NewMsg(subj)

	var o purgeOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return err
			}
		}
	}

	m.Header.Set(kvop, kvpurge)
	m.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	pubOpts := make([]jetstream.PublishOpt, 0)
	if o.ttl > 0 {
		pubOpts = append(pubOpts, jetstream.WithMsgTTL(o.ttl))
	}

	if o.revision != 0 {
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(o.revision, 10))
	}

	_, err := kv.js.PublishMsg(ctx, m, pubOpts...)
	return err
}

// Implementation for Watch
type watcher struct {
	mu          sync.Mutex
	updates     chan *KeyValueEntry
	sub         *nats.Subscription
	initDone    bool
	initPending uint64
	received    uint64
}

// Updates returns the interior channel.
func (w *watcher) Updates() <-chan *KeyValueEntry {
	if w == nil {
		return nil
	}
	return w.updates
}

// Stop will unsubscribe from the watcher.
func (w *watcher) Stop() error {
	if w == nil {
		return nil
	}
	return w.sub.Unsubscribe()
}

func (kv *kvs) Watch(ctx context.Context, keys []string, opts ...WatchOpt) (KeyWatcher, error) {
	for _, key := range keys {
		if !searchKeyValid(key) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidKey, "key cannot be empty and must be a valid NATS subject")
		}
	}
	var o watchOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}

	// Could be a pattern so don't check for validity as we normally do.
	for i, key := range keys {
		var b strings.Builder
		b.WriteString(kv.pre)
		b.WriteString(key)
		keys[i] = b.String()
	}

	// if no keys are provided, watch all keys
	if len(keys) == 0 {
		var b strings.Builder
		b.WriteString(kv.pre)
		b.WriteString(AllKeys)
		keys = []string{b.String()}
	}

	// We will block below on placing items on the chan. That is by design.
	w := &watcher{updates: make(chan *KeyValueEntry, 256)}

	update := func(m *nats.Msg) {
		tokens, err := GetMetadataFields(m.Reply)
		if err != nil {
			return
		}
		if len(m.Subject) <= len(kv.pre) {
			return
		}
		subj := m.Subject[len(kv.pre):]

		var op KeyValueOp
		if len(m.Header) > 0 {
			if m.Header.Get(kvop) != "" {
				switch m.Header.Get(kvop) {
				case kvdel:
					op = KeyValueDelete
				case kvpurge:
					op = KeyValuePurge
				}
			} else if m.Header.Get(jetstream.MarkerReasonHeader) != "" {
				switch m.Header.Get(jetstream.MarkerReasonHeader) {
				case "MaxAge", "Purge":
					op = KeyValuePurge
				case "Remove":
					op = KeyValueDelete
				}
			}
		}
		delta := ParseNum(tokens[AckNumPendingTokenPos])
		w.mu.Lock()
		defer w.mu.Unlock()
		if !o.ignoreDeletes || (op != KeyValueDelete && op != KeyValuePurge) {
			entry := &KeyValueEntry{
				Bucket:    kv.name,
				Key:       subj,
				Value:     m.Data,
				Revision:  ParseNum(tokens[AckStreamSeqTokenPos]),
				Created:   time.Unix(0, int64(ParseNum(tokens[AckTimestampSeqTokenPos]))),
				Delta:     delta,
				Operation: op,
			}
			w.updates <- entry
		}
		// Check if done and initial values.
		if !w.initDone {
			w.received++
			// We set this on the first trip through..
			if w.initPending == 0 {
				w.initPending = delta
			}
			if w.received > w.initPending || delta == 0 {
				w.initDone = true
				w.updates <- nil
			}
		}
	}

	// Used ordered consumer to deliver results.
	subOpts := []nats.SubOpt{nats.BindStream(kv.streamName), nats.OrderedConsumer()}
	if !o.includeHistory {
		subOpts = append(subOpts, nats.DeliverLastPerSubject())
	}
	if o.updatesOnly {
		subOpts = append(subOpts, nats.DeliverNew())
	}
	if o.metaOnly {
		subOpts = append(subOpts, nats.HeadersOnly())
	}
	if o.resumeFromRevision > 0 {
		subOpts = append(subOpts, nats.StartSequence(o.resumeFromRevision))
	}
	subOpts = append(subOpts, nats.Context(ctx))
	// Create the sub and rest of initialization under the lock.
	// We want to prevent the race between this code and the
	// update() callback.
	w.mu.Lock()
	defer w.mu.Unlock()
	var sub *nats.Subscription
	var err error
	if len(keys) == 1 {
		sub, err = kv.pushJS.Subscribe(keys[0], update, subOpts...)
	} else {
		subOpts = append(subOpts, nats.ConsumerFilterSubjects(keys...))
		sub, err = kv.pushJS.Subscribe("", update, subOpts...)
	}
	if err != nil {
		return nil, err
	}
	sub.SetClosedHandler(func(_ string) {
		close(w.updates)
	})
	// If there were no pending messages at the time of the creation
	// of the consumer, send the marker.
	// Skip if UpdatesOnly() is set, since there will never be updates initially.
	if !o.updatesOnly {
		initialPending, err := sub.InitialConsumerPending()
		if err == nil && initialPending == 0 {
			w.initDone = true
			w.updates <- nil
		}
	} else {
		// if UpdatesOnly was used, mark initialization as complete
		w.initDone = true
	}
	w.sub = sub
	return w, nil
}

// WatchAll will invoke the callback for all updates.
func (kv *kvs) WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error) {
	return kv.Watch(ctx, []string{AllKeys}, opts...)
}

// ListKeys will return all keys as an iter.Seq2[string, error] iterator.
func (kv *kvs) ListKeys(ctx context.Context, filters ...string) (iter.Seq2[string, error], error) {
	watcher, err := kv.Watch(ctx, filters, IgnoreDeletes(), MetaOnly())
	if err != nil {
		return nil, err
	}

	return func(yield func(string, error) bool) {
		defer watcher.Stop()

		for {
			select {
			case entry := <-watcher.Updates():
				if entry == nil { // Indicates all initial values are received
					return
				}
				if !yield(entry.Key, nil) {
					return
				}
			case <-ctx.Done():
				yield("", ctx.Err())
				return
			}
		}
	}, nil
}

// History will return all historical values for the key.
func (kv *kvs) History(ctx context.Context, key string) ([]KeyValueEntry, error) {
	watcher, err := kv.Watch(ctx, []string{key}, IncludeHistory())
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var entries []KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		entries = append(entries, *entry)
	}
	if len(entries) == 0 {
		return nil, ErrKeyNotFound
	}
	return entries, nil
}

// Bucket returns the current bucket name.
func (kv *kvs) Bucket() string {
	return kv.name
}

const kvDefaultPurgeDeletesMarkerThreshold = 30 * time.Minute

// PurgeDeletes will remove all current delete markers.
func (kv *kvs) PurgeDeletes(ctx context.Context, opts ...KVPurgeDeletesOpt) error {
	var o purgeDeletesOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return err
			}
		}
	}
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer watcher.Stop()

	var limit time.Time
	olderThan := o.dmthr
	// Negative value is used to instruct to always remove markers, regardless
	// of age. If set to 0 (or not set), use our default value.
	if olderThan == 0 {
		olderThan = kvDefaultPurgeDeletesMarkerThreshold
	}
	if olderThan > 0 {
		limit = time.Now().Add(-olderThan)
	}

	var deleteMarkers []KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		if op := entry.Operation; op == KeyValueDelete || op == KeyValuePurge {
			deleteMarkers = append(deleteMarkers, *entry)
		}
	}
	// Stop watcher here so as we purge we do not have the system continually updating numPending.
	watcher.Stop()

	var b strings.Builder
	// Do actual purges here.
	for _, entry := range deleteMarkers {
		b.WriteString(kv.pre)
		b.WriteString(entry.Key)
		purgeOpts := []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject(b.String())}
		if olderThan > 0 && entry.Created.After(limit) {
			purgeOpts = append(purgeOpts, jetstream.WithPurgeKeep(1))
		}
		if err := kv.stream.Purge(ctx, purgeOpts...); err != nil {
			return err
		}
		b.Reset()
	}
	return nil
}

// Status retrieves the status and configuration of a bucket
func (kv *kvs) Status(ctx context.Context) (KeyValueStatus, error) {
	info, err := kv.stream.Info(ctx)
	if err != nil {
		return KeyValueStatus{}, err
	}

	return KeyValueStatus{
		Bucket:         kv.name,
		Values:         info.State.Msgs,
		History:        info.Config.MaxMsgsPerSubject,
		TTL:            info.Config.MaxAge,
		BackingStore:   "JetStream",
		Bytes:          info.State.Bytes,
		IsCompressed:   info.Config.Compression != jetstream.NoCompression,
		LimitMarkerTTL: info.Config.SubjectDeleteMarkerTTL,
		StreamInfo:     info,
	}, nil
}

func mapStreamToKVS(js jetstream.JetStream, pushJS nats.JetStreamContext, stream jetstream.Stream) *kvs {
	info := stream.CachedInfo()
	bucket := strings.TrimPrefix(info.Config.Name, kvBucketNamePre)
	kv := &kvs{
		name:       bucket,
		streamName: info.Config.Name,
		pre:        fmt.Sprintf(kvSubjectsPreTmpl, bucket),
		js:         js,
		pushJS:     pushJS,
		stream:     stream,
		// Determine if we need to use the JS prefix in front of Put and Delete operations
		useJSPfx:  js.Options().APIPrefix != jetstream.DefaultAPIPrefix,
		useDirect: info.Config.AllowDirect,
	}

	// If we are mirroring, we will have mirror direct on, so just use the mirror name
	// and override use
	if m := info.Config.Mirror; m != nil {
		bucket := strings.TrimPrefix(m.Name, kvBucketNamePre)
		if m.External != nil && m.External.APIPrefix != "" {
			kv.useJSPfx = false
			kv.pre = fmt.Sprintf(kvSubjectsPreTmpl, bucket)
			kv.putPre = fmt.Sprintf(kvSubjectsPreDomainTmpl, m.External.APIPrefix, bucket)
		} else {
			kv.putPre = fmt.Sprintf(kvSubjectsPreTmpl, bucket)
		}
	}

	return kv
}

// Helper for copying when we do not want to change user's version.
func copyStreamSource(ss *jetstream.StreamSource) *jetstream.StreamSource {
	nss := *ss
	// Check pointers
	if ss.OptStartTime != nil {
		t := *ss.OptStartTime
		nss.OptStartTime = &t
	}
	if ss.External != nil {
		ext := *ss.External
		nss.External = &ext
	}
	return &nss
}

func (kv *kvs) buildKeySubject(key string) string {
	var b strings.Builder
	if kv.useJSPfx {
		b.WriteString(kv.js.Options().APIPrefix)
	}
	if kv.putPre != "" {
		b.WriteString(kv.putPre)
	} else {
		b.WriteString(kv.pre)
	}
	b.WriteString(key)
	return b.String()
}
