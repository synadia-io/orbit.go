// Copyright 2024-2025 Synadia Communications Inc.
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

package pcgroups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	kvElasticBucketName = "elastic-consumer-groups"
)

type ElasticConsumerGroupConsumerInstance struct {
	StreamName             string
	ConsumerGroupName      string
	MemberName             string
	Config                 *ElasticConsumerGroupConfig
	MessageHandlerCB       func(msg jetstream.Msg)
	consumerUserConfig     jetstream.ConsumerConfig // The user provided config
	consumer               jetstream.Consumer
	currentPinnedID        string
	consumerConsumeContext jetstream.ConsumeContext
	js                     jetstream.JetStream
	kv                     jetstream.KeyValue
	contextCancel          context.CancelFunc
	keyWatcher             jetstream.KeyWatcher
	doneChan               chan error
}

// ElasticConsumerGroupConfig is the configuration of an elastic consumer group
type ElasticConsumerGroupConfig struct {
	MaxMembers            uint            `json:"max_members"`                  // The maximum number of members the consumer group can have, i.e. the number of partitions
	Filter                string          `json:"filter"`                       // The filter, used to both filter the message and partition them, must include at least one "*" wildcard
	PartitioningWildcards []int           `json:"partitioning_wildcards"`       // The indexes of the wildcards in the filter that will be used for partitioning. For example, if the subject has the pattern `"foo.<key>", then the filter is "foo.*" and the partitioning wildcard is 1.
	MaxBufferedMsgs       int64           `json:"max_buffered_msg,omitempty"`   // The max number of messages buffered in the consumer group's stream
	MaxBufferedBytes      int64           `json:"max_buffered_bytes,omitempty"` // The max number of bytes buffered in the consumer group's stream
	Members               []string        `json:"members,omitempty"`            // The list of members in the consumer group
	MemberMappings        []MemberMapping `json:"member_mappings,omitempty"`    // Or the member mappings, which is a list of member names and the partitions that are assigned to them
}

// IsInMembership returns true if the member name is in the current membership of the elastic consumer group
func (config *ElasticConsumerGroupConfig) IsInMembership(name string) bool {
	// valid config has either members or member mappings
	return slices.ContainsFunc(config.MemberMappings, func(mapping MemberMapping) bool { return mapping.Member == name }) || slices.Contains(config.Members, name)
}

// GetElasticConsumerGroupConfig gets the consumer group's config from the KV bucket
func GetElasticConsumerGroupConfig(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string) (*ElasticConsumerGroupConfig, error) {
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	return getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
}

// ElasticConsume is the function that will start a go routine to consume messages from the stream (when active)
func ElasticConsume(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberName string, messageHandler func(msg jetstream.Msg), config jetstream.ConsumerConfig) (ConsumerGroupConsumeContext, error) {
	var err error

	if messageHandler == nil {
		return nil, errors.New("a message handler must be provided")
	}

	if config.AckPolicy != jetstream.AckExplicitPolicy {
		return nil, errors.New("the ack policy when consuming from elastic consumer groups must be explicit")
	}

	if config.InactiveThreshold == 0 {
		config.InactiveThreshold = consumerIdleTimeout
	}

	if config.AckWait < ackWait {
		config.AckWait = ackWait
	}

	instance := ElasticConsumerGroupConsumerInstance{
		StreamName:         streamName,
		ConsumerGroupName:  consumerGroupName,
		MemberName:         memberName,
		consumerUserConfig: config,
		MessageHandlerCB:   messageHandler,
	}

	instance.js = js

	_, err = instance.js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return nil, fmt.Errorf("the elastic consumer group's stream does not exist: %w", err)
	}

	instance.kv, err = instance.js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	// Try to get the current config if there's one
	instance.Config, err = getElasticConsumerGroupConfig(ctx, instance.kv, streamName, consumerGroupName)
	if err != nil {
		return nil, fmt.Errorf("can not get the current elastic consumer group's config: %w", err)
	}

	instanceRoutineContext := context.Background()
	instanceRoutineContext, instance.contextCancel = context.WithCancel(instanceRoutineContext)

	instance.keyWatcher, err = instance.kv.Watch(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		return nil, err
	}

	if instance.Config.IsInMembership(memberName) {
		instance.joinMemberConsumer()
	}

	instance.doneChan = make(chan error, 1)

	go instance.instanceRoutine(instanceRoutineContext)

	return &instance, nil
}

// Stop stops the consumer instance
func (instance *ElasticConsumerGroupConsumerInstance) Stop() {
	if instance.contextCancel != nil {
		instance.contextCancel()
	}
}

// Done returns the error (if any) when the consumer group instance is done or an unrecoverable error occurs
func (instance *ElasticConsumerGroupConsumerInstance) Done() <-chan error {
	return instance.doneChan
}

// The control routine that will watch for changes in the consumer group config
func (instance *ElasticConsumerGroupConsumerInstance) instanceRoutine(ctx context.Context) {
	for {
		select {
		case updateMsg, ok := <-instance.keyWatcher.Updates():
			if !ok {
				instance.stopConsuming()
				instance.doneChan <- errors.New("the elastic consumer group config watcher has been closed, stopping")
				return
			}

			if updateMsg == nil {
				break
			}

			// If the message is a delete operation, we stop consuming and return
			if updateMsg.Operation() == jetstream.KeyValueDelete {
				instance.stopConsuming()
				instance.doneChan <- nil
				return
			}

			var newConfig ElasticConsumerGroupConfig
			err := json.Unmarshal(updateMsg.Value(), &newConfig)
			if err != nil {
				instance.stopConsuming()
				instance.doneChan <- fmt.Errorf("elastic consumer group %s config watcher received a bad JSON message: %w", composeKey(instance.StreamName, instance.ConsumerGroupName), err)
				return
			}

			err = validateConfig(newConfig)
			if err != nil {
				instance.stopConsuming()
				instance.doneChan <- fmt.Errorf("elastic consumer group %s config watcher received an invalid config: %w", composeKey(instance.StreamName, instance.ConsumerGroupName), err)
				return
			}

			if newConfig.MaxMembers != instance.Config.MaxMembers ||
				newConfig.Filter != instance.Config.Filter || newConfig.MaxBufferedMsgs != instance.Config.MaxBufferedMsgs || newConfig.MaxBufferedBytes != instance.Config.MaxBufferedBytes ||
				!reflect.DeepEqual(newConfig.PartitioningWildcards, instance.Config.PartitioningWildcards) {
				instance.stopConsuming()
				instance.doneChan <- fmt.Errorf("elastic consumer group config %s watcher received a bad change in the configuration: max number of members, buffered messages, filter or partitioning wildcards changed", composeCGSName(instance.StreamName, instance.MemberName))
				return
			}
			// new config looks ok to use

			// optimization if nothing changed and already have the consumer for that member
			if instance.consumer != nil && reflect.DeepEqual(newConfig.Members, instance.Config.Members) && reflect.DeepEqual(newConfig.MemberMappings, instance.Config.MemberMappings) {
				break
			}

			instance.Config.Members = newConfig.Members
			instance.Config.MemberMappings = newConfig.MemberMappings
			instance.processMembershipChange(ctx)

		case <-ctx.Done():
			instance.stopConsuming()
			instance.doneChan <- nil
			return
		case <-time.After(consumerIdleTimeout + 1*time.Second):
			// We want it to always be trying to self-correct if it's not currently joined (and in membership, so we should really be trying to consume something).
			// This is what does the 'catch-all' if no-one deletes and re-creates the elastic consumers when the membership changes, because of the elastic consumer's idle time out eventually cleans up the old consumer.
			// Which means that the consumer Create in joinMemberConsumer will then re-create it with the right filters for our membership's view, and then we can start actually consuming from it.
			if instance.consumer == nil && instance.Config.IsInMembership(instance.MemberName) {
				instance.joinMemberConsumer()
			}
		}
	}
}

// CreateElastic creates an elastic consumer group
// Creates the sourcing work queue stream that is going to be used by the members to actually consume messages
func CreateElastic(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, maxNumMembers uint, filter string, partitioningWildcards []int, maxBufferedMessages int64, maxBufferedBytes int64) (*ElasticConsumerGroupConfig, error) {
	config := ElasticConsumerGroupConfig{
		MaxMembers:            maxNumMembers,
		Filter:                filter,
		PartitioningWildcards: partitioningWildcards,
		MaxBufferedMsgs:       maxBufferedMessages,
		MaxBufferedBytes:      maxBufferedBytes,
	}

	err := validateConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid elastic consumer group config: %w", err)
	}

	filterDest := getPartitioningTransformDest(config)

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	streamInfo, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	// The consumer group's stream will have the same number of replicas and storage as the source stream
	// Would people want an override for this?
	replicas := streamInfo.Config.Replicas
	storage := streamInfo.Config.Storage

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvElasticBucketName, Replicas: replicas, Storage: jetstream.FileStorage})
		if err != nil {
			return nil, err
		}
	}

	// Get or create the consumer group config
	consumerGroupConfig := ElasticConsumerGroupConfig{}
	value, err := kv.Get(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			// CreateElastic an entry as none exists yet
			consumerGroupConfig = config
			payload, err := json.Marshal(consumerGroupConfig)
			if err != nil {
				return nil, err
			}

			_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), payload)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		// check that the current entry matches the max number of members (i.e., number of partitions) and filter
		err = json.Unmarshal(value.Value(), &consumerGroupConfig)
		if err != nil {
			return nil, err
		}

		if consumerGroupConfig.MaxMembers != maxNumMembers || consumerGroupConfig.Filter != filter || consumerGroupConfig.MaxBufferedMsgs != maxBufferedMessages || consumerGroupConfig.MaxBufferedBytes != maxBufferedBytes ||
			!slices.Equal(consumerGroupConfig.PartitioningWildcards, partitioningWildcards) {
			return nil, errors.New("the existing elastic consumer group config can not be updated to the requested one, please delete the existing elastic consumer group and create a new one")
		}
	}

	// create the consumer group's stream
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      composeCGSName(streamName, consumerGroupName),
		Retention: jetstream.WorkQueuePolicy,
		Replicas:  replicas,
		Storage:   storage,
		MaxMsgs:   maxBufferedMessages,
		MaxBytes:  maxBufferedBytes,
		Discard:   jetstream.DiscardNew,
		Sources: []*jetstream.StreamSource{
			{
				Name:          streamName,
				OptStartSeq:   0,
				OptStartTime:  nil,
				FilterSubject: "",
				SubjectTransforms: []jetstream.SubjectTransformConfig{
					{
						Source:      filter,
						Destination: filterDest,
					},
				},
			},
		},
		AllowDirect: true,
	})
	if err != nil {
		return nil, fmt.Errorf("can't create the elastic consumer group's stream: %w", err)
	}

	return &consumerGroupConfig, nil
}

// DeleteElastic Deletes an elastic consumer group
func DeleteElastic(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string) error {
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return err
	}

	// First delete the PCG's entry in the KV bucket
	err = kv.Delete(ctx, composeKey(streamName, consumerGroupName))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return errors.New("error deleting the elastic consumer groups' configs")
	}

	// Then delete the PCG's stream
	err = js.DeleteStream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return fmt.Errorf("could not delete the elastic consumer group's stream: %w", err)
	}

	return nil
}

// ListElasticConsumerGroups lists the elastic consumer groups for a given stream
func ListElasticConsumerGroups(ctx context.Context, js jetstream.JetStream, streamName string) ([]string, error) {
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, fmt.Errorf("error getting elastic consumer group KV bucket: %w", err)
	}

	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating a key lister on the elastic consumer groups' bucket: %w", err)
	}

	var consumerGroupNames []string

	for key := range lister.Keys() {
		parts := strings.Split(key, ".")
		if parts[0] == streamName {
			if len(parts) >= 2 {
				consumerGroupNames = append(consumerGroupNames, parts[1])
			}
		}
	}

	return consumerGroupNames, nil
}

// AddMembers adds members to an elastic consumer group
func AddMembers(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberNamesToAdd []string) ([]string, error) {
	if streamName == "" || consumerGroupName == "" || len(memberNamesToAdd) == 0 {
		return nil, errors.New("invalid stream name or elastic consumer group name or no member names")
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, fmt.Errorf("can not get the current elastic consumer group's config: %w", err)
	}

	var existingMembers = make(map[string]struct{})

	if len(consumerGroupConfig.MemberMappings) != 0 {
		return nil, errors.New("can't add members to an elastic consumer group that uses member mappings")
	}

	for _, existingMember := range consumerGroupConfig.Members {
		existingMembers[existingMember] = struct{}{}
	}

	for _, memberName := range memberNamesToAdd {
		if memberName != "" {
			existingMembers[memberName] = struct{}{}
		}
	}

	var newMembers []string
	for member := range existingMembers {
		newMembers = append(newMembers, member)
	}

	consumerGroupConfig.Members = newMembers

	marshaled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshall the elastic consumer group's config: %w", err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshaled)
	if err != nil {
		return nil, fmt.Errorf("couldn't put the elastic consumer group's config in the bucket: %w", err)
	}

	return consumerGroupConfig.Members, nil
}

// DeleteMembers drops members from an elastic consumer group
func DeleteMembers(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberNamesToDrop []string) ([]string, error) {
	if streamName == "" || consumerGroupName == "" || len(memberNamesToDrop) == 0 {
		return nil, errors.New("invalid stream name or elastic consumer group name or no member names")
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, fmt.Errorf("can not get the current elastic consumer group's config: %w", err)
	}

	droppingMembers := make(map[string]struct{})

	if len(consumerGroupConfig.MemberMappings) != 0 {
		return nil, errors.New("can't drop members from a elastic consumer group that uses member mappings")
	}

	for _, droppingMember := range memberNamesToDrop {
		droppingMembers[droppingMember] = struct{}{}
	}

	var newMembers []string

	for _, existingMember := range consumerGroupConfig.Members {
		if _, ok := droppingMembers[existingMember]; !ok {
			newMembers = append(newMembers, existingMember)
		}
	}

	consumerGroupConfig.Members = newMembers

	marshaled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshall the elastic consumer group's config: %w", err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshaled)
	if err != nil {
		return nil, fmt.Errorf("couldn't put the elastic consumer group's config in the bucket: %w", err)
	}

	return consumerGroupConfig.Members, nil
}

// SetMemberMappings sets the custom member mappings for an elastic consumer group
func SetMemberMappings(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberMappings []MemberMapping) error {
	if streamName == "" || consumerGroupName == "" || len(memberMappings) == 0 {
		return errors.New("invalid stream name or elastic consumer group name or member mappings")
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return fmt.Errorf("can not get the current elastic consumer group's config: %w", err)
	}

	if len(consumerGroupConfig.Members) != 0 {
		consumerGroupConfig.Members = []string{}
	}

	consumerGroupConfig.MemberMappings = memberMappings

	err = validateConfig(*consumerGroupConfig)
	if err != nil {
		return fmt.Errorf("invalid elastic consumer group config: %w", err)
	}

	marshaled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return fmt.Errorf("couldn't marshall the elastic consumer group's config: %w", err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshaled)
	if err != nil {
		return fmt.Errorf("couldn't put the elastic consumer group's config in the bucket: %w", err)
	}

	return nil
}

// DeleteMemberMappings deletes the custom member mappings for an elastic consumer group
func DeleteMemberMappings(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string) error {
	if streamName == "" || consumerGroupName == "" {
		return errors.New("invalid stream name or elastic consumer group name or member mappings")
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return fmt.Errorf("the elastic consumer group KV bucket doesn't exist: %w", err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return fmt.Errorf("can not get the current elastic consumer group's config: %w", err)
	}

	consumerGroupConfig.MemberMappings = []MemberMapping{}

	marshaled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return fmt.Errorf("couldn't marshall the elastic consumer group's config: %w", err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshaled)
	if err != nil {
		return fmt.Errorf("couldn't put the elastic consumer group's config in the bucket: %w", err)
	}

	return nil
}

// ListElasticActiveMembers lists the active members of an elastic consumer group
func ListElasticActiveMembers(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string) ([]string, error) {
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, err
	}

	var activeMembers []string

	cGC, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, err
	}

	if len(cGC.Members) == 0 && len(cGC.MemberMappings) == 0 {
		return []string{}, nil
	}

	s, err := js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return nil, err
	}

	lister := s.ListConsumers(ctx)
	for cInfo := range lister.Info() {
		if len(cGC.Members) != 0 {
			for _, m := range cGC.Members {
				if cInfo.Name == m {
					activeMembers = append(activeMembers, cInfo.Name)
					break
				}
			}
		} else if len(cGC.MemberMappings) != 0 {
			for _, mapping := range cGC.MemberMappings {
				if cInfo.Name == mapping.Member {
					activeMembers = append(activeMembers, cInfo.Name)
					break
				}
			}
		}
	}

	return activeMembers, nil
}

// ElasticIsInMembershipAndActive checks if a member is included in the elastic consumer group and is active
func ElasticIsInMembershipAndActive(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberName string) (bool, bool, error) {
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return false, false, err
	}

	cGC, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return false, false, err
	}

	inMembership := cGC.IsInMembership(memberName)

	s, err := js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return false, false, err
	}

	isActive := false
	lister := s.ListConsumers(ctx)

	for cInfo := range lister.Info() {
		if len(cGC.Members) != 0 {
			for _, m := range cGC.Members {
				if cInfo.Name == m {
					isActive = true
					break
				}
			}
		} else if len(cGC.MemberMappings) != 0 {
			for _, mapping := range cGC.MemberMappings {
				if cInfo.Name == mapping.Member {
					isActive = true
					break
				}
			}
		}
	}

	return inMembership, isActive, nil
}

// ElasticMemberStepDown forces the current active (pinned) application instance for a member of an elastic consumer group to step down
func ElasticMemberStepDown(ctx context.Context, js jetstream.JetStream, streamName string, consumerGroupName string, memberName string) error {
	s, err := js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return err
	}

	err = s.UnpinConsumer(ctx, memberName, memberName)
	return err
}

// ElasticGetPartitionFilters For the given ElasticConsumerGroupConfig returns the list of partition filters for the given member
func ElasticGetPartitionFilters(config ElasticConsumerGroupConfig, memberName string) []string {
	return GeneratePartitionFilters(config.Members, config.MaxMembers, config.MemberMappings, memberName)
}

// Shim callback function to strip the partition number from the subject before passing the message to the user's callback
func (instance *ElasticConsumerGroupConsumerInstance) consumerCallback(msg jetstream.Msg) {
	// check pinned-id is there
	pid := msg.Headers().Get("Nats-Pin-Id")
	if pid == "" {
		log.Println("Warning: received a message without a pinned-id header")
		// TODO should we give up here and say there's a problem? (maybe running over a pre 2.11 version of the server?)
	} else {
		if instance.currentPinnedID == "" {
			instance.currentPinnedID = pid
		} else if instance.currentPinnedID != pid {
			// received a message with a different pinned-id header, assuming there was a change of pinned member
			instance.currentPinnedID = pid
		}
	}

	strippedMessage := newConsumerGroupMsg(msg)
	instance.MessageHandlerCB(strippedMessage)
}

// joinMemberConsumer attempts to create (which is idempotent for a given consumer configuration) the member's consumer if the member is in the current list of members and if successful, starts consuming messages from it
func (instance *ElasticConsumerGroupConsumerInstance) joinMemberConsumer() {
	var err error
	ctx := context.Background()

	filters := ElasticGetPartitionFilters(*instance.Config, instance.MemberName)

	// if we are no longer in the membership list, nothing to do
	if len(filters) == 0 {
		return
	}

	config := instance.consumerUserConfig
	config.Durable = ""
	config.Name = instance.MemberName
	config.FilterSubjects = filters

	config.PriorityGroups = []string{instance.MemberName}
	config.PriorityPolicy = jetstream.PriorityPolicyPinned
	config.PinnedTTL = config.AckWait

	// before starting to actually consume messages from the stream consumer, we need to verify that the consumer is created with the correct filters, so Create() must be successful
	instance.consumer, err = instance.js.CreateConsumer(ctx, composeCGSName(instance.StreamName, instance.ConsumerGroupName), config)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerExists) {
			// try to delete the consumer if we can't create it to our desired config, we or someone else will try to re-create it within 5 seconds
			err := instance.js.DeleteConsumer(ctx, composeCGSName(instance.StreamName, instance.ConsumerGroupName), instance.MemberName)
			if err != nil {
				log.Printf("Warning: error trying to delete our member's consumer after trying to create it to our desired config: %v\n", err)
				// will try again later
				return
			} else {
				instance.consumer, err = instance.js.CreateConsumer(ctx, composeCGSName(instance.StreamName, instance.ConsumerGroupName), config)
				if err != nil {
					// will try again later
					return
				}
			}
		}
		// just return in any case
		// not logging here because some errors can happen during normal operation
		// e.g. JS API error: filtered consumer not unique on workqueue stream can happen because all the members cannot be perfectly synchronized processing membership changes
		return
	}

	instance.startConsuming()
}

// Start to actively consume (pull) messages from the consumer
func (instance *ElasticConsumerGroupConsumerInstance) startConsuming() {
	var err error

	instance.consumerConsumeContext, err = instance.consumer.Consume(instance.consumerCallback, jetstream.PullExpiry(pullTimeout), jetstream.PullPriorityGroup(instance.MemberName))
	if err != nil {
		log.Printf("Error starting to consume on my consumer: %v\n", err)
		return
	}
}

// Stops the consumption of messages from the consumer
func (instance *ElasticConsumerGroupConsumerInstance) stopConsuming() {
	if instance.consumer != nil {
		if instance.consumerConsumeContext != nil {
			instance.consumerConsumeContext.Stop()
			instance.consumerConsumeContext = nil
		}

		instance.consumer = nil
	}
}

// Processes membership changes
func (instance *ElasticConsumerGroupConsumerInstance) processMembershipChange(ctx context.Context) {
	// get the current consumer info to get the current pinned-id
	var isPinned bool

	if instance.consumer != nil {
		ci, err := instance.consumer.Info(ctx)
		if err == nil { // ignoring error as the consumer may not exist yet
			if slices.ContainsFunc(ci.PriorityGroups, func(pg jetstream.PriorityGroupState) bool {
				return pg.Group == instance.MemberName && pg.PinnedClientID == instance.currentPinnedID
			}) {
				isPinned = true
			}
		}
		instance.stopConsuming()
	}

	// We must first delete the consumer in case some new partitions got assigned to us as a member was removed.
	// Just adding the filters to our existing consumer is not enough as you may have to now go back to a previous sequence number in the stream
	// as we may have already gone past those sequence numbers so must delete and recreate the consumer
	//
	// Note: there's a chance we are currently processing a message and have not acked it yet and the ack could happen before the consumer is re-created
	// in that case the ack will fail (error on sync ack) and the message will be redelivered which may look like a duplicate message if the user doesn't use sync acks
	// Maybe doing something to wait bit, like a consumer pause first and then wait for some time (ack wait?) before deleting and re-creating?

	// only the pinned member should delete the consumer
	if isPinned {
		err := instance.js.DeleteConsumer(ctx, composeCGSName(instance.StreamName, instance.ConsumerGroupName), instance.MemberName)
		if err != nil {
			if !errors.Is(err, jetstream.ErrConsumerNotFound) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("Error trying to delete our member's consumer: %v\n", err)
			}
		}
	} else {
		// backoff, if someone else believes they are pinned let them delete and re-create the consumer first (to try and avoid flapping)
		time.Sleep(time.Duration(rand.IntN(100)+400) * time.Millisecond)
	}

	instance.joinMemberConsumer()
}

func validateConfig(config ElasticConsumerGroupConfig) error {
	// Validate the max number of members
	numActualPartitions := config.MaxMembers
	if numActualPartitions < 1 {
		return errors.New("the max number of members must be >= 1")
	}

	// validate the filter and the partitioning wildcards
	filterTokens := strings.Split(config.Filter, ".")
	numWildcards := 0

	for _, s := range filterTokens {
		if s == "*" {
			numWildcards++
		}
	}

	if numWildcards < 1 {
		return errors.New("filter must contain at least one * wildcard")
	}

	if len(config.PartitioningWildcards) < 1 || len(config.PartitioningWildcards) > numWildcards {
		return errors.New("the number of partitioning wildcards must be between 1 and the total number of * wildcards in the filter")
	}

	pwcs := make(map[int]struct{}) // partitioning wildcard presence

	for _, pWildcard := range config.PartitioningWildcards {
		if _, ok := pwcs[pWildcard]; ok {
			return errors.New("partitioning wildcard indexes must be unique")
		} else {
			pwcs[pWildcard] = struct{}{}
		}

		if pWildcard < 1 || pWildcard > numWildcards {
			return errors.New("partitioning wildcard indexes must be greater than 1 and less than or equal to the number of * wildcards in the filter")
		}
	}

	// validate the members and member mappings
	if len(config.Members) != 0 && len(config.MemberMappings) != 0 {
		return errors.New("either members or member mappings must be provided, not both")
	}

	// We are pretty tolerant for members as we always deduplicate them and cap them to the max number of members when processing the membership change

	if len(config.MemberMappings) != 0 {
		if len(config.MemberMappings) < 1 || len(config.MemberMappings) > int(numActualPartitions) {
			return errors.New("the number of member mappings must be between 1 and the max number of members")
		}

		members := make(map[string]struct{})
		partitions := make(map[int]struct{})

		for _, mm := range config.MemberMappings {
			if _, ok := members[mm.Member]; ok {
				return errors.New("member names must be unique")
			} else {
				members[mm.Member] = struct{}{}
			}

			for _, p := range mm.Partitions {
				if _, ok := partitions[p]; ok {
					return errors.New("partition numbers must be used only once")
				} else {
					partitions[p] = struct{}{}
				}

				if p < 0 || p >= int(numActualPartitions) {
					return errors.New("partition numbers must be between 0 and one less than the max number of members")
				}
			}
		}

		var uniquePartitionNumbersCount int

		for range partitions {
			uniquePartitionNumbersCount++
		}

		if uniquePartitionNumbersCount != int(numActualPartitions) {
			return errors.New("the number of unique partition numbers must be equal to the max number of members")
		}
	}

	return nil
}

func getPartitioningTransformDest(config ElasticConsumerGroupConfig) string {
	wildcards := make([]string, len(config.PartitioningWildcards))

	for i, wc := range config.PartitioningWildcards {
		wildcards[i] = strconv.Itoa(wc)
	}

	wildcardList := strings.Join(wildcards, ",")
	cwIndex := 1
	filterTokens := strings.Split(config.Filter, ".")

	for i, s := range filterTokens {
		if s == "*" {
			filterTokens[i] = fmt.Sprintf("{{Wildcard(%d)}}", cwIndex)
			cwIndex++
		}
	}

	destFromFilter := strings.Join(filterTokens, ".")
	return fmt.Sprintf("{{Partition(%d,%s)}}.%s", config.MaxMembers, wildcardList, destFromFilter)
}

func getElasticConsumerGroupConfig(ctx context.Context, kv jetstream.KeyValue, streamName string, consumerGroupName string) (*ElasticConsumerGroupConfig, error) {
	if streamName == "" || consumerGroupName == "" {
		return nil, errors.New("invalid stream name or elastic consumer group name")
	}

	message, err := kv.Get(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, errors.New("error getting the elastic consumer group's config: not found")
		} else {
			return nil, fmt.Errorf("error getting the elastic consumer group's config: %w", err)
		}
	}

	var consumerGroupConfig ElasticConsumerGroupConfig

	err = json.Unmarshal(message.Value(), &consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON value for the elastic consumer group's config: %w", err)
	}

	err = validateConfig(consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid elastic consumer group config: %w", err)
	}

	return &consumerGroupConfig, nil
}

func deduplicateStringSlice(slice []string) []string {
	seen := make(map[string]struct{})
	var set []string

	for _, s := range slice {
		if _, exist := seen[s]; !exist {
			seen[s] = struct{}{}
			set = append(set, s)
		}
	}

	return set
}

// Compose the Consumer Group Stream Name
func composeCGSName(streamName string, consumerGroupName string) string {
	return streamName + "-" + consumerGroupName
}
