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

package streamconsumergroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"reflect"
	"slices"
	"strings"
)

const (
	kvStaticBucketName = "static-consumer-groups"
)

type StaticConsumerGroupConsumerInstance struct {
	StreamName             string
	ConsumerGroupName      string
	MemberName             string
	CConfig                jetstream.ConsumerConfig
	CGConfig               *StaticConsumerGroupConfig
	Consumer               jetstream.Consumer
	CurrentPID             string
	ConsumerConsumeContext jetstream.ConsumeContext
	MessageHandlerCB       func(msg jetstream.Msg)
	js                     jetstream.JetStream
	kv                     jetstream.KeyValue
}

type StaticConsumerGroupConfig struct {
	MaxMembers     uint            `json:"max_members"`
	Filter         string          `json:"filter"`
	Members        []string        `json:"members,omitempty"`
	MemberMappings []MemberMapping `json:"member_mappings,omitempty"`
}

func (config *StaticConsumerGroupConfig) IsInMembership(name string) bool {
	// valid config has either members or member mappings
	return slices.ContainsFunc(config.MemberMappings, func(mapping MemberMapping) bool { return mapping.Member == name }) || slices.Contains(config.Members, name)
}

// GetStaticConsumerGroupConfig gets the static consumer group's config from the KV bucket
func GetStaticConsumerGroupConfig(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string) (*StaticConsumerGroupConfig, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the static consumer group KV bucket doesn't exist: %w", err)
	}

	return getStaticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
}

// StaticConsume is the main consume function that will consume messages from the stream (when active) and call the message handler for each message
// meant to be used in a go routine
func StaticConsume(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, memberName string, messageHandler func(msg jetstream.Msg), cconfig jetstream.ConsumerConfig) error {
	if messageHandler == nil {
		return errors.New("a message handler must be provided")
	}

	instance := StaticConsumerGroupConsumerInstance{
		StreamName:        streamName,
		ConsumerGroupName: consumerGroupName,
		MemberName:        memberName,
		CConfig:           cconfig,
		MessageHandlerCB:  messageHandler,
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	instance.js = js

	_, err = js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("the static consumer group's stream does not exist: %w", err)
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return fmt.Errorf("the static consumer group KV bucket doesn't exist: %w", err)
	}

	instance.kv = kv

	// Try to get the current config if there's one
	instance.CGConfig, err = getStaticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return fmt.Errorf("can not get the current static consumer group's config: %w", err)
	}

	if instance.CGConfig.IsInMembership(memberName) {
		err = instance.joinMemberConsumerStatic(ctx)
		if err != nil {
			return err
		}
	} else {
		return errors.New("the member name is not in the current static consumer group membership")
	}

	// Since static config doesn't change, we can just watch for the deletion of the consumer group's config
	watcher, err := kv.Watch(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		return err
	}
	for {
		select {
		case updateMsg, ok := <-watcher.Updates():
			if ok {
				if updateMsg != nil {
					// If the message is a delete operation, we stop and return
					if updateMsg.Operation() == jetstream.KeyValueDelete {
						instance.stopAndDeleteMemberConsumer()
						return nil
					}

					var newConfig StaticConsumerGroupConfig
					err := json.Unmarshal(updateMsg.Value(), &newConfig)
					if err != nil {
						return fmt.Errorf("static consumer group %s config watcher received a bad JSON message: %w", composeKey(streamName, consumerGroupName), err)
					}

					err = validateStaticConfig(newConfig)
					if err != nil {
						return fmt.Errorf("static consumer group %s config watcher received an invalid config: %w", composeKey(streamName, consumerGroupName), err)
					}

					if newConfig.MaxMembers != instance.CGConfig.MaxMembers ||
						newConfig.Filter != instance.CGConfig.Filter ||
						!reflect.DeepEqual(newConfig.Members, instance.CGConfig.Members) || !reflect.DeepEqual(newConfig.MemberMappings, instance.CGConfig.MemberMappings) {
						instance.stopAndDeleteMemberConsumer()
						return errors.New(" static consumer group config watcher received a change in the configuration, terminating")
					}
					// No change in the config, ignore
				}
			}
		case <-ctx.Done():
			instance.stop()
			return nil
		}
	}
}

// CreateStatic creates a consumer group
func CreateStatic(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, maxNumMembers uint, filter string, members []string, memberMappings []MemberMapping) (*StaticConsumerGroupConfig, error) {
	config := StaticConsumerGroupConfig{
		MaxMembers:     maxNumMembers,
		Filter:         filter,
		Members:        members,
		MemberMappings: memberMappings,
	}

	err := validateStaticConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid static consumer group config: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("couldn't get the JetStream instance: %w", err)
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	streamInfo, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	replicas := streamInfo.Config.Replicas

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvStaticBucketName, Replicas: replicas, Storage: jetstream.FileStorage})
		if err != nil {
			return nil, err
		}
	}

	// Get or create the consumer group config
	cgConfig := StaticConsumerGroupConfig{}
	value, err := kv.Get(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			// CreateStatic an entry as none exists yet
			cgConfig = config
			payload, err := json.Marshal(cgConfig)
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
		// check that the current entry matches the max number of members (i.e. number of partitions) and filter
		err = json.Unmarshal(value.Value(), &cgConfig)
		if err != nil {
			return nil, fmt.Errorf("static consumer group config already exists and is not valid JSON: %w", err)
		}

		if cgConfig.MaxMembers != maxNumMembers || cgConfig.Filter != filter || !slices.Equal(cgConfig.Members, members) || !reflect.DeepEqual(cgConfig.MemberMappings, memberMappings) {
			return nil, errors.New("the existing static consumer group config doesn't match ours")
		}
	}

	return &cgConfig, nil
}

// DeleteStatic Deletes a consumer group
func DeleteStatic(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("couldn't get the JetStream instance: %w", err)
	}

	// First delete the config bucket's entry all the consumers for the consumer group
	s, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return err
	}

	// Just delete the PCG's entry in the KV bucket
	err = kv.Delete(ctx, composeKey(streamName, consumerGroupName))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return errors.New("error deleting the static consumer groups' configs")
	}

	lister := s.ListConsumers(ctx)

	var consumerDeleteErrors []error

	for i := range lister.Info() {
		if strings.HasPrefix(i.Name, consumerGroupName+"-") {
			err = s.DeleteConsumer(ctx, i.Name)
			if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
				consumerDeleteErrors = append(consumerDeleteErrors, fmt.Errorf("error deleting consumer %s: %w", i.Name, err))
			}
		}
	}

	return errors.Join(consumerDeleteErrors...)
}

// ListStaticConsumerGroups lists the consumer groups for a given stream
func ListStaticConsumerGroups(ctx context.Context, nc *nats.Conn, streamName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return nil, fmt.Errorf("the static consumer group's KV bucket doesn't exist: %w", err)
	}

	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating a key lister on the static consumer groups' bucket: %w", err)
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

func ListStaticActiveMembers(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return nil, err
	}

	var activeMembers []string

	cGC, err := getStaticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	lister := s.ListConsumers(ctx)

	for cInfo := range lister.Info() {
		if len(cGC.Members) != 0 {
			for _, m := range cGC.Members {
				if cInfo.Name == composeStaticConsumerName(consumerGroupName, m) && cInfo.NumWaiting != 0 {
					activeMembers = append(activeMembers, m)
					break
				}
			}
		} else if len(cGC.MemberMappings) != 0 {
			for _, mapping := range cGC.MemberMappings {
				if cInfo.Name == composeStaticConsumerName(consumerGroupName, mapping.Member) && cInfo.NumWaiting != 0 {
					activeMembers = append(activeMembers, mapping.Member)
					break
				}
			}
		}
	}

	return activeMembers, nil
}

// StaticMemberStepDown forces the current active instance of a member to step down
func StaticMemberStepDown(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, memberName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	s, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}

	err = s.UnpinConsumer(ctx, composeStaticConsumerName(consumerGroupName, memberName), memberName)
	if err != nil {
		log.Printf("Error trying to unpin the member's consumer: %v\n", err)
		return err
	}
	return nil
}

// Shim callback function to strip the partition number from the subject before passing the message to the user's callback
func (consumerInstance *StaticConsumerGroupConsumerInstance) consumerCallback(msg jetstream.Msg) {
	// check pinned-id is there
	pid := msg.Headers().Get("Nats-Pin-Id")
	if pid == "" {
		log.Println("Warning: received a message without a pinned-id header")
		// TODO should we give up here and say there's a problem? (maybe running over a pre 2.11 version of the server?)
	} else {
		if consumerInstance.CurrentPID == "" {
			consumerInstance.CurrentPID = pid
		} else if consumerInstance.CurrentPID != pid {
			// received a message with a different pinned-id header, assuming there was a change of pinned member
			consumerInstance.CurrentPID = pid
		}
	}

	strippedMessage := newConsumerGroupMsg(msg)
	consumerInstance.MessageHandlerCB(strippedMessage)
}

// CreateStatic the member's consumer if the member is in the current list of members and starts consuming
func (consumerInstance *StaticConsumerGroupConsumerInstance) joinMemberConsumerStatic(ctx context.Context) error {
	var err error

	filters := GeneratePartitionFilters(consumerInstance.CGConfig.Members, consumerInstance.CGConfig.MaxMembers, consumerInstance.CGConfig.MemberMappings, consumerInstance.MemberName)

	if len(filters) == 0 {
		return nil
	}

	config := consumerInstance.CConfig
	config.Durable = composeStaticConsumerName(consumerInstance.ConsumerGroupName, consumerInstance.MemberName)
	config.FilterSubjects = filters

	if config.AckWait == 0 {
		config.AckWait = ackWait
	}

	config.PriorityGroups = []string{consumerInstance.MemberName}
	config.PriorityPolicy = jetstream.PriorityPolicyPinned
	config.PinnedTTL = config.AckWait

	consumerInstance.Consumer, err = consumerInstance.js.CreateConsumer(ctx, consumerInstance.StreamName, config)
	if err != nil {
		return fmt.Errorf("error creating our member's consumer: %w", err)
	}

	consumerInstance.startConsuming()

	return nil
}

// when instance becomes the active one
func (consumerInstance *StaticConsumerGroupConsumerInstance) startConsuming() {
	var err error

	consumerInstance.ConsumerConsumeContext, err = consumerInstance.Consumer.Consume(consumerInstance.consumerCallback, jetstream.PullExpiry(pullTimeout), jetstream.PullPriorityGroup(consumerInstance.MemberName))
	if err != nil {
		log.Printf("Error starting to consume on my consumer: %v\n", err)
		return
	}
}

func (consumerInstance *StaticConsumerGroupConsumerInstance) stop() {
	if consumerInstance.Consumer != nil {
		if consumerInstance.ConsumerConsumeContext != nil {
			consumerInstance.ConsumerConsumeContext.Stop()
			consumerInstance.ConsumerConsumeContext = nil
		}

		consumerInstance.Consumer = nil
	}
}

func (consumerInstance *StaticConsumerGroupConsumerInstance) stopAndDeleteMemberConsumer() {
	consumerInstance.stop()

	err := consumerInstance.js.DeleteConsumer(context.Background(), consumerInstance.StreamName, composeStaticConsumerName(consumerInstance.ConsumerGroupName, consumerInstance.MemberName))
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return
	}
	if err != nil {
		log.Printf("Error trying to delete our member's consumer: %v\n", err)
	}
}

func validateStaticConfig(config StaticConsumerGroupConfig) error {
	// First validate the max number of members
	numActualPartitions := config.MaxMembers
	if numActualPartitions < 1 {
		return errors.New("the max number of members must be >= 1")
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

		members := make(map[string]any)
		partitions := make(map[int]any)

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

func getStaticConsumerGroupConfig(ctx context.Context, kv jetstream.KeyValue, streamName string, consumerGroupName string) (*StaticConsumerGroupConfig, error) {
	if streamName == "" || consumerGroupName == "" {
		return nil, errors.New("invalid stream name or consumer group name")
	}

	message, err := kv.Get(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, errors.New("error getting the static consumer group's config: not found")
		} else {
			return nil, fmt.Errorf("error getting the static consumer group's config: %w", err)
		}
	}

	var consumerGroupConfig StaticConsumerGroupConfig

	err = json.Unmarshal(message.Value(), &consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON value for the static consumer group's config: %w", err)
	}

	err = validateStaticConfig(consumerGroupConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid static consumer group config: %w", err)
	}

	return &consumerGroupConfig, nil
}

// compose the stream's consumer name for the member in the static consumer group
func composeStaticConsumerName(cgName string, member string) string {
	return cgName + "-" + member
}
