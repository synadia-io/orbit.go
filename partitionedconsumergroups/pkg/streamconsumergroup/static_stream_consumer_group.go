package streamconsumergroup

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"reflect"
	"slices"
	"strings"
	"time"
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
	MemberMappings []MemberMapping `json:"member-mappings,omitempty"`
}

func (config *StaticConsumerGroupConfig) IsInMembership(name string) bool {
	// valid config has either members or member mappings
	return slices.ContainsFunc(config.MemberMappings, func(mapping MemberMapping) bool { return mapping.Member == name }) || slices.Contains(config.Members, name)
}

// GetStaticConsumerGroupConfig gets the static consumer group's config from the KV bucket
func GetStaticConsumerGroupConfig(nc *nats.Conn, streamName string, consumerGroupName string) (*StaticConsumerGroupConfig, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	return getStaticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
}

// StaticConsume is the main consume function that will consume messages from the stream (when active) and call the message handler for each message
// meant to be used in a go routine
func StaticConsume(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, memberName string, messageHandler func(msg jetstream.Msg), cconfig jetstream.ConsumerConfig) error {
	var err error

	instance := StaticConsumerGroupConsumerInstance{
		StreamName:             streamName,
		ConsumerGroupName:      consumerGroupName,
		MemberName:             memberName,
		CConfig:                cconfig,
		CGConfig:               nil,
		Consumer:               nil,
		ConsumerConsumeContext: nil,
		CurrentPID:             "",
		MessageHandlerCB:       nil,
		js:                     nil,
		kv:                     nil,
	}

	if messageHandler == nil {
		return errors.New("a message handler must be provided")
	}

	instance.MessageHandlerCB = messageHandler

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	instance.js = js

	_, err = js.Stream(ctx, streamName)
	if err != nil {
		return errors.Join(errors.New("the consumer group's stream does not exist"), err)
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	instance.kv = kv

	// Try to get the current config if there's one
	instance.CGConfig, err = getStaticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return errors.Join(errors.New("can not get the current consumer group's config"), err)
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
						// Human error is very possible if they put the config messages directly (e.g. using `nats kv put`)
						// Best to ignore or better to error out?
						log.Printf("Error:  consumer group %s config watcher received and is ignoring a bad JSON message", composeKey(streamName, consumerGroupName))
						break
					}

					err = validateStaticConfig(newConfig)
					if err != nil {
						// same question ignore or error out?
						log.Printf("Error:  consumer group %s config watcher received and is ignoring an invalid config", composeKey(streamName, consumerGroupName))
						break
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
		return nil, errors.Join(errors.New("invalid static consumer group config"), err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream instance: %v", err)
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
			return nil, err
		}

		if cgConfig.MaxMembers != maxNumMembers || cgConfig.Filter != filter || !slices.Equal(cgConfig.Members, members) || !reflect.DeepEqual(cgConfig.MemberMappings, memberMappings) {
			return nil, errors.New("the existing consumer group config doesn't match ours")
		}
	}

	return &cgConfig, nil
}

// DeleteStatic Deletes a consumer group
func DeleteStatic(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream instance: %v", err)
	}

	// First delete all the consumers for the consumer group
	s, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}

	lister := s.ListConsumers(ctx)

	for i := range lister.Info() {
		if strings.HasPrefix(i.Name, consumerGroupName+"-") {
			_ = s.DeleteConsumer(ctx, i.Name)
		}
	}

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return err
	}

	// Just delete the PCG's entry in the KV bucket
	err = kv.Delete(ctx, composeKey(streamName, consumerGroupName))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return errors.New("error deleting the consumer groups' configs")
	}

	return nil
}

// ListStaticConsumerGroups lists the consumer groups for a given stream
func ListStaticConsumerGroups(nc *nats.Conn, streamName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvStaticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, errors.Join(errors.New("error creating a key lister on the consumer groups' bucket"), err)
	}

	var consumerGroupNames []string

	for key := range lister.Keys() {
		parts := strings.Split(key, ".")
		if parts[0] == streamName {
			consumerGroupNames = append(consumerGroupNames, parts[1])
		}
	}

	return consumerGroupNames, nil
}

func ListStaticActiveMembers(nc *nats.Conn, streamName string, consumerGroupName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
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
func StaticMemberStepDown(nc *nats.Conn, streamName string, consumerGroupName string, memberName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

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
		config.AckWait = 6 * time.Second
	}

	config.PriorityGroups = []string{consumerInstance.MemberName}
	config.PriorityPolicy = jetstream.PriorityPolicyPinned
	config.PinnedTTL = config.AckWait

	consumerInstance.Consumer, err = consumerInstance.js.CreateConsumer(ctx, consumerInstance.StreamName, config)
	if err != nil {
		return errors.Join(errors.New("error creating our member's consumer"), err)
	}

	consumerInstance.startConsuming()

	return nil
}

// when instance becomes the active one
func (consumerInstance *StaticConsumerGroupConsumerInstance) startConsuming() {
	var err error

	consumerInstance.ConsumerConsumeContext, err = consumerInstance.Consumer.Consume(consumerInstance.consumerCallback, jetstream.PullExpiry(3*time.Second), jetstream.PullPriorityGroup(consumerInstance.MemberName))
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
			return nil, errors.New("error getting the consumer group's config: not found")
		} else {
			return nil, errors.Join(errors.New("error getting the consumer group's config"), err)
		}
	}

	var consumerGroupConfig StaticConsumerGroupConfig

	err = json.Unmarshal(message.Value(), &consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("invalid JSON value for the consumer group's config"), err)
	}

	err = validateStaticConfig(consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("invalid consumer group config"), err)
	}

	return &consumerGroupConfig, nil
}

// compose the stream's consumer name for the member in the static consumer group
func composeStaticConsumerName(cgName string, member string) string {
	return cgName + "-" + member
}
