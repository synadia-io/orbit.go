package streamconsumergroup

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	kvElasticBucketName = "elastic-consumer-groups"
)

type ElasticConsumerGroupConsumerInstance struct {
	StreamName             string
	ConsumerGroupName      string
	MemberName             string
	CConfig                jetstream.ConsumerConfig
	Config                 *ElasticConsumerGroupConfig
	Consumer               jetstream.Consumer
	CurrentPID             string
	ConsumerConsumeContext jetstream.ConsumeContext
	MessageHandlerCB       func(msg jetstream.Msg)
	js                     jetstream.JetStream
	kv                     jetstream.KeyValue
}

type ElasticConsumerGroupConfig struct {
	MaxMembers            uint            `json:"max_members"`
	Filter                string          `json:"filter"`
	PartitioningWildcards []int           `json:"partitioning-wildcards"`
	MaxBufferedMsgs       int64           `json:"max-buffered-msg,omitempty"`
	MaxBufferedBytes      int64           `json:"max-buffered-bytes,omitempty"`
	Members               []string        `json:"members,omitempty"`
	MemberMappings        []MemberMapping `json:"member-mappings,omitempty"`
}

func (config *ElasticConsumerGroupConfig) IsInMembership(name string) bool {
	// valid config has either members or member mappings
	return slices.ContainsFunc(config.MemberMappings, func(mapping MemberMapping) bool { return mapping.Member == name }) || slices.Contains(config.Members, name)
}

// GetElasticConsumerGroupConfig gets the consumer group's config from the KV bucket
func GetElasticConsumerGroupConfig(nc *nats.Conn, streamName string, consumerGroupName string) (*ElasticConsumerGroupConfig, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	return getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
}

// ElasticConsume is the main consume function that will consume messages from the stream (when active) and call the message handler for each message
// meant to be used in a go routine
func ElasticConsume(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, memberName string, messageHandler func(msg jetstream.Msg), config jetstream.ConsumerConfig) error {
	var err error

	instance := ElasticConsumerGroupConsumerInstance{
		StreamName:             streamName,
		ConsumerGroupName:      consumerGroupName,
		MemberName:             memberName,
		CConfig:                config,
		Config:                 nil,
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

	_, err = js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return errors.Join(errors.New("the consumer group's stream does not exist"), err)
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	instance.kv = kv

	// Try to get the current config if there's one
	instance.Config, err = getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return errors.Join(errors.New("can not get the current consumer group's config"), err)
	}

	if instance.Config.IsInMembership(memberName) {
		instance.joinMemberConsumer(ctx)
	}

	watcher, err := kv.Watch(ctx, composeKey(streamName, consumerGroupName))
	if err != nil {
		return err
	}
	for {
		select {
		case updateMsg, ok := <-watcher.Updates():
			if ok {
				if updateMsg != nil {
					if updateMsg.Operation() == jetstream.KeyValueDelete {
						instance.stop()

						return errors.New(" consumer group config has been deleted")
					}

					var newConfig ElasticConsumerGroupConfig
					err := json.Unmarshal(updateMsg.Value(), &newConfig)
					if err != nil {
						// Human error is very possible if they put the config messages directly (e.g. using `nats kv put`)
						// Best to ignore or better to error out?
						log.Printf("Error:  consumer group %s config watcher received and is ignoring a bad JSON message", composeCGSName(streamName, memberName))
						break
					}

					err = validateConfig(newConfig)
					if err != nil {
						// same question ignore or error out?
						log.Printf("Error:  consumer group %s config watcher received and is ignoring an invalid config", composeCGSName(streamName, memberName))
						break
					}

					if newConfig.MaxMembers != instance.Config.MaxMembers ||
						newConfig.Filter != instance.Config.Filter || newConfig.MaxBufferedMsgs != instance.Config.MaxBufferedMsgs || newConfig.MaxBufferedBytes != instance.Config.MaxBufferedBytes ||
						!reflect.DeepEqual(newConfig.PartitioningWildcards, instance.Config.PartitioningWildcards) {
						log.Printf(" consumer group %s config watcher received a change in the configuration, terminating", composeCGSName(streamName, memberName))
						instance.stop()
						return errors.New(" consumer group config max number of members, buffered messages, filter or partitioning wildcards changed")
					}
					// new config looks ok to use

					// optimization if nothing changed and already have the consumer for that member
					if instance.Consumer != nil && reflect.DeepEqual(newConfig.Members, instance.Config.Members) && reflect.DeepEqual(newConfig.MemberMappings, instance.Config.MemberMappings) {
						break
					}

					instance.Config.Members = newConfig.Members
					instance.Config.MemberMappings = newConfig.MemberMappings
					instance.processMembershipChange(ctx)
				}
			}
		case <-ctx.Done():
			instance.stop()
			return nil
		case <-time.After(time.Second * 15):
			if instance.Consumer == nil && instance.Config.IsInMembership(instance.MemberName) {
				instance.joinMemberConsumer(ctx)
			}
		}
	}
}

// CreateElastic creates an elastic consumer group
// Creates the sourcing stream that is going to be used by the members to consume from
func CreateElastic(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string, maxNumMembers uint, filter string, partitioningWildcards []int, maxBufferedMessages int64, maxBufferedBytes int64) (*ElasticConsumerGroupConfig, error) {
	config := ElasticConsumerGroupConfig{
		MaxMembers:            maxNumMembers,
		Filter:                filter,
		PartitioningWildcards: partitioningWildcards,
		MaxBufferedMsgs:       maxBufferedMessages,
		MaxBufferedBytes:      maxBufferedBytes,
	}

	err := validateConfig(config)
	if err != nil {
		return nil, errors.Join(errors.New("invalid consumer group config"), err)
	}

	filterDest := getPartitioningTransformDest(config)

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
		// check that the current entry matches the max number of members (i.e. number of partitions) and filter
		err = json.Unmarshal(value.Value(), &consumerGroupConfig)
		if err != nil {
			return nil, err
		}

		if consumerGroupConfig.MaxMembers != maxNumMembers || consumerGroupConfig.Filter != filter || !slices.Equal(consumerGroupConfig.PartitioningWildcards, partitioningWildcards) {
			return nil, errors.New("the existing consumer group config doesn't match ours")
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
		return nil, errors.Join(errors.New("can't create the consumer group's stream"), err)
	}

	return &consumerGroupConfig, nil
}

// DeleteElastic Deletes a consumer group
func DeleteElastic(ctx context.Context, nc *nats.Conn, streamName string, consumerGroupName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream instance: %v", err)
	}

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return err
	}

	// First delete the PCG's entry in the KV bucket
	err = kv.Delete(ctx, composeKey(streamName, consumerGroupName))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return errors.New("error deleting the consumer groups' configs")
	}

	// Then delete the PCG's stream
	err = js.DeleteStream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return errors.Join(errors.New("could not delete the consumer group's stream"), err)
	}

	return nil
}

// ListElasticConsumerGroups lists the consumer groups for a given stream
func ListElasticConsumerGroups(nc *nats.Conn, streamName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	lister, _ := kv.ListKeys(ctx)
	if err != nil {
		return nil, errors.Join(errors.New("error creating a key lister on the consumer groups' bucket"), err)
	}

	var consumerGroupNames []string

	for {
		select {
		case key, ok := <-lister.Keys():
			if ok {
				parts := strings.Split(key, ".")
				if parts[0] == streamName {
					consumerGroupNames = append(consumerGroupNames, parts[1])
				}
			} else {
				return consumerGroupNames, nil
			}
		}
	}
}

// AddMembers adds members to a consumer group
func AddMembers(nc *nats.Conn, streamName string, consumerGroupName string, memberNamesToAdd []string) ([]string, error) {
	if streamName == "" || consumerGroupName == "" || len(memberNamesToAdd) == 0 {
		return nil, errors.New("invalid stream name or consumer group name or no member names")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, errors.Join(errors.New("can not get the current consumer group's config"), err)
	}

	var existingMembers = make(map[string]struct{})

	if len(consumerGroupConfig.MemberMappings) != 0 {
		return nil, errors.New("can't add members to a consumer group that uses member mappings")
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

	marshalled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("couldn't marshall the consumer group's config"), err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshalled)
	if err != nil {
		return nil, errors.Join(errors.New("couldn't put the consumer group's config in the bucket"), err)
	}

	return consumerGroupConfig.Members, nil
}

// DeleteMembers drops members from a consumer group
func DeleteMembers(nc *nats.Conn, streamName string, consumerGroupName string, memberNamesToDrop []string) ([]string, error) {
	if streamName == "" || consumerGroupName == "" || len(memberNamesToDrop) == 0 {
		return nil, errors.New("invalid stream name or consumer group name or no member names")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	// Get or create the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, errors.Join(errors.New("can not get the current consumer group's config"), err)
	}

	var droppingMembers = make(map[string]struct{})

	if len(consumerGroupConfig.MemberMappings) != 0 {
		return nil, errors.New("can't drop members from a consumer group that uses member mappings")
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

	marshalled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("couldn't marshall the consumer group's config"), err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshalled)
	if err != nil {
		return nil, errors.Join(errors.New("couldn't put the consumer group's config in the bucket"), err)
	}

	return consumerGroupConfig.Members, nil
}

// SetMemberMappings sets the custom member mappings for a consumer group
func SetMemberMappings(nc *nats.Conn, streamName string, consumerGroupName string, memberMappings []MemberMapping) error {
	if streamName == "" || consumerGroupName == "" || len(memberMappings) == 0 {
		return errors.New("invalid stream name or consumer group name or member mappings")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return errors.Join(errors.New("can not get the current consumer group's config"), err)
	}

	if len(consumerGroupConfig.Members) != 0 {
		consumerGroupConfig.Members = []string{}
	}

	consumerGroupConfig.MemberMappings = memberMappings

	err = validateConfig(*consumerGroupConfig)
	if err != nil {
		return errors.Join(errors.New("invalid consumer group config"), err)
	}

	marshalled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return errors.Join(errors.New("couldn't marshall the consumer group's config"), err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshalled)
	if err != nil {
		return errors.Join(errors.New("couldn't put the consumer group's config in the bucket"), err)
	}

	return nil
}

// DeleteMemberMappings deletes the custom member mappings for a consumer group
func DeleteMemberMappings(nc *nats.Conn, streamName string, consumerGroupName string) error {
	if streamName == "" || consumerGroupName == "" {
		return errors.New("invalid stream name or consumer group name or member mappings")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return errors.Join(errors.New("the consumer group KV bucket doesn't exist"), err)
	}

	// Get the consumer group config
	consumerGroupConfig, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return errors.Join(errors.New("can not get the current consumer group's config"), err)
	}

	consumerGroupConfig.MemberMappings = []MemberMapping{}

	marshalled, err := json.Marshal(consumerGroupConfig)
	if err != nil {
		return errors.Join(errors.New("couldn't marshall the consumer group's config"), err)
	}

	// update the config record
	_, err = kv.Put(ctx, composeKey(streamName, consumerGroupName), marshalled)
	if err != nil {
		return errors.Join(errors.New("couldn't put the consumer group's config in the bucket"), err)
	}

	return nil
}

func ListElasticActiveMembers(nc *nats.Conn, streamName string, consumerGroupName string) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	kv, err := js.KeyValue(ctx, kvElasticBucketName)
	if err != nil {
		return nil, err
	}

	var activeMembers []string

	cGC, err := getElasticConsumerGroupConfig(ctx, kv, streamName, consumerGroupName)
	if err != nil {
		return nil, err
	}

	s, err := js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return nil, err
	}

	lister := s.ListConsumers(ctx)
	for {
		select {
		case cInfo, ok := <-lister.Info():
			if ok {
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
			} else {
				return activeMembers, nil
			}
		}
	}
}

// ElasticIsInMembershipAndActive checks if a member is included in the consumer group and is active
func ElasticIsInMembershipAndActive(nc *nats.Conn, streamName string, consumerGroupName string, memberName string) (bool, bool, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return false, false, err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

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

	lister := s.ListConsumers(ctx)
	for {
		select {
		case cInfo, ok := <-lister.Info():
			if ok {
				if cmp.Compare(cInfo.Name, memberName) == 0 {
					return inMembership, true, nil
				}
			} else {
				return inMembership, false, nil
			}
		}
	}
}

// ElasticMemberStepDown forces the current active instance of a member to step down
func ElasticMemberStepDown(nc *nats.Conn, streamName string, consumerGroupName string, memberName string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	s, err := js.Stream(ctx, composeCGSName(streamName, consumerGroupName))
	if err != nil {
		return err
	}

	err = s.UnpinConsumer(ctx, memberName, memberName)
	if err != nil {
		log.Printf("Error trying to unpin our member's consumer: %v\n", err)
		return err
	}
	return nil
}

// ElasticGetPartitionFilters For the given ElasticConsumerGroupConfig returns the list of partition filters for a given member
func ElasticGetPartitionFilters(config ElasticConsumerGroupConfig, memberName string) []string {
	return GeneratePartitionFilters(config.Members, config.MaxMembers, config.MemberMappings, memberName)
}

// Shim callback function to strip the partition number from the subject before passing the message to the user's callback
func (consumerInstance *ElasticConsumerGroupConsumerInstance) consumerCallback(msg jetstream.Msg) {
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

// CreateElastic the member's consumer if the member is in the current list of members
func (consumerInstance *ElasticConsumerGroupConsumerInstance) joinMemberConsumer(ctx context.Context) {
	var err error

	filters := ElasticGetPartitionFilters(*consumerInstance.Config, consumerInstance.MemberName)

	// if we are no longer in the membership list, nothing to do
	if len(filters) == 0 {
		return
	}

	config := consumerInstance.CConfig
	config.Durable = ""
	config.Name = consumerInstance.MemberName
	config.FilterSubjects = filters

	config.AckPolicy = jetstream.AckExplicitPolicy

	if config.InactiveThreshold == 0 {
		config.InactiveThreshold = 12 * time.Second
	}

	if config.AckWait == 0 {
		config.AckWait = 6 * time.Second
	}

	config.PriorityGroups = []string{consumerInstance.MemberName}
	config.PriorityPolicy = jetstream.PriorityPolicyPinned
	config.PinnedTTL = config.AckWait

	consumerInstance.Consumer, err = consumerInstance.js.CreateOrUpdateConsumer(ctx, composeCGSName(consumerInstance.StreamName, consumerInstance.ConsumerGroupName), config)
	if err != nil {
		// not logging anything here as this is expected to happen in many recoverable failure scenarios
		return
	}

	consumerInstance.startConsuming()
}

// when instance becomes the active one
func (consumerInstance *ElasticConsumerGroupConsumerInstance) startConsuming() {
	var err error

	consumerInstance.ConsumerConsumeContext, err = consumerInstance.Consumer.Consume(consumerInstance.consumerCallback, jetstream.PullExpiry(3*time.Second), jetstream.PullPriorityGroup(consumerInstance.MemberName))
	if err != nil {
		log.Printf("Error starting to consume on my consumer: %v\n", err)
		return
	}

}

func (consumerInstance *ElasticConsumerGroupConsumerInstance) processMembershipChange(ctx context.Context) {
	// there's a membership change

	// get the current consumer info to get the current pinned-id
	var isPinned bool

	if consumerInstance.Consumer != nil {
		ci, err := consumerInstance.Consumer.Info(ctx)
		if err == nil { // ignoring error ad the consumer may not exist yet
			if slices.ContainsFunc(ci.PriorityGroups, func(pg jetstream.PriorityGroupState) bool {
				if pg.Group == consumerInstance.MemberName && pg.PinnedClientID == consumerInstance.CurrentPID {
					return true
				}
				return false
			}) {
				isPinned = true
			}
		}
		consumerInstance.stop()
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
		err := consumerInstance.js.DeleteConsumer(ctx, composeCGSName(consumerInstance.StreamName, consumerInstance.ConsumerGroupName), consumerInstance.MemberName)
		if err != nil {
			if !errors.Is(err, jetstream.ErrConsumerNotFound) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf("Error trying to delete our member's consumer: %v\n", err)
			}
		}
	} else {
		time.Sleep(250 * time.Millisecond)
	}

	consumerInstance.joinMemberConsumer(ctx)
}

func (consumerInstance *ElasticConsumerGroupConsumerInstance) stop() {
	if consumerInstance.Consumer != nil {
		if consumerInstance.ConsumerConsumeContext != nil {
			consumerInstance.ConsumerConsumeContext.Stop()
			consumerInstance.ConsumerConsumeContext = nil
		}

		consumerInstance.Consumer = nil
	}
}

func validateConfig(config ElasticConsumerGroupConfig) error {
	// First validate the max number of members
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

	pwcs := make(map[int]any) // partitioning wildcard presence

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

func getPartitioningTransformDest(config ElasticConsumerGroupConfig) string {
	wildcards := make([]string, len(config.PartitioningWildcards))

	for i, wc := range config.PartitioningWildcards {
		wildcards[i] = strconv.Itoa(wc)
	}

	wildcardList := strings.Join(wildcards, ",")
	var destFromFilter string
	cwIndex := 1
	filterTokens := strings.Split(config.Filter, ".")

	for i, s := range filterTokens {
		if s == "*" {
			filterTokens[i] = fmt.Sprintf("{{Wildcard(%d)}}", cwIndex)
			cwIndex++
		}
	}

	destFromFilter = strings.Join(filterTokens, ".")
	return fmt.Sprintf("{{Partition(%d,%s)}}.%s", config.MaxMembers, wildcardList, destFromFilter)
}

func getElasticConsumerGroupConfig(ctx context.Context, kv jetstream.KeyValue, streamName string, consumerGroupName string) (*ElasticConsumerGroupConfig, error) {
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

	var consumerGroupConfig ElasticConsumerGroupConfig

	err = json.Unmarshal(message.Value(), &consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("invalid JSON value for the consumer group's config"), err)
	}

	err = validateConfig(consumerGroupConfig)
	if err != nil {
		return nil, errors.Join(errors.New("invalid consumer group config"), err)
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
