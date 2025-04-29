package streamconsumergroup

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"slices"
	"time"
)

// Contains the things that are common to all types of consumer-groups

type MemberMapping struct {
	Member     string `json:"member"`
	Partitions []int  `json:"partitions"`
}

type ConsumerGroupMsg struct {
	msg jetstream.Msg
}

// Compose the consumer group's config key name
func composeKey(streamName string, consumerGroupName string) string {
	return streamName + "." + consumerGroupName
}

// jetstream.Msg shim to strip the partition number from the subject
func newConsumerGroupMsg(msg jetstream.Msg) *ConsumerGroupMsg {
	return &ConsumerGroupMsg{msg: msg}
}

func GeneratePartitionFilters(members []string, maxMembers uint, memberMappings []MemberMapping, memberName string) []string {
	if len(members) != 0 {
		members := deduplicateStringSlice(members)
		slices.Sort(members)

		if uint(len(members)) > maxMembers {
			members = members[:maxMembers]
		}

		// Distribute the partitions amongst the members trying to minimize the number of partitions getting re-distributed
		// to another member as the number of members increases/decreases
		numMembers := uint(len(members))

		if numMembers > 0 {
			// rounded number of partitions per member
			var numPer = maxMembers / numMembers
			var myFilters []string

			for i := uint(0); i < maxMembers; i++ {
				var memberIndex = i / numPer

				if i < (numMembers * numPer) {
					if members[memberIndex%numMembers] == memberName {
						myFilters = append(myFilters, fmt.Sprintf("%d.>", i))
					}
				} else {
					// remainder if the number of partitions is not a multiple of the number of members
					if members[(i-(numMembers*numPer))%numMembers] == memberName {
						myFilters = append(myFilters, fmt.Sprintf("%d.>", i))
					}
				}
			}

			return myFilters
		}
		return []string{}
	} else if len(memberMappings) != 0 {
		var myFilters []string

		for _, mapping := range memberMappings {
			if mapping.Member == memberName {
				for _, pn := range mapping.Partitions {
					myFilters = append(myFilters, fmt.Sprintf("%d.>", pn))
				}
			}
		}

		return myFilters
	}
	return []string{}
}

func (scgMsg *ConsumerGroupMsg) Metadata() (*jetstream.MsgMetadata, error) {

	return scgMsg.msg.Metadata()
}

// Data returns the message body
func (scgMsg *ConsumerGroupMsg) Data() []byte {
	return scgMsg.msg.Data()
}

// Headers returns a map of headers for a message
func (scgMsg *ConsumerGroupMsg) Headers() nats.Header {
	return scgMsg.msg.Headers()
}

// Subject returns a subject on which a message is published
func (scgMsg *ConsumerGroupMsg) Subject() string {
	// strips the first token of the subject (it contains the partition number)
	subject := scgMsg.msg.Subject()
	for i := range subject {
		if subject[i] == '.' {
			return scgMsg.msg.Subject()[i+1:]
		}
	}
	return subject
}

// Reply returns a reply subject for a message
func (scgMsg *ConsumerGroupMsg) Reply() string {
	return scgMsg.msg.Reply()
}

// Ack acknowledges a message
// This tells the server that the message was successfully processed, and it can move on to the next message
func (scgMsg *ConsumerGroupMsg) Ack() error {
	return scgMsg.msg.Ack()

}

// DoubleAck acknowledges a message and waits for ack from server
func (scgMsg *ConsumerGroupMsg) DoubleAck(ctx context.Context) error {
	return scgMsg.msg.DoubleAck(ctx)
}

// Nak negatively acknowledges a message
// This tells the server to redeliver the message
func (scgMsg *ConsumerGroupMsg) Nak() error {
	return scgMsg.msg.Nak()
}

// NakWithDelay negatively acknowledges a message
// This tells the server to redeliver the message
// after the given `delay` duration
func (scgMsg *ConsumerGroupMsg) NakWithDelay(delay time.Duration) error {
	return scgMsg.msg.NakWithDelay(delay)
}

// InProgress tells the server that this message is being worked on
// It resets the redelivery timer on the server
func (scgMsg *ConsumerGroupMsg) InProgress() error {
	return scgMsg.msg.InProgress()
}

// Term tells the server to not redeliver this message, regardless of the value of nats.MaxDeliver
func (scgMsg *ConsumerGroupMsg) Term() error {
	return scgMsg.msg.Term()
}

func (scgMsg *ConsumerGroupMsg) TermWithReason(reason string) error {
	return scgMsg.msg.TermWithReason(reason)
}
