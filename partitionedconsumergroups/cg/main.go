package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"os"
	"partitioned_stream_consumer_test/pkg/streamconsumergroup"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cgStruct struct {
	streamName         string
	consumerGroupName  string
	memberName         string
	memberNames        []string
	memberMappingArgs  []string
	processingDuration time.Duration
	filter             string
	maxMembers         uint
	maxBufferedMsgs    int64
	maxBufferedBytes   int64
	pwcis              []int // partitioning wildcard indexes
	consuming          bool
	myContextCancel    context.CancelFunc
	natsContext        string
	consumerStatic     bool
	force              bool
	nc                 *nats.Conn
	prompt             bool
	qch                sync.WaitGroup
}

var (
	cg cgStruct
)

func (cg *cgStruct) static() {
	cg.consumerStatic = true
}

func (cg *cgStruct) elastic() {
	cg.consumerStatic = false
}

func (cg *cgStruct) setProcessingTime(processingTimeInput string) (time.Duration, error) {
	var err error
	cg.processingDuration, err = time.ParseDuration(processingTimeInput)
	if err != nil {
		return 0, err
	}
	return cg.processingDuration, nil
}

func (cg *cgStruct) lsStaticAction(_ *fisk.ParseContext) error {
	groups, err := streamconsumergroup.ListStaticConsumerGroups(cg.nc, cg.streamName)
	if err != nil {
		return err
	}

	fmt.Printf("static consumer groups: %+v\n", groups)
	return nil
}

func (cg *cgStruct) lsElasticAction(_ *fisk.ParseContext) error {
	groups, err := streamconsumergroup.ListElasticConsumerGroups(cg.nc, cg.streamName)
	if err != nil {
		return err
	}

	fmt.Printf("elastic consumer groups: %+v\n", groups)
	return nil
}

func (cg *cgStruct) infoStaticAction(_ *fisk.ParseContext) error {
	config, err := streamconsumergroup.GetStaticConsumerGroupConfig(cg.nc, cg.streamName, cg.consumerGroupName)
	if err != nil {
		return err
	} else {
		fmt.Printf("config: max members=%d, filter=%s\n", config.MaxMembers, config.Filter)
		if len(config.Members) != 0 {
			fmt.Printf("members: %+v\n", config.Members)
		} else if len(config.MemberMappings) != 0 {
			fmt.Printf("Member mappings: %+v\n", config.MemberMappings)
		} else {
			fmt.Printf("no members or mappings defined\n")
		}
		activeMembers, err := streamconsumergroup.ListStaticActiveMembers(cg.nc, cg.streamName, cg.consumerGroupName)
		if err != nil {
			return err
		}
		fmt.Printf("currently active members: %+v\n", activeMembers)
		return nil
	}
}

func (cg *cgStruct) infoElasticAction(_ *fisk.ParseContext) error {
	config, err := streamconsumergroup.GetElasticConsumerGroupConfig(cg.nc, cg.streamName, cg.consumerGroupName)
	if err != nil {
		return err
	} else {
		fmt.Printf("config: max members=%d, filter=%s, partitioning wildcards %+v\n", config.MaxMembers, config.Filter, config.PartitioningWildcards)
		if len(config.Members) != 0 {
			fmt.Printf("members: %+v\n", config.Members)
		} else if len(config.MemberMappings) != 0 {
			fmt.Printf("Member mappings: %+v\n", config.MemberMappings)
		} else {
			fmt.Printf("no members or mappings defined\n")
		}
		activeMembers, err := streamconsumergroup.ListElasticActiveMembers(cg.nc, cg.streamName, cg.consumerGroupName)
		if err != nil {
			return err
		}
		fmt.Printf("currently active members: %+v\n", activeMembers)
		return nil
	}
}

func (cg *cgStruct) addElasticAction(_ *fisk.ParseContext) error {
	members, err := streamconsumergroup.AddMembers(cg.nc, cg.streamName, cg.consumerGroupName, cg.memberNames)

	if err != nil {
		return err
	} else {
		fmt.Printf("added members: %+v\n", members)
		return nil
	}
}

func (cg *cgStruct) dropElasticAction(_ *fisk.ParseContext) error {
	members, err := streamconsumergroup.DeleteMembers(cg.nc, cg.streamName, cg.consumerGroupName, cg.memberNames)

	if err != nil {
		return err
	} else {
		fmt.Printf("dropped members: %+v\n", members)
		return nil
	}
}

func (cg *cgStruct) createStaticBalancedAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	_, err := streamconsumergroup.CreateStatic(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.maxMembers, cg.filter, cg.memberNames, []streamconsumergroup.MemberMapping{})
	if err != nil {
		return err
	}

	return nil
}

func (cg *cgStruct) createStaticMappedAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	memberMappings, err := parseMemberMappings(cg.memberMappingArgs)

	_, err = streamconsumergroup.CreateStatic(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.maxMembers, cg.filter, []string{}, memberMappings)
	if err != nil {
		return err
	}

	return nil
}

func (cg *cgStruct) createElasticAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	_, err := streamconsumergroup.CreateElastic(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.maxMembers, cg.filter, cg.pwcis, int64(cg.maxBufferedMsgs), int64(cg.maxBufferedBytes))
	if err != nil {
		return err
	}

	return nil
}

func (cg *cgStruct) deleteStaticAction(_ *fisk.ParseContext) error {
	if !cg.force {
		fmt.Print("WARNING: this operation will cause all existing consumer members to terminate consuming are you sure? (y/n): ")
		var confirmation string
		_, _ = fmt.Scanln(&confirmation)

		if confirmation != "y" {
			return errors.New("operation cancelled")
		}
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	err := streamconsumergroup.DeleteStatic(ctx, cg.nc, cg.streamName, cg.consumerGroupName)
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (cg *cgStruct) deleteElasticAction(_ *fisk.ParseContext) error {
	if !cg.force {
		fmt.Print("WARNING: this operation will cause all existing consumer members to terminate consuming are you sure? (y/n): ")
		var confirmation string
		_, _ = fmt.Scanln(&confirmation)

		if confirmation != "y" {
			return errors.New("operation cancelled")
		}
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	err := streamconsumergroup.DeleteElastic(ctx, cg.nc, cg.streamName, cg.consumerGroupName)
	cancel()
	if err != nil {
		return err
	}
	return nil
}

func (cg *cgStruct) createElasticMappingAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	var config *streamconsumergroup.ElasticConsumerGroupConfig
	config, err := streamconsumergroup.GetElasticConsumerGroupConfig(cg.nc, cg.streamName, cg.consumerGroupName)
	if err != nil {
		return err
	}

	config.MemberMappings, err = parseMemberMappings(cg.memberMappingArgs)
	err = streamconsumergroup.SetMemberMappings(cg.nc, cg.streamName, cg.consumerGroupName, config.MemberMappings)
	if err != nil {
		return err
	}

	fmt.Printf("member mapping: %+v\n", config.MemberMappings)
	return nil
}

func (cg *cgStruct) deleteElasticMappingAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	err := streamconsumergroup.DeleteMemberMappings(cg.nc, cg.streamName, cg.consumerGroupName)

	return err
}

func (cg *cgStruct) memberStaticInfoAction(_ *fisk.ParseContext) error {
	cgConfig, err := streamconsumergroup.GetStaticConsumerGroupConfig(cg.nc, cg.streamName, cg.consumerGroupName)
	if err != nil {
		return err
	}

	actives, err := streamconsumergroup.ListStaticActiveMembers(cg.nc, cg.streamName, cg.consumerGroupName)
	if err != nil {
		return err
	}

	if cgConfig.IsInMembership(cg.memberName) {
		fmt.Printf("member %s is part of the consumer group membership\n", cg.memberName)
		if slices.Contains(actives, cg.memberName) {
			fmt.Printf("member %s is active\n", cg.memberName)
		} else {
			fmt.Printf("***Warning*** member %s is part of the consumer group membership but has NO active instance\n", cg.memberName)
		}
	} else {
		fmt.Printf("member %s is not part of the consumer group membership\n", cg.memberName)
	}
	return nil
}

func (cg *cgStruct) memberElasticInfoAction(_ *fisk.ParseContext) error {
	member, active, err := streamconsumergroup.ElasticIsInMembershipAndActive(cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName)
	if err != nil {
		fmt.Printf("can't list active members: %v\n", err)
	}

	if member {
		if active {
			fmt.Printf("member %s is part of the consumer group membership and is active\n", cg.memberName)
		} else {
			fmt.Printf("member %s is part of the consumer group membership\n", cg.memberName)
			fmt.Printf("***Warning*** member %s is part of the consumer group membership but has NO active instance\n", cg.memberName)
		}
	} else {
		fmt.Printf("member %s is not currently part of the consumer group membership\n", cg.memberName)
	}
	return nil
}

func (cg *cgStruct) stepDownStaticAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	err := streamconsumergroup.StaticMemberStepDown(cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName)
	if err != nil {
		return err
	}

	return nil
}

func (cg *cgStruct) stepDownElasticAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)

	err := streamconsumergroup.ElasticMemberStepDown(cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName)
	if err != nil {
		return err
	}

	return nil
}

func (cg *cgStruct) consumeStaticAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)
	cg.static()
	cg.consume(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName, messageHandler(cg.processingDuration), 1)
	return nil
}

func (cg *cgStruct) consumeElasticAction(_ *fisk.ParseContext) error {
	myContext := context.Background()
	myContext, cg.myContextCancel = context.WithCancel(myContext)
	cg.elastic()
	cg.consume(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName, messageHandler(cg.processingDuration), 1)
	return nil
}

// Example of consuming messages.
// Can be used to demonstrate the consumer group functionality using this CLI tool.
func (cg *cgStruct) consume(myContext context.Context, nc *nats.Conn, streamName string, consumerGroupName string, memberName string, messageHandler func(msg jetstream.Msg), maxAcksPending int) {
	cg.consuming = true
	var err error
	config := jetstream.ConsumerConfig{
		MaxAckPending: maxAcksPending,
		AckWait:       6 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	go func() {
		cg.qch.Add(1)

		if cg.consumerStatic {
			err = streamconsumergroup.StaticConsume(myContext, nc, streamName, consumerGroupName, memberName, messageHandler, config)
		} else {
			err = streamconsumergroup.ElasticConsume(myContext, nc, streamName, consumerGroupName, memberName, messageHandler, config)
		}
		if err != nil {
			log.Printf("could not join or remain in the consumer group: %v\n", err)
		}
		time.Sleep(1 * time.Second)
		cg.qch.Done()
	}()
}

// Example callback, waits for 'processing time' and acks the message.
func messageHandler(processingTime time.Duration) func(msg jetstream.Msg) {
	return func(msg jetstream.Msg) {
		pid := msg.Headers().Get("Nats-Pin-Id")
		var seqNumber uint64
		mmd, err := msg.Metadata()
		if err != nil {
			fmt.Printf("can't get message metadata: %v", err)
		}
		seqNumber = mmd.Sequence.Stream
		fmt.Printf("[%s] received a message on (subject=%s, seq=%d, pinnedID=%s). Processing ... ", cg.memberName, msg.Subject(), seqNumber, pid)
		time.Sleep(processingTime)
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		err = msg.DoubleAck(ctx)
		if err != nil {
			log.Printf("message could not be acked! (it will be or may already have been re-delivered): %+v\n", err)
		} else {
			log.Printf("acked\n")
		}
	}
}

func (cg *cgStruct) promptAction(_ *fisk.ParseContext) error {
	cg.prompt = true
	return nil
}

func (cg *cgStruct) promptStaticAction(_ *fisk.ParseContext) error {
	cg.static()
	cg.prompt = true
	return nil
}

func main() {

	app := fisk.New("cg", "Partitioned Consumer Group CLI tool")
	app.Version("0.1.0")
	app.HelpFlag.Short('h')
	app.Flag("context", "nats CLI context to use").StringVar(&cg.natsContext)

	addCommonArgs := func(f *fisk.CmdClause) {
		f.Arg("stream", "stream name").Required().StringVar(&cg.streamName)
		f.Arg("name", "consumer group name").Required().StringVar(&cg.consumerGroupName)
	}

	// I had the prompt command as the default (so if you didn't pass any command it would just go to the prompt)
	// But currently with Fisk doing that breaks the ability to work as a plugin
	// So commenting out for the moment
	//fisk.Command("prompt", "interactive prompt").Action(cg.promptAction).Default()

	staticCommand := app.Command("static", "static consumer groups mode")
	elasticCommand := app.Command("elastic", "elastic consumer groups mode")

	staticCommand.Command("prompt", "interactive prompt").Action(cg.promptStaticAction)
	elasticCommand.Command("prompt", "interactive prompt").Action(cg.promptAction)

	//
	// Static
	//

	lsStaticCommand := staticCommand.Command("ls", "list the consumer groups defined on a stream").Action(cg.lsStaticAction)
	lsStaticCommand.Alias("list")
	lsStaticCommand.Arg("stream", "stream name").Required().StringVar(&cg.streamName)

	infoStaticCommand := staticCommand.Command("info", "get static consumer group info").Action(cg.infoStaticAction)
	addCommonArgs(infoStaticCommand)

	staticCreateCommand := staticCommand.Command("create", "create a static partitioned consumer group")
	staticCreateBalancedCommand := staticCreateCommand.Command("balanced", "create a static partitioned consumer group with balanced members").Action(cg.createStaticBalancedAction)
	addCommonArgs(staticCreateBalancedCommand)
	staticCreateBalancedCommand.Arg("max-members", "max number of members").Required().UintVar(&cg.maxMembers)
	staticCreateBalancedCommand.Arg("filter", "filter").Required().StringVar(&cg.filter)
	staticCreateBalancedCommand.Arg("members", "member names").Required().StringsVar(&cg.memberNames)

	staticCreateMappedCommand := staticCreateCommand.Command("mapped", "create a static partitioned consumer group with member mappings").Action(cg.createStaticMappedAction)
	addCommonArgs(staticCreateMappedCommand)
	staticCreateMappedCommand.Arg("max-members", "max number of members").Required().UintVar(&cg.maxMembers)
	staticCreateMappedCommand.Arg("filter", "filter").Required().StringVar(&cg.filter)
	staticCreateMappedCommand.Arg("mappings", "mappings of members to partition numbers in the format <member>:<partition1>,<partition2>,...").Required().StringsVar(&cg.memberMappingArgs)

	staticDeleteCommand := staticCommand.Command("delete", "delete a static partitioned consumer group").Alias("rm").Action(cg.deleteStaticAction)
	addCommonArgs(staticDeleteCommand)
	staticDeleteCommand.Flag("force", "force delete the consumer group").Short('f').BoolVar(&cg.force)

	staticMemberInfoCommand := staticCommand.Command("member-info", "get static consumer group member info").Alias("memberinfo").Alias("minfo").Action(cg.memberStaticInfoAction)
	addCommonArgs(staticMemberInfoCommand)
	staticMemberInfoCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)

	staticStepDownCommand := staticCommand.Command("step-down", "initiate a step down for a member").Alias("stepdown").Alias("sd").Action(cg.stepDownStaticAction)
	addCommonArgs(staticStepDownCommand)
	staticStepDownCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)

	staticConsumeCommand := staticCommand.Command("consume", "join a static partitioned consumer group").Alias("join").Action(cg.consumeStaticAction)
	addCommonArgs(staticConsumeCommand)
	staticConsumeCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)
	staticConsumeCommand.Flag("sleep", "sleep to simulate processing time").Default("20ms").DurationVar(&cg.processingDuration)

	//
	// Elastic
	//

	lsElasticCommand := elasticCommand.Command("ls", "list the consumer groups defined on a stream").Action(cg.lsElasticAction)
	lsElasticCommand.Alias("list")
	lsElasticCommand.Arg("stream", "stream name").Required().StringVar(&cg.streamName)

	infoElesticCommand := elasticCommand.Command("info", "get elastic consumer group info").Action(cg.infoElasticAction)
	addCommonArgs(infoElesticCommand)

	elasticCreateCommand := elasticCommand.Command("create", "create an elastic partitioned consumer group").Action(cg.createElasticAction)
	addCommonArgs(elasticCreateCommand)
	elasticCreateCommand.Arg("max-members", "max number of members").Required().UintVar(&cg.maxMembers)
	elasticCreateCommand.Arg("filter", "filter").Required().StringVar(&cg.filter)
	elasticCreateCommand.Arg("partitioning wildcard indexes", "list of partitioning wildcard indexes").Required().IntsVar(&cg.pwcis)
	elasticCreateCommand.Flag("max-buffered-msgs", "max number of buffered messages").Default("0").Int64Var(&cg.maxBufferedMsgs)
	elasticCreateCommand.Flag("max-buffered-bytes", "max number of buffered bytes").Default("0").Int64Var(&cg.maxBufferedBytes)

	elasticDeleteCommand := elasticCommand.Command("delete", "delete an elastic partitioned consumer group").Alias("rm").Action(cg.deleteElasticAction)
	addCommonArgs(elasticDeleteCommand)
	elasticDeleteCommand.Flag("force", "force delete the consumer group").Short('f').BoolVar(&cg.force)

	addCommand := elasticCommand.Command("add", "add members to a partitioned consumer group").Action(cg.addElasticAction)
	addCommonArgs(addCommand)
	addCommand.Arg("members", "member names").Required().StringsVar(&cg.memberNames)

	dropCommand := elasticCommand.Command("drop", "drop members from a partitioned consumer group").Action(cg.dropElasticAction)
	addCommonArgs(dropCommand)
	dropCommand.Arg("members", "member names").Required().StringsVar(&cg.memberNames)

	createMappingCommand := elasticCommand.Command("create-mapping", "create member mappings for a partitioned consumer group").Action(cg.createElasticMappingAction)
	createMappingCommand.Alias("cm").Alias("createmapping")
	addCommonArgs(createMappingCommand)
	createMappingCommand.Arg("mappings", "mappings of members to partition numbers in the format <member>:<partition1>,<partition2>,...").Required().StringsVar(&cg.memberMappingArgs)

	deleteMappingCommand := elasticCommand.Command("delete-mapping", "delete member mappings for a partitioned consumer group").Action(cg.deleteElasticMappingAction)
	deleteMappingCommand.Alias("dm").Alias("deletemapping")
	addCommonArgs(deleteMappingCommand)

	elasticMemberInfoCommand := elasticCommand.Command("member-info", "get elastic consumer group member info").Alias("memberinfo").Alias("minfo").Action(cg.memberElasticInfoAction)
	addCommonArgs(elasticMemberInfoCommand)
	elasticMemberInfoCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)

	elasticStepDownCommand := elasticCommand.Command("step-down", "initiate a step down for a member").Alias("stepdown").Alias("sd").Action(cg.stepDownElasticAction)
	addCommonArgs(elasticStepDownCommand)
	elasticStepDownCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)

	elasticConsumeCommand := elasticCommand.Command("consume", "join a partitioned consumer group").Alias("join").Action(cg.consumeElasticAction)
	addCommonArgs(elasticConsumeCommand)
	elasticConsumeCommand.Arg("member", "member name").Required().StringVar(&cg.memberName)
	elasticConsumeCommand.Flag("sleep", "sleep to simulate processing time").Default("20ms").DurationVar(&cg.processingDuration)

	var err error

	cg.nc, err = natscontext.Connect(cg.natsContext)
	if err != nil {
		log.Fatalf("can't connect using nats CLI context %s %v", cg.natsContext, err)
	}
	// auto start consuming if all required flags are set
	app.MustParseWithUsage(os.Args[1:])
	// fisk.Parse()

	if cg.consuming {
		fmt.Println("consuming...")
		cg.qch.Wait()
	}

	if cg.prompt {
		prompt()
	}
}

func prompt() {
	// wait for user input to exit

	r := bufio.NewReader(os.Stdin)

	for {
		if cg.consumerStatic {
			fmt.Print("[static]")
		} else {
			fmt.Print("[elastic]")
		}
		if cg.consuming {
			fmt.Printf("[%s/%s/%s]> ", cg.streamName, cg.consumerGroupName, cg.memberName)
		} else {
			fmt.Print("> ")
		}

		input, err := r.ReadString('\n')
		if err != nil {
			log.Fatalf("can't read input: %v", err)
		}

		// trim newline and get the args if any
		input = strings.TrimSuffix(input, "\n")
		command, argsString, ok := strings.Cut(input, " ")
		var args []string

		if ok {
			args = strings.Split(argsString, " ")
		}

		switch command {
		case "?", "help":
			fmt.Println("Available commands:")
			fmt.Println("exit/quit - exit the program")
			fmt.Println("list/ls <stream name> - list partitioned consumer groups")
			fmt.Println("info <stream name> <partitioned consumer group name> - get partitioned consumer group info")
			fmt.Println("create <stream name> <partitioned consumer group name> <max members> <filter> <comma separated partitioning wildcard indexes> - create a partitioned consumer group")
			fmt.Println("delete/rm <stream name> <partitioned consumer group name>- delete a partitioned consumer group")
			fmt.Println("memberinfo/minfo <stream name> <partitioned consumer group name> <member name> - get partitioned consumer group member info")
			fmt.Println("add <stream name> <partitioned consumer group name> <member name> [...] - add a member to a partitioned consumer group")
			fmt.Println("drop <stream name> <partitioned consumer group name> <member name> [...] - remove a member from a partitioned consumer group")
			fmt.Println("deletemapping <stream name> <partitioned consumer group name> - delete all member mappings for a partitioned consumer group")
			fmt.Println("createmapping <stream name> <partitioned consumer group name> - create member mappings for a partitioned consumer group")
			fmt.Println("stepdown/sd <stream name> <partitioned consumer group name> <member name> - initiate a step down for a member")
			fmt.Println("consume/join <stream name> <partitioned consumer group name> <member name> - join a partitioned consumer group")
			fmt.Println("stop/leave - stop consuming and leave the partitioned consumer group")
			fmt.Println("static - static consumer groups mode")
			fmt.Println("elastic - elastic consumer groups mode")
		case "exit", "quit":
			log.Println("Exiting...")
			if cg.consuming {
				cg.myContextCancel()
			}
			os.Exit(0)
		case "static":
			cg.static()
		case "elastic":
			cg.elastic()
		case "processing-time":
			var processingTimeInput string

			if len(args) != 1 {
				fmt.Print("processing time: ")
				_, _ = fmt.Scanln(&processingTimeInput)
			} else {
				processingTimeInput = argsString
			}

			duration, err := cg.setProcessingTime(processingTimeInput)
			if err != nil {
				fmt.Printf("error: can't parse processing time: %v\n", err)
				break
			}

			fmt.Printf("processing time set to %v\n", duration)
		case "list", "ls":
			if len(args) != 1 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
			} else {
				cg.streamName = argsString
			}

			if cg.consumerStatic {
				err = cg.lsStaticAction(nil)
			} else {
				err = cg.lsElasticAction(nil)
			}
			if err != nil {
				fmt.Printf("error: can't list partitioned consumer groups: %v\n", err)
				break
			}
		case "add":
			var memberNameInput string

			if cg.consumerStatic {
				fmt.Println("can not add members to a static partitioned consumer groups, you must delete and recreate them")
				break
			}

			if len(args) < 3 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("member name (or space separated list of names): ")
				memberNameInput, _ = r.ReadString('\n')
				memberNameInput = strings.TrimSpace(memberNameInput)
				cg.memberNames = strings.Split(memberNameInput, " ")
			} else if len(args) >= 3 {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				cg.memberNames = args[2:]
			} else {
				fmt.Println("error: can not parse the input")
			}

			err = cg.addElasticAction(nil)

			if err != nil {
				fmt.Printf("can't add members: %v", err)
				break
			}
		case "drop":
			var memberNameInput string

			if cg.consumerStatic {
				fmt.Println("can not drop members from a static partitioned consumer groups, you must delete and recreate them")
			}

			if len(args) < 3 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("member name (or space separated list of names): ")
				memberNameInput, _ = r.ReadString('\n')
				memberNameInput = strings.TrimSpace(memberNameInput)
				cg.memberNames = strings.Split(memberNameInput, " ")
			} else if len(args) >= 3 {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				cg.memberNames = args[2:]
			} else {
				fmt.Println("error: can not parse the input")
			}

			err = cg.dropElasticAction(nil)

			if err != nil {
				fmt.Printf("can't drop members: %v", err)
				break
			}
		case "createmapping", "create-mapping", "cm":
			var err error

			if len(args) != 2 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
			}

			memberMappingsArgs := inputMemberMappings()
			memberMappings, err := parseMemberMappings(memberMappingsArgs)
			if err != nil {
				fmt.Printf("can't parse member mappings: %v", err)
				break
			}

			err = cg.createElasticMappingAction(nil)

			if err != nil {
				fmt.Printf("can't set member mappings: %v", err)
				break
			} else {
				fmt.Printf("member mappings set: %+v\n", memberMappings)
			}
		case "deletemapping", "delete-mapping", "dm":

			if len(args) != 2 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
			}

			fmt.Print("WARNING: this operation will cause all existing consumer members to terminate consuming are you sure? (y/n): ")
			var confirmation string
			_, _ = fmt.Scanln(&confirmation)

			if confirmation != "y" {
				break
			}

			err = streamconsumergroup.DeleteMemberMappings(cg.nc, cg.streamName, cg.consumerGroupName)
			if err != nil {
				fmt.Printf("can't delete member mappings: %v", err)
				break
			} else {
				fmt.Printf("member mappings deleted")
			}
		case "create":
			var err error

			if len(args) < 6 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("max members: ")
				_, _ = fmt.Scanf("%d", &cg.maxMembers)
				fmt.Print("filter: ")
				_, _ = fmt.Scanln(&cg.filter)
				if cg.consumerStatic {
					fmt.Print("space separated set of members (hit return to set member mappings instead): ")
					memberNameInput, _ := r.ReadString('\n')
					memberNameInput = strings.TrimSpace(memberNameInput)
					cg.memberNames = strings.Split(memberNameInput, " ")

					if len(cg.memberNames) == 1 && cg.memberNames[0] == "" {
						fmt.Printf("enter the member mappings\n")
						cg.memberMappingArgs = inputMemberMappings()
						if len(cg.memberMappingArgs) == 0 {
							fmt.Printf("member mappings not defined, can't create the paritioned consumer group")
							break
						}

						err := cg.createStaticBalancedAction(nil)
						if err != nil {
							fmt.Printf("can't create static partitioned consumer group: %v", err)
							break
						}
					} else {
						err := cg.createStaticMappedAction(nil)
						if err != nil {
							fmt.Printf("can't create static partitioned consumer group: %v", err)
							break
						}
					}
				} else { // Elastic
					fmt.Print("space separated partitioning wildcard indexes: ")
					pwciInput, _ := r.ReadString('\n')
					pwciInput = strings.TrimSpace(pwciInput)
					pwciArgs := strings.Split(pwciInput, " ")
					fmt.Print("max buffered messages (0 for no limit): ")
					_, _ = fmt.Scanf("%d", &cg.maxBufferedMsgs)
					fmt.Print("max buffered bytes (0 for no limit): ")
					_, _ = fmt.Scanf("%d", &cg.maxBufferedBytes)

					cg.pwcis = make([]int, len(pwciArgs))

					for i, pwci := range pwciArgs {
						var err error
						cg.pwcis[i], err = strconv.Atoi(pwci)
						if err != nil {
							fmt.Printf("can't parse partition %s: %v", pwci, err)
							break
						}
					}
				}
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				mm, err := strconv.Atoi(args[2])
				if err != nil {
					fmt.Printf("can't parse max members: %v", err)
					break
				}

				cg.maxMembers = uint(mm)
				cg.filter = args[3]
				pwciArgs := strings.Split(args[4], ",")
				cg.pwcis = make([]int, len(pwciArgs))

				for i, pwci := range pwciArgs {
					cg.pwcis[i], err = strconv.Atoi(pwci)
					if err != nil {
						fmt.Printf("can't parse partition %s: %v", pwci, err)
						break
					}
				}

				maxBufferedMsgs, err := strconv.Atoi(args[5])
				if err != nil {
					fmt.Printf("can't parse max buffered messages: %v", err)
					break
				}
				cg.maxBufferedMsgs = int64(maxBufferedMsgs)
			}

			myContext := context.Background()
			myContext, cg.myContextCancel = context.WithCancel(myContext)

			err = cg.createElasticAction(nil)
			if err != nil {
				fmt.Printf("can't create partitioned consumer group: %v", err)
			}
		case "delete", "rm":
			if len(args) != 2 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
			}

			var err error
			if cg.consumerStatic {
				err = cg.deleteStaticAction(nil)
			} else {
				err = cg.deleteElasticAction(nil)
			}
			if err != nil {
				fmt.Printf("can't delete paritioned consumer group: %v", err)
				break
			}
		case "info":
			if len(args) != 2 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
			}

			if cg.consumerStatic {
				err = cg.infoStaticAction(nil)
				if err != nil {
					fmt.Printf("can't get static partitioned consumer group config: %v", err)
					break
				}
			} else {
				err = cg.infoElasticAction(nil)
				if err != nil {
					fmt.Printf("can't get elastic partitioned consumer group config: %v", err)
					break
				}
			}
		case "memberinfo", "member-info", "minfo":
			if len(args) != 3 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("member name: ")
				_, _ = fmt.Scanln(&cg.memberName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				cg.memberName = args[2]
			}

			if cg.consumerStatic {
				err = cg.memberStaticInfoAction(nil)
			} else {
				err = cg.memberElasticInfoAction(nil)
			}
		case "stepdown", "step-down", "sd":

			if len(args) != 3 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("member name: ")
				_, _ = fmt.Scanln(&cg.memberName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				cg.memberName = args[2]
			}

			var err error
			if cg.consumerStatic {
				err = cg.stepDownStaticAction(nil)
			} else {
				err = cg.stepDownElasticAction(nil)
			}
			if err != nil {
				fmt.Printf("can't step down member: %v", err)
				break
			}
			fmt.Printf("member %s step down initiated", cg.memberName)
		case "consume", "join":
			if cg.consuming {
				fmt.Println("already consuming")
				break
			}

			if len(args) != 3 {
				fmt.Print("stream name: ")
				_, _ = fmt.Scanln(&cg.streamName)
				fmt.Print("consumer group name: ")
				_, _ = fmt.Scanln(&cg.consumerGroupName)
				fmt.Print("member name: ")
				_, _ = fmt.Scanln(&cg.memberName)
			} else {
				cg.streamName = args[0]
				cg.consumerGroupName = args[1]
				cg.memberName = strings.TrimSpace(args[2])
			}

			myContext := context.Background()
			myContext, cg.myContextCancel = context.WithCancel(myContext)

			go cg.consume(myContext, cg.nc, cg.streamName, cg.consumerGroupName, cg.memberName, messageHandler(cg.processingDuration), 1)
			cg.consuming = true
		case "stop", "leave":
			if !cg.consuming {
				fmt.Println("not consuming")
				break
			}

			cg.myContextCancel()
			cg.consuming = false
			cg.memberName = ""
		case "":
		default:
			fmt.Printf("unknown command: %s", command)
		}
	}
}

func inputMemberMappings() []string {
	var memberMappings []string
	for {
		var memberName string
		var partitionNumbersInput string

		fmt.Print("member name (hit return to finish): ")
		_, _ = fmt.Scanln(&memberName)
		if memberName == "" {
			break
		}
		fmt.Print("comma separated list of partition numbers: ")
		_, _ = fmt.Scanln(&partitionNumbersInput)

		memberMappings = append(memberMappings, fmt.Sprintf("%s:%s", memberName, partitionNumbersInput))
	}
	return memberMappings
}

func parseMemberMappings(mappings []string) ([]streamconsumergroup.MemberMapping, error) {
	var memberMappings []streamconsumergroup.MemberMapping
	for _, mapping := range mappings {
		memberName, partitionsInput, found := strings.Cut(mapping, ":")
		if !found {
			return nil, errors.New(fmt.Sprintf("can't parse member mapping %s: missing ':'", mapping))
		}
		partitionsArgs := strings.Split(partitionsInput, ",")

		partitionsNumbers := make([]int, len(partitionsArgs))
		for i, partition := range partitionsArgs {
			var err error
			partitionsNumbers[i], err = strconv.Atoi(partition)
			if err != nil {
				return nil, err
			}
		}
		memberMappings = append(memberMappings, streamconsumergroup.MemberMapping{
			Member:     memberName,
			Partitions: partitionsNumbers,
		})
	}
	return memberMappings, nil
}
