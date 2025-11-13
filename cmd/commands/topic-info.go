package commands

import (
	"context"
	"errors"
	"fmt"
	"flag"
	pulsarConfig "puls/cmd/config"
	pulsarClient "puls/cmd/client"
)

type TopicKind int

const (
	TopicNonPartitioned TopicKind = iota
	TopicPartitioned
)

func CmdTopicInfo(args []string) error {
	fs := flag.NewFlagSet("topic-info", flag.ContinueOnError)
	var ctxName, topicArg string
	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&topicArg, "topic", "", "topic name (persistent://tenant/ns/name or just name)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if topicArg == "" {
		return errors.New("usage: puls topic-info --topic <name or persistent://tenant/ns/name>")
	}

	cfg, err := pulsarConfig.LoadConfig()
	if err != nil {
		return err
	}
	cx, err := pulsarConfig.MustContext(cfg, ctxName)
	if err != nil {
		return err
	}
	h := pulsarClient.NewHTTP(cx)
	ctx := context.Background()

	ref, err := pulsarClient.ParseTopicArg(topicArg, cx)
	if err != nil {
		return err
	}

	// пробуем сначала как partitioned, потом как non-partitioned
	kind := TopicNonPartitioned
	empty := false
	var backlog int64

	if _, err := pulsarClient.GetPartitionedStats(ctx, h, ref); err == nil {
		kind = TopicPartitioned
		e, b, err := pulsarClient.IsEmptyPartitioned(ctx, h, ref)
		if err != nil {
			return err
		}
		empty, backlog = e, b
	} else {
		e, b, err := pulsarClient.IsEmptyNonPartitioned(ctx, h, ref)
		if err != nil {
			return err
		}
		empty, backlog = e, b
	}

	kindStr := "non-partitioned"
	if kind == TopicPartitioned {
		kindStr = "partitioned"
	}

	fmt.Printf("topic:   %s\n", ref.FullName)
	fmt.Printf("kind:    %s\n", kindStr)
	fmt.Printf("backlog: %d\n", backlog)
	fmt.Printf("empty(backlog=0): %v\n", empty)
	return nil
}
