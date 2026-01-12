package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
    
	pulsarClient "puls/cmd/client"
	pulsarConfig "puls/cmd/config"
	utils        "puls/cmd/utils"
)

func CmdShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	var ctxName, tenantOverride, nsOverride, kind string
	var showConsumers, asJSON, verbose bool
	var topSubs, topParts int

	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&kind, "kind", "auto", "topic kind: auto|partitioned|non-partitioned")
	fs.BoolVar(&showConsumers, "consumers", false, "print consumer details per subscription (can be very verbose)")
	fs.IntVar(&topSubs, "top-subs", 20, "print top N subscriptions by backlog (0=all)")
	fs.IntVar(&topParts, "top-partitions", 20, "print top N partitions by backlog for partitioned topics (0=all)")
	fs.BoolVar(&asJSON, "json", false, "print raw stats json (selected kind) instead of formatted output")
	fs.BoolVar(&verbose, "verbose", false, "debug to stderr")

	if err := fs.Parse(args); err != nil {
		return err
	}

	topicArg := ""
	if fs.NArg() > 0 {
		topicArg = fs.Arg(0)
	} else {
        return errors.New("usage: puls show [flags] <topic>\n  topic can be 'name' or 'persistent://tenant/ns/name'")
	}

	kind, err := utils.NormalizeKind(kind)
	if err != nil {
		return fmt.Errorf(err.Error())
	}

	cfg, err := pulsarConfig.LoadConfig()
	if err != nil {
		return err
	}
	cx, err := pulsarConfig.MustContext(cfg, ctxName)
	if err != nil {
		return err
	}

	// apply overrides without mutating original pointer
	cx2 := *cx
	if tenantOverride != "" {
		cx2.Tenant = tenantOverride
	}
	if nsOverride != "" {
		cx2.Namespace = nsOverride
	}

	if verbose {
		ctxLabel := ctxName
		if ctxLabel == "" {
			ctxLabel = cx2.Name
		}
		fmt.Fprintf(os.Stderr, "[puls] show: context=%q tenant=%q namespace=%q kind=%q topic=%q\n",
			ctxLabel, cx2.Tenant, cx2.Namespace, kind, topicArg)
	}

	h := pulsarClient.NewHTTP(&cx2)
	ctx := context.Background()

	ref, err := pulsarClient.ParseTopicArg(topicArg, &cx2)
	if err != nil {
		return err
	}

	finalKind, stats, err := fetchStatsByKind(ctx, h, ref, kind)
	if err != nil {
		return err
	}

	if asJSON {
		// raw json for debugging; keep stable, pretty
		fmt.Println(utils.PrettyJSON(stats))
		return nil
	}

	utils.PrintTopicStats(ref, finalKind, stats, showConsumers, topSubs, topParts)
	return nil
}

func fetchStatsByKind(
	ctx context.Context,
	h *pulsarClient.HttpClient,
	ref pulsarClient.TopicRef,
	kind string,
) (finalKind string, stats map[string]any, err error) {

	switch kind {
	case "partitioned":
		st, e := pulsarClient.GetPartitionedStats(ctx, h, ref)
		return "partitioned", st, e

	case "non-partitioned":
		st, e := pulsarClient.GetNonPartitionedStats(ctx, h, ref)
		return "non-partitioned", st, e

	case "auto":
		// Правильная логика:
		//  - partitioned-stats 200 => partitioned
		//  - partitioned-stats 404 => non-partitioned
		//  - любые другие ошибки => это ошибка (не пытаемся "маскировать" non-part)
		if st, e := pulsarClient.GetPartitionedStats(ctx, h, ref); e == nil {
			return "partitioned", st, nil
		} else if utils.IsNotFoundErr(e) {
			st2, e2 := pulsarClient.GetNonPartitionedStats(ctx, h, ref)
			if e2 != nil {
				return "", nil, e2
			}
			return "non-partitioned", st2, nil
		} else {
			return "", nil, e
		}

	default:
		return "", nil, fmt.Errorf("internal: unexpected kind=%q", kind)
	}
}
