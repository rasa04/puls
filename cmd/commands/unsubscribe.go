package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	pulsarClient  "puls/cmd/client"
	pulsarConfig  "puls/cmd/config"
	pulsarContext "puls/cmd/ctx"
	utils         "puls/cmd/utils"
)

func CmdUnsubscribe(args []string) error {
	fs := flag.NewFlagSet("unsubscribe", flag.ContinueOnError)

	var ctxName, tenantOverride, nsOverride string
	var kind, sub string
	var force, allPartitions, dryRun, verbose bool

	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&kind, "kind", "auto", "topic kind: auto|partitioned|non-partitioned")
	fs.StringVar(&sub, "subscription", "", "subscription name (or pass as 2nd arg)")
	// :contentReference[oaicite:2]{index=2}
	fs.BoolVar(&force, "force", false, "disconnect consumers and delete subscription forcefully (if supported)")
	fs.BoolVar(&allPartitions, "all-partitions", true, "if topic is partitioned, apply to all partitions")
	fs.BoolVar(&dryRun, "dry-run", false, "print what would be done without doing it")
	fs.BoolVar(&verbose, "verbose", false, "print detailed progress to stderr")

	if err := fs.Parse(args); err != nil {
		return err
	}

	pos := fs.Args()
	if len(pos) == 0 {
		return fmt.Errorf("usage: puls unsubscribe [flags] <topic> <subscription>\n  example: puls unsubscribe my-topic my-sub --force")
	}

	topicArg := pos[0]
	if sub == "" {
		if len(pos) < 2 {
			return fmt.Errorf("usage: puls unsubscribe [flags] <topic> <subscription>\n  example: puls unsubscribe my-topic my-sub --force")
		}
		sub = pos[1]
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

	// copy context for overrides + ParseTopicArg
	cx2 := *cx
	if tenantOverride != "" {
		cx2.Tenant = tenantOverride
	}
	if nsOverride != "" {
		cx2.Namespace = nsOverride
	}

	h := pulsarClient.NewHTTP(&cx2)
	ctx := context.Background()

	ref, err := pulsarClient.ParseTopicArg(topicArg, (*pulsarContext.Context)(&cx2))
	if err != nil {
		return err
	}

	if verbose {
		ctxLabel := ctxName
		if ctxLabel == "" {
			ctxLabel = cx2.Name
		}
		fmt.Fprintf(os.Stderr, "[puls] unsubscribe: context=%q tenant=%q namespace=%q topic=%q sub=%q kind=%q force=%v allPartitions=%v dryRun=%v\n",
			ctxLabel, cx2.Tenant, cx2.Namespace, ref.FullName, sub, kind, force, allPartitions, dryRun)
	}

	// определяем kind + (опционально) stats partitioned, чтобы знать число partitions
	delKind := kind
	var partStats map[string]any
	if delKind == "auto" {
		isPart, st, derr := detectPartitioned(ctx, h, ref)
		if derr != nil {
			return derr
		}
		if isPart {
			delKind = "partitioned"
			partStats = st
		} else {
			delKind = "non-partitioned"
		}
	}

	// если пользователь передал конкретную партицию "-partition-N" — это всегда "non-partitioned" топик
	if isPartitionTopicName(ref.Name) {
		delKind = "non-partitioned"
	}

	// targets
	targets := []pulsarClient.TopicRef{ref}

	// если partitioned + allPartitions — идём по всем партициям и удаляем подписку там
	if delKind == "partitioned" && allPartitions {
		// (1) попробуем удалить подписку на parent topic (иногда достаточно)
		if dryRun {
			fmt.Printf("would unsubscribe parent %s sub=%q force=%v\n", ref.FullName, sub, force)
		} else {
			if verbose {
				fmt.Fprintf(os.Stderr, "[puls] unsubscribe parent %s sub=%q ...\n", ref.FullName, sub)
			}
			// если это сработает — отлично, можно и не трогать партиции
			if err := pulsarClient.DeleteSubscription(ctx, h, ref, sub, force); err == nil {
				fmt.Printf("unsubscribed %s sub=%q\n", ref.FullName, sub)
				return nil
			} else if verbose {
				fmt.Fprintf(os.Stderr, "[puls] parent unsubscribe failed, will try partitions: %v\n", err)
			}
		}

		// (2) узнать число партиций
		if partStats == nil {
			st, err := pulsarClient.GetPartitionedStats(ctx, h, ref)
			if err != nil {
				return err
			}
			partStats = st
		}
		n := partitionsCountFromPartitionedStats(partStats)
		if n <= 0 {
			return fmt.Errorf("cannot determine partitions count for %s (stats has no metadata.partitions)", ref.FullName)
		}

		targets = make([]pulsarClient.TopicRef, 0, n)
		for i := 0; i < n; i++ {
			p := partitionRef(ref, i)
			targets = append(targets, p)
		}
	}

	// выполнить unsubscribe по targets
	var failed int
	for _, t := range targets {
		if dryRun {
			fmt.Printf("would unsubscribe %s sub=%q force=%v\n", t.FullName, sub, force)
			continue
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] unsubscribe %s sub=%q ...\n", t.FullName, sub)
		}
		if err := pulsarClient.DeleteSubscription(ctx, h, t, sub, force); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "error: unsubscribe %s sub=%q: %v\n", t.FullName, sub, err)
			continue
		}
		fmt.Printf("unsubscribed %s sub=%q\n", t.FullName, sub)
	}

	if failed > 0 {
		return fmt.Errorf("unsubscribe finished with %d error(s)", failed)
	}
	return nil
}

// -------- helpers (локальные для команды) --------

// detectPartitioned: как в delete — но возвращаем stats, чтобы вытащить partitions count.
// partitioned-stats на имени вида "...-partition-0" отдаёт 412, поэтому такое считаем non-partitioned.
func detectPartitioned(
	ctx context.Context,
	h *pulsarClient.HttpClient,
	t pulsarClient.TopicRef,
) (bool, map[string]any, error) {
	if isPartitionTopicName(t.Name) {
		return false, nil, nil
	}
	st, err := pulsarClient.GetPartitionedStats(ctx, h, t)
	if err == nil {
		return true, st, nil
	}
	if utils.IsNotFoundErr(err) {
		return false, nil, nil
	}
	// на всякий: если вдруг сервер ругается "topic name should not contain -partition-"
	if strings.Contains(strings.ToLower(err.Error()), "should not contain '-partition-'") {
		return false, nil, nil
	}
	return false, nil, err
}

func isPartitionTopicName(name string) bool {
	const mark = "-partition-"
	i := strings.LastIndex(name, mark)
	if i < 0 {
		return false
	}
	suf := name[i+len(mark):]
	if suf == "" {
		return false
	}
	for _, ch := range suf {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func partitionsCountFromPartitionedStats(stats map[string]any) int {
	// в partitioned-stats обычно есть metadata.partitions :contentReference[oaicite:3]{index=3}
	meta, ok := stats["metadata"].(map[string]any)
	if !ok {
		return 0
	}
	v, ok := meta["partitions"]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

func partitionRef(parent pulsarClient.TopicRef, idx int) pulsarClient.TopicRef {
	name := fmt.Sprintf("%s-partition-%d", parent.Name, idx)
	full := fmt.Sprintf("persistent://%s/%s/%s", parent.Tenant, parent.Namespace, name)
	return pulsarClient.TopicRef{
		FullName:  full,
		Tenant:    parent.Tenant,
		Namespace: parent.Namespace,
		Name:      name,
	}
}
