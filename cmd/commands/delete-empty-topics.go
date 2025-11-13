package commands

import (
	"context"
	"fmt"
	"os"
	"flag"
	pulsarConfig "puls/cmd/config"
	pulsarClient "puls/cmd/client"
)

func CmdDeleteEmptyTopics(args []string) error {
	fs := flag.NewFlagSet("delete-empty-topics", flag.ContinueOnError)
	var ctxName, tenantOverride, nsOverride, prefixOverride string
	var includeInternal bool
	var dry bool
	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&prefixOverride, "prefix", "", "topic name prefix filter (optional, overrides context prefix)")
	fs.BoolVar(&includeInternal, "include-internal", false, "include system/internal topics")
	fs.BoolVar(&dry, "dry-run", true, "only print what would be deleted, don't delete")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := pulsarConfig.LoadConfig()
	if err != nil {
		return err
	}
	cx, err := pulsarConfig.MustContext(cfg, ctxName)
	if err != nil {
		return err
	}
	tenant := cx.Tenant
	if tenantOverride != "" {
		tenant = tenantOverride
	}
	ns := cx.Namespace
	if nsOverride != "" {
		ns = nsOverride
	}
	prefix := cx.Prefix
	if prefixOverride != "" {
		prefix = prefixOverride
	}

	h := pulsarClient.NewHTTP(cx)
	ctx := context.Background()

	nonParts, err := pulsarClient.ListNonPartitionedTopics(ctx, h, tenant, ns, includeInternal)
	if err != nil {
		return err
	}
	parts, err := pulsarClient.ListPartitionedTopics(ctx, h, tenant, ns, includeInternal)
	if err != nil {
		return err
	}

	nonParts = pulsarClient.FilterTopicsByPrefix(nonParts, prefix)
	parts = pulsarClient.FilterTopicsByPrefix(parts, prefix)

	var candidatesNon []pulsarClient.TopicRef
	var candidatesPart []pulsarClient.TopicRef

	for _, t := range nonParts {
		e, b, err := pulsarClient.IsEmptyNonPartitioned(ctx, h, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: stats %s: %v\n", t.FullName, err)
			continue
		}
		if e {
			candidatesNon = append(candidatesNon, t)
		} else {
			_ = b
		}
	}

	for _, t := range parts {
		e, b, err := pulsarClient.IsEmptyPartitioned(ctx, h, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: partitioned-stats %s: %v\n", t.FullName, err)
			continue
		}
		if e {
			candidatesPart = append(candidatesPart, t)
		} else {
			_ = b
		}
	}

	total := len(candidatesNon) + len(candidatesPart)
	if total == 0 {
		fmt.Println("no empty topics found (backlog>0 or no topics match prefix)")
		return nil
	}

	fmt.Printf("empty topics (backlog=0), tenant=%s namespace=%s prefix=%q:\n", tenant, ns, prefix)
	for _, t := range candidatesNon {
		fmt.Printf("  non-partitioned: %s\n", t.FullName)
	}
	for _, t := range candidatesPart {
		fmt.Printf("  partitioned:     %s\n", t.FullName)
	}

	if dry {
		fmt.Println("\nDRY-RUN: nothing deleted. Re-run with --dry-run=false to actually delete.")
		return nil
	}

	for _, t := range candidatesNon {
		if err := pulsarClient.DeleteNonPartitionedTopic(ctx, h, t); err != nil {
			fmt.Fprintf(os.Stderr, "delete %s failed: %v\n", t.FullName, err)
		} else {
			fmt.Println("deleted:", t.FullName)
		}
	}

	for _, t := range candidatesPart {
		if err := pulsarClient.DeletePartitionedTopic(ctx, h, t); err != nil {
			fmt.Fprintf(os.Stderr, "delete partitioned %s failed: %v\n", t.FullName, err)
		} else {
			fmt.Println("deleted partitioned:", t.FullName)
		}
	}

	return nil
}