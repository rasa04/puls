package commands

import (
	"context"
	"flag"
	"fmt"
	"os"

	pulsarClient "puls/cmd/client"
	pulsarConfig "puls/cmd/config"
)

func CmdDeleteEmptyTopics(args []string) error {
	fs := flag.NewFlagSet("delete-empty-topics", flag.ContinueOnError)
	var ctxName, tenantOverride, nsOverride, prefixOverride string
	var includeInternal bool
	var dry bool
	var verbose bool

	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&prefixOverride, "prefix", "", "topic name prefix filter (optional, overrides context prefix)")
	fs.BoolVar(&includeInternal, "include-internal", false, "include system/internal topics")
	fs.BoolVar(&dry, "dry-run", true, "only print what would be deleted, don't delete")
	fs.BoolVar(&verbose, "verbose", false, "print detailed progress")

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

	if verbose {
		ctxLabel := ctxName
		if ctxLabel == "" {
			ctxLabel = cx.Name
		}
		fmt.Fprintf(os.Stderr,
			"[puls] delete-empty-topics: context=%q tenant=%q namespace=%q prefix=%q includeInternal=%v dryRun=%v\n",
			ctxLabel, tenant, ns, prefix, includeInternal, dry,
		)
	}

	h := pulsarClient.NewHTTP(cx)
	ctx := context.Background()

	if verbose {
		fmt.Fprintln(os.Stderr, "[puls] listing topics from Pulsar admin API...")
	}

	nonParts, err := pulsarClient.ListNonPartitionedTopics(ctx, h, tenant, ns, includeInternal)
	if err != nil {
		return err
	}
	parts, err := pulsarClient.ListPartitionedTopics(ctx, h, tenant, ns, includeInternal)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(os.Stderr,
			"[puls] found %d non-partitioned and %d partitioned topics (before prefix filter)\n",
			len(nonParts), len(parts),
		)
	}

	nonParts = pulsarClient.FilterTopicsByPrefix(nonParts, prefix)
	parts = pulsarClient.FilterTopicsByPrefix(parts, prefix)

	if verbose {
		fmt.Fprintf(os.Stderr,
			"[puls] after prefix=%q: %d non-partitioned, %d partitioned\n",
			prefix, len(nonParts), len(parts),
		)
	}

	var candidatesNon []pulsarClient.TopicRef
	var candidatesPart []pulsarClient.TopicRef

	// non-partitioned
	for _, t := range nonParts {
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] checking non-partitioned %s ... ", t.FullName)
		}
		e, b, err := pulsarClient.IsEmptyNonPartitioned(ctx, h, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: stats %s: %v\n", t.FullName, err)
			continue
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "backlog=%d empty=%v\n", b, e)
		}
		if e {
			candidatesNon = append(candidatesNon, t)
		}
	}

	// partitioned
	for _, t := range parts {
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] checking partitioned %s ... ", t.FullName)
		}
		e, b, err := pulsarClient.IsEmptyPartitioned(ctx, h, t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: partitioned-stats %s: %v\n", t.FullName, err)
			continue
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "backlog=%d empty=%v\n", b, e)
		}
		if e {
			candidatesPart = append(candidatesPart, t)
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

	if verbose {
		fmt.Fprintf(os.Stderr, "[puls] starting deletion of %d topics (non=%d, partitioned=%d)\n",
			total, len(candidatesNon), len(candidatesPart))
	}

	for _, t := range candidatesNon {
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] deleting non-partitioned: %s\n", t.FullName)
		}
		if err := pulsarClient.DeleteNonPartitionedTopic(ctx, h, t); err != nil {
			fmt.Fprintf(os.Stderr, "delete %s failed: %v\n", t.FullName, err)
		} else {
			fmt.Println("deleted:", t.FullName)
		}
	}

	for _, t := range candidatesPart {
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] deleting partitioned: %s\n", t.FullName)
		}
		if err := pulsarClient.DeletePartitionedTopic(ctx, h, t); err != nil {
			fmt.Fprintf(os.Stderr, "delete partitioned %s failed: %v\n", t.FullName, err)
		} else {
			fmt.Println("deleted partitioned:", t.FullName)
		}
	}

	if verbose {
		fmt.Fprintln(os.Stderr, "[puls] delete-empty-topics finished")
	}

	return nil
}
