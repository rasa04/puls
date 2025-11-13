package commands

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"strconv"

	pulsarClient "puls/cmd/client"
	pulsarConfig "puls/cmd/config"
)

type topicInfo struct {
	Ref     pulsarClient.TopicRef
	Backlog int64
	Kind    string // "non-partitioned" / "partitioned"
}

func CmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	var ctxName, tenantOverride, nsOverride, prefixOverride string
	var includeInternal bool
	var full bool
	var verbose bool
	var parallel int
	var withPartitioned bool

	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&prefixOverride, "prefix", "", "topic name prefix (optional, overrides context prefix)")
	fs.BoolVar(&includeInternal, "include-internal", false, "include system/internal topics")
	fs.BoolVar(&full, "full", false, "show all topics (including backlog=0)")
	fs.BoolVar(&verbose, "verbose", false, "print detailed progress to stderr")
	fs.IntVar(&parallel, "parallel", 16, "max parallel stats requests")
	fs.BoolVar(&withPartitioned, "with-partitioned", false, "with partitioned topics")

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
			"[puls] list: context=%q tenant=%q namespace=%q prefix=%q includeInternal=%v full=%v parallel=%d\n",
			ctxLabel, tenant, ns, prefix, includeInternal, full, parallel,
		)
	}

	h := pulsarClient.NewHTTP(cx)
	ctx := context.Background()

	if verbose {
		fmt.Fprintln(os.Stderr, "[puls] listing topics from Pulsar admin API...")
	}

	var result []topicInfo
	result, err = listNonPartitioned(
		ctx,
		h,
		tenant,
		ns,
		includeInternal,
		verbose,
		prefix,
		full,
		parallel,
	)
	if err != nil {
		return err
	}

	// если указали флаг
	if withPartitioned {
		parts, err := pulsarClient.ListPartitionedTopics(ctx, h, tenant, ns, includeInternal)
		if err != nil {
			return err
		}
	
		if verbose {
			fmt.Fprintf(
				os.Stderr,
				"[puls] found %d partitioned topics (before prefix filter)\n",
				len(parts),
			)
		}
	
		parts = pulsarClient.FilterTopicsByPrefix(parts, prefix)
	
		if verbose {
			fmt.Fprintf(
				os.Stderr,
				"[puls] after prefix=%q: %d partitioned\n",
				prefix,
				len(parts),
			)
			fmt.Fprintf(os.Stderr, "[puls] fetching stats for partitioned topics in parallel (parallel=%d)...\n", parallel)
		}
	
		// --- параллельно тянем бэклоги ---
		partInfos := pulsarClient.FetchPartitionedBacklogsParallel(ctx, h, parts, parallel)
		// partitioned
		for _, info := range partInfos {
			if info.Err != nil {
				fmt.Fprintf(os.Stderr, "warn: partitioned-stats %s: %v\n", info.Ref.FullName, info.Err)
				continue
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "[puls] stats partitioned %s: backlog=%d empty=%v\n",
					info.Ref.FullName, info.Backlog, info.Empty)
			}
			if !full && info.Backlog == 0 {
				continue
			}
			result = append(result, topicInfo{
				Ref:     info.Ref,
				Backlog: info.Backlog,
				Kind:    "partitioned",
			})
		}
	}

	if len(result) == 0 {
		if full {
			fmt.Println("no topics found (check tenant/namespace/prefix)")
		} else {
			fmt.Println("no topics with backlog > 0 found")
		}
		return nil
	}

	// сортируем по имени (стабильный вывод)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Ref.FullName < result[j].Ref.FullName
	})

	printList(result)

	if verbose {
		fmt.Fprintf(os.Stderr, "[puls] list finished, printed %d topics\n", len(result))
	}

	return nil
}

func listNonPartitioned(
	ctx context.Context,
	h *pulsarClient.HttpClient,
	tenant string,
	ns string,
	includeInternal bool,
	verbose bool,
	prefix string,
	full bool,
	parallel int,
) ([]topicInfo, error) {
	var result []topicInfo
	nonParts, err := pulsarClient.ListNonPartitionedTopics(ctx, h, tenant, ns, includeInternal)
	if err != nil {
		return nil, err
	}
	if verbose {
		fmt.Fprintf(
			os.Stderr,
			"[puls] found %d non-partitioned topics (before prefix filter)\n",
			len(nonParts),
		)
	}

	nonParts = pulsarClient.FilterTopicsByPrefix(nonParts, prefix)
	if verbose {
		fmt.Fprintf(
			os.Stderr,
			"[puls] after prefix=%q: %d non-partitioned\n",
			prefix, len(nonParts),
		)
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "[puls] fetching stats for non-partitioned topics in parallel (parallel=%d)...\n", parallel)
	}
	nonInfos := pulsarClient.FetchNonPartitionedBacklogsParallel(ctx, h, nonParts, parallel)
	// non-partitioned
	for _, info := range nonInfos {
		if info.Err != nil {
			fmt.Fprintf(os.Stderr, "warn: stats %s: %v\n", info.Ref.FullName, info.Err)
			continue
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] stats non-partitioned %s: backlog=%d empty=%v\n",
				info.Ref.FullName, info.Backlog, info.Empty)
		}
		if !full && info.Backlog == 0 {
			continue
		}
		result = append(result, topicInfo{
			Ref:     info.Ref,
			Backlog: info.Backlog,
			Kind:    "non-partitioned",
		})
	}
	return result, nil
}

// helpers

func printList(result []topicInfo) {
	// вычисляем максимальную длину имени — чтобы красиво выровнять колонку
	maxNameLen := 0
	for _, ti := range result {
	    if l := len(ti.Ref.FullName); l > maxNameLen {
	        maxNameLen = l
	    }
	}
	
	// заголовок
	fmt.Printf("%-*s | %12s | %s\n", maxNameLen, "TOPIC", "BACKLOG", "KIND")
	
	// простая «линия» под заголовком
	fmt.Printf("%s-+-%s-+-%s\n",
	    strings.Repeat("-", maxNameLen),
	    strings.Repeat("-", 12),
	    strings.Repeat("-", 6),
	)

	// строки с данными
	for _, ti := range result {
		kindShort := "part"
		if ti.Kind == "non-partitioned" {
			kindShort = "nonpar"
		}
		fmt.Printf("%-*s | %12s | %s\n",
			maxNameLen,
			ti.Ref.FullName,
			formatIntWithSep(ti.Backlog),
			kindShort,
		)
	}
}

func formatIntWithSep(n int64) string {
    // 0 → "0"
    if n == 0 {
        return "0"
    }
    neg := n < 0
    if neg {
        n = -n
    }

    s := strconv.FormatInt(n, 10)
    // добавляем "_" каждые три цифры справа
    for i := len(s) - 3; i > 0; i -= 3 {
        s = s[:i] + "_" + s[i:]
    }

    if neg {
        return "-" + s
    }
    return s
}
