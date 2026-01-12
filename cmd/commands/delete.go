package commands

import (
	"context"
	"flag"
	"fmt"
	"os"

	pulsarClient  "puls/cmd/client"
	pulsarConfig  "puls/cmd/config"
	pulsarContext "puls/cmd/ctx"
	utils         "puls/cmd/utils"
)

func CmdDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	var ctxName, tenantOverride, nsOverride, kind string
	var dryRun, verbose bool

	fs.StringVar(&ctxName, "context", "", "context name (optional)")
	fs.StringVar(&tenantOverride, "tenant", "", "override tenant (optional)")
	fs.StringVar(&nsOverride, "namespace", "", "override namespace (optional)")
	fs.StringVar(&kind, "kind", "auto", "topic kind: auto|partitioned|non-partitioned")
	fs.BoolVar(&dryRun, "dry-run", false, "print what would be deleted without deleting")
	fs.BoolVar(&verbose, "verbose", false, "print detailed progress to stderr")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: puls delete [flags] <topic> [<topic>...]")
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

	// делаем копию контекста для ParseTopicArg (чтобы работали override tenant/ns)
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
		fmt.Fprintf(os.Stderr,
			"[puls] delete: context=%q tenant=%q namespace=%q kind=%q dryRun=%v\n",
			ctxLabel, cx2.Tenant, cx2.Namespace, kind, dryRun,
		)
	}

	h := pulsarClient.NewHTTP(&cx2)
	ctx := context.Background()

	var failed int
	for _, arg := range fs.Args() {
		tr, err := pulsarClient.ParseTopicArg(arg, (*pulsarContext.Context)(&cx2))
		if err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "error: parse topic %q: %v\n", arg, err)
			continue
		}

		delKind := kind
		if delKind == "auto" {
			isPart, derr := isPartitionedAuto(ctx, h, tr)
			if derr != nil {
				failed++
				fmt.Fprintf(os.Stderr, "error: detect kind %s: %v\n", tr.FullName, derr)
				continue
			}
			if isPart {
				delKind = "partitioned"
			} else {
				delKind = "non-partitioned"
			}
		}

		if dryRun {
			fmt.Printf("would delete %-13s %s\n", delKind, tr.FullName)
			continue
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "[puls] deleting %s %s...\n", delKind, tr.FullName)
		}

		switch delKind {
		case "partitioned":
			if err := pulsarClient.DeletePartitionedTopic(ctx, h, tr); err != nil {
				failed++
				fmt.Fprintf(os.Stderr, "error: delete partitioned %s: %v\n", tr.FullName, err)
				continue
			}
		case "non-partitioned":
			if err := pulsarClient.DeleteNonPartitionedTopic(ctx, h, tr); err != nil {
				failed++
				fmt.Fprintf(os.Stderr, "error: delete non-partitioned %s: %v\n", tr.FullName, err)
				continue
			}
		default:
			// не должно случиться
			failed++
			fmt.Fprintf(os.Stderr, "error: internal: unexpected kind=%q\n", delKind)
			continue
		}

		fmt.Printf("deleted %-13s %s\n", delKind, tr.FullName)
	}

	if failed > 0 {
		return fmt.Errorf("delete finished with %d error(s)", failed)
	}
	return nil
}

// auto-detect: пробуем partitioned-stats.
// - если 200 => partitioned
// - если 404 => non-partitioned
// - иначе => ошибка
func isPartitionedAuto(ctx context.Context, h *pulsarClient.HttpClient, t pulsarClient.TopicRef) (bool, error) {
	_, err := pulsarClient.GetPartitionedStats(ctx, h, t)
	if err == nil {
		return true, nil
	}
	if utils.IsNotFoundErr(err) {
		return false, nil
	}
	return false, err
}
