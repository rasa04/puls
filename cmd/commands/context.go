package commands

import (
	"fmt"
	"sort"
	"flag"
	"strings"
	"errors"
	"encoding/json"
	pulsarConfig "puls/cmd/config"
	pulsarContext "puls/cmd/ctx"
)

func CmdContext(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: puls context [use|current|get|set|list|delete]")
	}
	sub := args[0]
	cfg, err := pulsarConfig.LoadConfig()
	if err != nil {
		return err
	}

	switch sub {

	case "current":
		if cfg.Current == "" {
			fmt.Println("(no current context)")
			return nil
		}
		fmt.Println(cfg.Current)
		return nil

	case "list":
		if len(cfg.Contexts) == 0 {
			fmt.Println("(no contexts)")
			return nil
		}
		names := make([]string, 0, len(cfg.Contexts))
		for name := range cfg.Contexts {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			mark := " "
			if name == cfg.Current {
				mark = "*"
			}
			fmt.Printf("%s %s\n", mark, name)
		}
		return nil

	case "use":
		if len(args) < 2 {
			return errors.New("usage: puls context use <name>")
		}
		name := args[1]
		if _, ok := cfg.Contexts[name]; !ok {
			return fmt.Errorf("context %q not found", name)
		}
		cfg.Current = name
		if err := pulsarConfig.SaveConfig(cfg); err != nil {
			return err
		}
		fmt.Println("current context:", name)
		return nil

	case "get":
		var name string
		if len(args) >= 2 {
			name = args[1]
		} else {
			name = cfg.Current
		}
		if name == "" {
			return errors.New("no context selected; use: puls context use <name>")
		}
		c := cfg.Contexts[name]
		if c == nil {
			return fmt.Errorf("context %q not found", name)
		}
		b, _ := json.MarshalIndent(c, "", "  ")
		fmt.Println(string(b))
		return nil

	case "delete":
		if len(args) < 2 {
			return errors.New("usage: puls context delete <name>")
		}
		name := args[1]
		if _, ok := cfg.Contexts[name]; !ok {
			return fmt.Errorf("context %q not found", name)
		}
		delete(cfg.Contexts, name)
		if cfg.Current == name {
			cfg.Current = ""
		}
		if err := pulsarConfig.SaveConfig(cfg); err != nil {
			return err
		}
		fmt.Println("deleted context:", name)
		return nil

	case "set":
		fs := flag.NewFlagSet("context set", flag.ContinueOnError)
		var name, urlStr, tok, tenant, ns, prefix string
		var timeout int
		fs.StringVar(&name, "name", "", "context name (required)")
		fs.StringVar(&urlStr, "url", "", "admin URL (e.g. http://broker:8080/admin/v2)")
		fs.StringVar(&tok, "token", "", "bearer token (optional)")
		fs.StringVar(&tenant, "tenant", "", "tenant (e.g. amocrm)")
		fs.StringVar(&ns, "namespace", "", "namespace (e.g. core-dev)")
		fs.StringVar(&prefix, "prefix", "", "topic name prefix filter (optional)")
		fs.IntVar(&timeout, "timeout", 10, "HTTP timeout in seconds")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		rest := fs.Args()
		if urlStr == "" && len(rest) > 0 {
			urlStr = rest[0]
		}
		if name == "" {
			return errors.New("--name is required")
		}
		if cfg.Contexts[name] == nil {
			cfg.Contexts[name] = &pulsarContext.Context{Name: name}
		}
		cx := cfg.Contexts[name]
		if urlStr != "" {
			cx.AdminURL = strings.TrimRight(urlStr, "/")
		}
		if tok != "" {
			cx.Token = tok
		}
		if tenant != "" {
			cx.Tenant = tenant
		}
		if ns != "" {
			cx.Namespace = ns
		}
		if prefix != "" {
			cx.Prefix = prefix
		}
		if timeout > 0 {
			cx.HTTPTimeoutSec = timeout
		}
		if cfg.Current == "" {
			cfg.Current = name
		}
		if err := pulsarConfig.SaveConfig(cfg); err != nil {
			return err
		}
		fmt.Println("saved context:", name)
		return nil

	default:
		return fmt.Errorf("unknown subcommand: %s", sub)
	}
}
