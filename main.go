package main

import (
	"fmt"
	"os"
	"sort"

	commands "puls/cmd/commands"
)

type command struct {
	Run  func([]string) error
	Help string
}

func main() {
	cmds := make(map[string]command)

	cmds["context"] = command{Run: commands.CmdContext, Help: "manage contexts (use/current/set/get/list/delete)"}
	cmds["list"] = command{Run: commands.CmdList, Help: "list topics (--full, --with-partitioned, --verbose)"}
	cmds["show"] = command{Run: commands.CmdShow, Help: "show detailed topic info"}
	cmds["unsubscribe"] = command{Run: commands.CmdUnsubscribe, Help: "delete subscription from topic"}
	cmds["delete"] = command{Run: commands.CmdDelete, Help: "delete topic(s)"}
	cmds["delete-empty-topics"] = command{Run: commands.CmdDeleteEmptyTopics, Help: "delete topics with backlog=0 (default dry-run)"}
	cmds["help"] = command{
		Run: func(args []string) error {
			printHelp(cmds)
			return nil
		},
		Help: "show help",
	}

	if len(os.Args) < 2 {
		printHelp(cmds)
		os.Exit(2)
	}

	cmdName := os.Args[1]
	args := os.Args[2:]

	if cmdName == "-h" || cmdName == "--help" {
		printHelp(cmds)
		return
	}

	// puls <cmd> --help (покажем общий help + подсказку)
	if len(args) > 0 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help") {
		fmt.Printf("usage: puls %s [flags] ...\n\n", cmdName)
		printHelp(cmds)
		return
	}

	c, ok := cmds[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "error: unknown command: %s\n\n", cmdName)
		printHelp(cmds)
		os.Exit(2)
	}

	if err := c.Run(args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func printHelp(cmds map[string]command) {
	fmt.Println("usage: puls <command> [args]")
	fmt.Println("commands:")

	names := make([]string, 0, len(cmds))
	for n := range cmds {
		if n == "help" {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		fmt.Printf("  %-18s %s\n", n, cmds[n].Help)
	}
	fmt.Printf("  %-18s %s\n", "help", cmds["help"].Help)

	fmt.Println()
	fmt.Println("tips:")
	fmt.Println("  puls <command> --help   show command flags")
}
