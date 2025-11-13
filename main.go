package main

import (
	"fmt"
	"os"
	commands "puls/cmd/commands"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: puls <command> [args]")
		fmt.Fprintln(os.Stderr, "commands: context, delete-empty-topics, topic-info")
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "context":
		err = commands.CmdContext(args)
	case "list":
        err = commands.CmdList(args)
	case "delete-empty-topics":
		err = commands.CmdDeleteEmptyTopics(args)
	case "topic-info":
		err = commands.CmdTopicInfo(args)
	case "help", "-h", "--help":
		fmt.Println("usage: puls <command> [args]")
		fmt.Println("commands:")
		fmt.Println("  context             manage contexts (use/current/set/get/list/delete)")
		fmt.Println("  delete-empty-topics delete topics with zero backlog")
		fmt.Println("  topic-info          show backlog and kind for a topic")
		return
	default:
		err = fmt.Errorf("unknown command: %s", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
