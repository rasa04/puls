package utils

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pulsarClient "puls/cmd/client"
)

type subInfo struct {
	Name         string
	Type         string
	Backlog      int64
	Consumers    int
	UnackedTotal int64
	ConsumersRaw []any
}

type partInfo struct {
	Name    string
	Backlog int64
	RateIn  float64
	RateOut float64
}

// shared (можно переиспользовать и в delete.go)
func IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "404") || strings.Contains(s, "not found")
}

func PrettyJSON(m map[string]any) string {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		// fallback
		return fmt.Sprintf("%v", m)
	}
	return string(b)
}

func PrintTopicStats(
	ref pulsarClient.TopicRef,
	kind string,
	stats map[string]any,
	showConsumers bool,
	topSubs int,
	topParts int,
) {
	fmt.Printf("topic: %s\n", ref.FullName)
	fmt.Printf("kind:  %s\n", kind)

	// Основные метрики
	topicBacklog := extractTopicBacklog(kind, stats)
	subs, subsBacklog, subsConsumers, subsUnacked := extractSubscriptions(stats)
	publishers := sliceLen(stats["publishers"])

	fmt.Printf("backlog(topic):      %s\n", formatIntWithSep(topicBacklog))
	fmt.Printf("backlog(subs sum):   %s\n", formatIntWithSep(subsBacklog))
	fmt.Printf("subscriptions:       %d\n", len(subs))
	fmt.Printf("consumers(total):    %d\n", subsConsumers)
	fmt.Printf("unacked(total):      %s\n", formatIntWithSep(subsUnacked))
	fmt.Printf("publishers(prods):   %d\n", publishers)

	// Rates/throughput если есть
	if v := float64From(stats["msgRateIn"]); v != 0 {
		fmt.Printf("msgRateIn:           %.3f msg/s\n", v)
	}
	if v := float64From(stats["msgRateOut"]); v != 0 {
		fmt.Printf("msgRateOut:          %.3f msg/s\n", v)
	}
	if v := float64From(stats["msgThroughputIn"]); v != 0 {
		fmt.Printf("throughputIn:        %s/s\n", humanBytes(int64(v)))
	}
	if v := float64From(stats["msgThroughputOut"]); v != 0 {
		fmt.Printf("throughputOut:       %s/s\n", humanBytes(int64(v)))
	}
	if v := float64From(stats["averageMsgSize"]); v != 0 {
		fmt.Printf("averageMsgSize:      %s\n", humanBytes(int64(v)))
	}
	if v := int64From(stats["storageSize"]); v != 0 {
		fmt.Printf("storageSize:         %s\n", humanBytes(v))
	}

	// Partitioned: покажем топ партиции (если есть partitions)
	if kind == "partitioned" {
		parts := extractPartitions(stats)
		if len(parts) > 0 {
			sort.Slice(parts, func(i, j int) bool {
				if parts[i].Backlog == parts[j].Backlog {
					return parts[i].Name < parts[j].Name
				}
				return parts[i].Backlog > parts[j].Backlog
			})

			fmt.Println()
			fmt.Printf("partitions: %d\n", len(parts))

			limit := topParts
			if limit == 0 || limit > len(parts) {
				limit = len(parts)
			}
			for i := 0; i < limit; i++ {
				p := parts[i]
				fmt.Printf("  - %s  backlog=%s", p.Name, formatIntWithSep(p.Backlog))
				if p.RateIn != 0 {
					fmt.Printf("  in=%.3f", p.RateIn)
				}
				if p.RateOut != 0 {
					fmt.Printf("  out=%.3f", p.RateOut)
				}
				fmt.Println()
			}
			if limit < len(parts) {
				fmt.Printf("  ... (%d more)\n", len(parts)-limit)
			}
		}
	}

	// Subscriptions: топ по backlog
	if len(subs) > 0 {
		sort.Slice(subs, func(i, j int) bool {
			if subs[i].Backlog == subs[j].Backlog {
				return subs[i].Name < subs[j].Name
			}
			return subs[i].Backlog > subs[j].Backlog
		})

		fmt.Println()
		fmt.Println("subscriptions (by backlog):")

		limit := topSubs
		if limit == 0 || limit > len(subs) {
			limit = len(subs)
		}
		for i := 0; i < limit; i++ {
			s := subs[i]
			if s.Type != "" {
				fmt.Printf("  - %s  type=%s  backlog=%s  consumers=%d  unacked=%s\n",
					s.Name, s.Type, formatIntWithSep(s.Backlog), s.Consumers, formatIntWithSep(s.UnackedTotal))
			} else {
				fmt.Printf("  - %s  backlog=%s  consumers=%d  unacked=%s\n",
					s.Name, formatIntWithSep(s.Backlog), s.Consumers, formatIntWithSep(s.UnackedTotal))
			}

			if showConsumers && len(s.ConsumersRaw) > 0 {
				for idx, cRaw := range s.ConsumersRaw {
					c, ok := cRaw.(map[string]any)
					if !ok {
						continue
					}
					cname := stringFrom(c["consumerName"])
					addr := stringFrom(c["address"])
					unacked := int64From(c["unackedMessages"])
					fmt.Printf("      consumer #%d: name=%s addr=%s unacked=%s\n",
						idx+1, cname, addr, formatIntWithSep(unacked))
				}
			}
		}
		if limit < len(subs) {
			fmt.Printf("  ... (%d more)\n", len(subs)-limit)
		}
	}
}

// ---------- extraction ----------

func extractTopicBacklog(kind string, stats map[string]any) int64 {
	// partitioned часто имеет totalBacklog
	if kind == "partitioned" {
		if v := int64From(stats["totalBacklog"]); v != 0 {
			return v
		}
	}
	// иногда есть msgBacklog
	if v := int64From(stats["msgBacklog"]); v != 0 {
		return v
	}
	// fallback: сумма backlog по подпискам
	_, subsSum, _, _ := extractSubscriptions(stats)
	return subsSum
}

func extractSubscriptions(stats map[string]any) ([]subInfo, int64, int, int64) {
	raw, ok := stats["subscriptions"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, 0, 0, 0
	}

	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]subInfo, 0, len(raw))
	var sumBacklog int64
	var sumConsumers int
	var sumUnacked int64

	for _, name := range names {
		subMap, ok := raw[name].(map[string]any)
		if !ok {
			continue
		}
		backlog := int64From(subMap["msgBacklog"])
		typ := stringFrom(subMap["type"])

		consumersRaw, _ := subMap["consumers"].([]any)
		consCount := len(consumersRaw)

		var unackedTotal int64
		for _, cRaw := range consumersRaw {
			cm, ok := cRaw.(map[string]any)
			if !ok {
				continue
			}
			unackedTotal += int64From(cm["unackedMessages"])
		}

		sumBacklog += backlog
		sumConsumers += consCount
		sumUnacked += unackedTotal

		out = append(out, subInfo{
			Name:         name,
			Type:         typ,
			Backlog:      backlog,
			Consumers:    consCount,
			UnackedTotal: unackedTotal,
			ConsumersRaw: consumersRaw,
		})
	}

	return out, sumBacklog, sumConsumers, sumUnacked
}

func extractPartitions(stats map[string]any) []partInfo {
	partsRaw, ok := stats["partitions"].(map[string]any)
	if !ok || len(partsRaw) == 0 {
		return nil
	}
	names := make([]string, 0, len(partsRaw))
	for name := range partsRaw {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]partInfo, 0, len(partsRaw))
	for _, name := range names {
		pm, ok := partsRaw[name].(map[string]any)
		if !ok {
			continue
		}
		backlog := int64From(pm["msgBacklog"])
		if backlog == 0 {
			// fallback: сумма backlog по subs внутри партиции
			_, sum, _, _ := extractSubscriptions(pm)
			backlog = sum
		}
		out = append(out, partInfo{
			Name:    name,
			Backlog: backlog,
			RateIn:  float64From(pm["msgRateIn"]),
			RateOut: float64From(pm["msgRateOut"]),
		})
	}
	return out
}

// ---------- conversions ----------

func float64From(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

func int64From(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	case json.Number:
		i, _ := x.Int64()
		return i
	default:
		return 0
	}
}

func stringFrom(v any) string {
	s, _ := v.(string)
	return s
}

func sliceLen(v any) int {
	if v == nil {
		return 0
	}
	if s, ok := v.([]any); ok {
		return len(s)
	}
	return 0
}

// ---------- formatting ----------

func formatIntWithSep(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "_" + s[i:]
	}
	if neg {
		return "-" + s
	}
	return s
}

func humanBytes(n int64) string {
	if n < 0 {
		return "-" + humanBytes(-n)
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}[exp]
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffix)
}
