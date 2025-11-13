package client

import (
	"fmt"
	"encoding/json"
	"net/url"
	"errors"
	"net/http"
	"strings"
	"time"
	"io"
	"context"
	pulsarContext "puls/cmd/ctx"
)

type httpClient struct {
	base string
	tok  string
	c    *http.Client
}

type TopicRef struct {
	FullName  string
	Tenant    string
	Namespace string
	Name      string
}

func NewHTTP(ctx *pulsarContext.Context) *httpClient {
	return &httpClient{
		base: strings.TrimRight(ctx.AdminURL, "/"),
		tok:  ctx.Token,
		c:    &http.Client{Timeout: time.Duration(ctx.HTTPTimeoutSec) * time.Second},
	}
}

func (h *httpClient) req(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, body)
	if err != nil {
		return nil, err
	}
	if h.tok != "" {
		req.Header.Set("Authorization", "Bearer "+h.tok)
	}
	if method == "POST" || method == "PUT" || method == "DELETE" {
		req.Header.Set("Content-Type", "application/json")
	}
	return h.c.Do(req)
}

func ListNonPartitionedTopics(
	ctx context.Context,
	h *httpClient,
	tenant, ns string,
	includeSystem bool,
) ([]TopicRef, error) {
	q := ""
	if includeSystem {
		q = "?includeSystem=true"
	}
	path := fmt.Sprintf("/persistent/%s/%s%s", url.PathEscape(tenant), url.PathEscape(ns), q)
	resp, err := h.req(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list non-partitioned topics: %s (%s)", resp.Status, string(b))
	}
	var arr []string
	if err := json.NewDecoder(resp.Body).Decode(&arr); err != nil {
		return nil, err
	}
	res := make([]TopicRef, 0, len(arr))
	for _, s := range arr {
		tr, err := parseFullTopicName(s)
		if err != nil {
			continue
		}
		res = append(res, tr)
	}
	return res, nil
}

func ListPartitionedTopics(
	ctx context.Context,
	h *httpClient,
	tenant, ns string,
	includeSystem bool,
) ([]TopicRef, error) {
	q := ""
	if includeSystem {
		q = "?includeSystem=true"
	}
	path := fmt.Sprintf("/persistent/%s/%s/partitioned%s", url.PathEscape(tenant), url.PathEscape(ns), q)
	resp, err := h.req(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list partitioned topics: %s (%s)", resp.Status, string(b))
	}
	var arr []string
	if err := json.NewDecoder(resp.Body).Decode(&arr); err != nil {
		return nil, err
	}
	res := make([]TopicRef, 0, len(arr))
	for _, s := range arr {
		tr, err := parseFullTopicName(s)
		if err != nil {
			continue
		}
		res = append(res, tr)
	}
	return res, nil
}


func getNonPartitionedStats(ctx context.Context, h *httpClient, t TopicRef) (map[string]any, error) {
	path := fmt.Sprintf("/persistent/%s/%s/%s/stats",
		url.PathEscape(t.Tenant),
		url.PathEscape(t.Namespace),
		url.PathEscape(t.Name),
	)
	resp, err := h.req(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("stats %s: %s (%s)", t.FullName, resp.Status, string(b))
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func GetPartitionedStats(ctx context.Context, h *httpClient, t TopicRef) (map[string]any, error) {
	path := fmt.Sprintf("/persistent/%s/%s/%s/partitioned-stats",
		url.PathEscape(t.Tenant),
		url.PathEscape(t.Namespace),
		url.PathEscape(t.Name),
	)
	resp, err := h.req(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("partitioned-stats %s: %s (%s)", t.FullName, resp.Status, string(b))
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}


func IsEmptyNonPartitioned(ctx context.Context, h *httpClient, t TopicRef) (bool, int64, error) {
	s, err := getNonPartitionedStats(ctx, h, t)
	if err != nil {
		return false, 0, err
	}
	backlog := sumBacklogFromStats(s)
	return backlog == 0, backlog, nil
}

func IsEmptyPartitioned(ctx context.Context, h *httpClient, t TopicRef) (bool, int64, error) {
	s, err := GetPartitionedStats(ctx, h, t)
	if err != nil {
		return false, 0, err
	}
	var backlog int64

	if v, ok := s["totalBacklog"]; ok {
		switch x := v.(type) {
		case float64:
			backlog = int64(x)
		case json.Number:
			if i, err := x.Int64(); err == nil {
				backlog = i
			}
		}
	} else if pv, ok := s["partitions"]; ok {
		if parts, ok := pv.(map[string]any); ok {
			for _, sv := range parts {
				if pm, ok := sv.(map[string]any); ok {
					backlog += sumBacklogFromStats(pm)
				}
			}
		}
	} else {
		backlog = sumBacklogFromStats(s)
	}

	return backlog == 0, backlog, nil
}

func DeleteNonPartitionedTopic(ctx context.Context, h *httpClient, t TopicRef) error {
	path := fmt.Sprintf("/persistent/%s/%s/%s",
		url.PathEscape(t.Tenant),
		url.PathEscape(t.Namespace),
		url.PathEscape(t.Name),
	)
	resp, err := h.req(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete topic %s: %s (%s)", t.FullName, resp.Status, string(b))
	}
	return nil
}

func DeletePartitionedTopic(ctx context.Context, h *httpClient, t TopicRef) error {
	path := fmt.Sprintf("/persistent/%s/%s/%s/partitions",
		url.PathEscape(t.Tenant),
		url.PathEscape(t.Namespace),
		url.PathEscape(t.Name),
	)
	resp, err := h.req(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete partitioned topic %s: %s (%s)", t.FullName, resp.Status, string(b))
	}
	return nil
}

func FilterTopicsByPrefix(topics []TopicRef, prefix string) []TopicRef {
	if prefix == "" {
		return topics
	}
	out := make([]TopicRef, 0, len(topics))
	for _, t := range topics {
		if strings.HasPrefix(t.Name, prefix) {
			out = append(out, t)
		}
	}
	return out
}

// helpers

func parseFullTopicName(full string) (TopicRef, error) {
	const prefix = "persistent://"
	if !strings.HasPrefix(full, prefix) {
		return TopicRef{}, fmt.Errorf("unsupported topic name format (expected persistent://...): %s", full)
	}
	rest := strings.TrimPrefix(full, prefix)
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		return TopicRef{}, fmt.Errorf("invalid topic name: %s", full)
	}
	return TopicRef{
		FullName:  full,
		Tenant:    parts[0],
		Namespace: parts[1],
		Name:      parts[2],
	}, nil
}

func ParseTopicArg(arg string, ctx *pulsarContext.Context) (TopicRef, error) {
	if strings.HasPrefix(arg, "persistent://") {
		return parseFullTopicName(arg)
	}
	if ctx.Tenant == "" || ctx.Namespace == "" {
		return TopicRef{}, errors.New("tenant/namespace not set in context; use --topic persistent://tenant/ns/name or set context")
	}
	full := fmt.Sprintf("persistent://%s/%s/%s", ctx.Tenant, ctx.Namespace, arg)
	return TopicRef{
		FullName:  full,
		Tenant:    ctx.Tenant,
		Namespace: ctx.Namespace,
		Name:      arg,
	}, nil
}

func sumBacklogFromStats(stats map[string]any) int64 {
	v, ok := stats["subscriptions"]
	if !ok {
		return 0
	}
	subs, ok := v.(map[string]any)
	if !ok {
		return 0
	}
	var total int64
	for _, sv := range subs {
		sub, ok := sv.(map[string]any)
		if !ok {
			continue
		}
		switch x := sub["msgBacklog"].(type) {
		case float64:
			total += int64(x)
		case json.Number:
			if i, err := x.Int64(); err == nil {
				total += i
			}
		}
	}
	return total
}
