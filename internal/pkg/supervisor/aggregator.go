/*
 * Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package supervisor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/logging"
)

const (
	remoteSourceUpMetric = "dcgm_exporter_remote_source_up"
	scrapeTimeout        = 10 * time.Second
)

type workerScrapeResult struct {
	worker Worker
	body   []byte
	err    error
	up     bool
}

type metricsAggregator struct {
	workers []Worker
}

// NewAggregator creates an Aggregator for a slice of workers.
func NewAggregator(workers []Worker) Aggregator {
	return &metricsAggregator{
		workers: workers,
	}
}

func (a *metricsAggregator) Statuses() []WorkerStatus {
	statuses := make([]WorkerStatus, len(a.workers))
	for i, w := range a.workers {
		statuses[i] = w.Status()
	}
	return statuses
}

func (a *metricsAggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	data, err := a.ScrapeAll(r.Context())
	if err != nil {
		slog.Error("Aggregation failed", slog.String(logging.ErrorKey, err.Error()))
		http.Error(w, fmt.Sprintf("Aggregation error: %v", err), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(data)
}

func (a *metricsAggregator) ScrapeAll(ctx context.Context) ([]byte, error) {
	scrapeCtx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()

	results := make([]workerScrapeResult, len(a.workers))
	var wg sync.WaitGroup

	for i, w := range a.workers {
		wg.Add(1)
		go func(idx int, worker Worker) {
			defer wg.Done()
			body, err := scrapeWorker(scrapeCtx, worker.SocketPath())
			if err != nil {
				slog.Warn("Failed to scrape worker",
					slog.String("alias", worker.Spec().Alias),
					slog.String("uri", worker.Spec().URI),
					slog.String(logging.ErrorKey, err.Error()),
				)
				results[idx] = workerScrapeResult{
					worker: worker,
					err:    err,
					up:     false,
				}
			} else {
				results[idx] = workerScrapeResult{
					worker: worker,
					body:   body,
					up:     true,
				}
			}
		}(i, w)
	}

	wg.Wait()

	return mergePrometheusMetrics(results)
}

func scrapeWorker(ctx context.Context, socketPath string) ([]byte, error) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: scrapeTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func mergePrometheusMetrics(results []workerScrapeResult) ([]byte, error) {
	var buf bytes.Buffer
	seenHelp := make(map[string]bool)
	seenType := make(map[string]bool)

	// Merge all bodies and deduplicate HELP/TYPE comments
	for _, res := range results {
		if !res.up || len(res.body) == 0 {
			continue
		}

		scanner := bufio.NewScanner(bytes.NewReader(res.body))
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "# HELP ") {
				parts := strings.SplitN(line, " ", 4)
				if len(parts) >= 3 {
					metricName := parts[2]
					if seenHelp[metricName] {
						continue
					}
					seenHelp[metricName] = true
				}
			} else if strings.HasPrefix(line, "# TYPE ") {
				parts := strings.SplitN(line, " ", 4)
				if len(parts) >= 3 {
					metricName := parts[2]
					if seenType[metricName] {
						continue
					}
					seenType[metricName] = true
				}
			}

			buf.WriteString(line)
			buf.WriteByte('\n')
		}
	}

	// Append synthetic status metrics for remote sources
	buf.WriteString(fmt.Sprintf("# HELP %s Status of remote nv-hostengine connection (1 = up, 0 = down)\n", remoteSourceUpMetric))
	buf.WriteString(fmt.Sprintf("# TYPE %s gauge\n", remoteSourceUpMetric))
	for _, res := range results {
		val := 0
		if res.up {
			val = 1
		}
		spec := res.worker.Spec()
		buf.WriteString(fmt.Sprintf("%s{remote_source=\"%s\",hostname=\"%s\",source_type=\"%s\"} %d\n",
			remoteSourceUpMetric,
			escapeMetricLabel(spec.URI),
			escapeMetricLabel(spec.Alias),
			escapeMetricLabel(spec.SourceType),
			val,
		))
	}

	return buf.Bytes(), nil
}

func escapeMetricLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

