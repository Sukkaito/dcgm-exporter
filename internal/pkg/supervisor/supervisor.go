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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/urfave/cli/v2"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/appconfig"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/logging"
)

// RunSupervisor starts worker processes for each remote source and serves the aggregated metrics.
func RunSupervisor(lifecycleCtx context.Context, c *cli.Context, config *appconfig.Config) error {
	sockDir, err := os.MkdirTemp("", "dcgm-exporter-workers-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary socket directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(sockDir)
	}()

	slog.Info("Starting supervisor for multiple remote sources",
		slog.Int("source_count", len(config.RemoteSources)),
		slog.String("socket_dir", sockDir),
	)

	workers := make([]Worker, 0, len(config.RemoteSources))
	for i, spec := range config.RemoteSources {
		sockPath := filepath.Join(sockDir, fmt.Sprintf("worker-%d.sock", i))
		w := NewWorker(spec, sockPath, os.Args)
		workers = append(workers, w)
	}

	for _, w := range workers {
		if err := w.Start(lifecycleCtx); err != nil {
			slog.Error("Failed to start worker",
				slog.String("alias", w.Spec().Alias),
				slog.String("uri", w.Spec().URI),
				slog.String(logging.ErrorKey, err.Error()),
			)
		}
	}

	defer func() {
		for _, w := range workers {
			_ = w.Stop()
		}
	}()

	agg := NewAggregator(workers)

	router := mux.NewRouter()
	router.Handle("/metrics", agg)
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_ = json.NewEncoder(w).Encode(agg.Statuses())
	})
	router.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html>
<head><title>DCGM Exporter (Multi-Source Supervisor)</title></head>
<body>
<h1>DCGM Exporter (Multi-Source Supervisor)</h1>
<p><a href="/metrics">Metrics</a></p>
<p><a href="/healthz">Healthz</a></p>
</body>
</html>`))
	})

	readTimeout := appconfig.DefaultWebReadTimeout
	if config.WebReadTimeout > 0 {
		readTimeout = config.WebReadTimeout
	}
	writeTimeout := appconfig.DefaultWebWriteTimeout
	if config.WebWriteTimeout > 0 {
		writeTimeout = config.WebWriteTimeout
	}

	server := &http.Server{
		Addr:         config.Address,
		Handler:      router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	webConfig := &web.FlagConfig{
		WebListenAddresses: &[]string{config.Address},
		WebSystemdSocket:   &config.WebSystemdSocket,
		WebConfigFile:      &config.WebConfigFile,
	}

	var serverWg sync.WaitGroup
	serverErrChan := make(chan error, 1)

	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		slog.Info("Supervisor HTTP server listening", slog.String("address", config.Address))
		if err := web.ListenAndServe(server, webConfig, slog.Default()); err != nil && err != http.ErrServerClosed {
			serverErrChan <- err
		}
	}()

	select {
	case <-lifecycleCtx.Done():
		slog.Info("Supervisor shutting down...")
	case err := <-serverErrChan:
		slog.Error("Supervisor HTTP server encountered error", slog.String(logging.ErrorKey, err.Error()))
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	serverWg.Wait()

	return nil
}

