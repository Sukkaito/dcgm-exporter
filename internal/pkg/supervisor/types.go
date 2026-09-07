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
	"net/http"
	"time"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/appconfig"
)

// WorkerStatus represents the live state of a worker process.
type WorkerStatus struct {
	Spec       appconfig.RemoteSourceSpec `json:"spec"`
	Running    bool                       `json:"running"`
	Pid        int                        `json:"pid,omitempty"`
	SocketPath string                     `json:"socket_path"`
	LastError  string                     `json:"last_error,omitempty"`
	LastScrape time.Time                  `json:"last_scrape,omitempty"`
}

// Worker defines the lifecycle of a remote hostengine worker child process.
type Worker interface {
	Start(ctx context.Context) error
	Stop() error
	Status() WorkerStatus
	SocketPath() string
	Spec() appconfig.RemoteSourceSpec
}

// Aggregator gathers metrics from all workers and merges them into Prometheus format.
type Aggregator interface {
	http.Handler
	ScrapeAll(ctx context.Context) ([]byte, error)
	Statuses() []WorkerStatus
}

