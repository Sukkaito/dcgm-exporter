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
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/appconfig"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/logging"
)

type workerProcess struct {
	mu         sync.RWMutex
	spec       appconfig.RemoteSourceSpec
	socketPath string
	baseArgs   []string
	cmd        *exec.Cmd
	cancel     context.CancelFunc
	running    bool
	lastError  string
	lastScrape time.Time
	stopOnce   sync.Once
	doneChan   chan struct{}
}

// NewWorker constructs a worker manager for a remote hostengine source.
func NewWorker(spec appconfig.RemoteSourceSpec, socketPath string, originalArgs []string) Worker {
	filteredArgs := filterWorkerArgs(originalArgs)
	return &workerProcess{
		spec:       spec,
		socketPath: socketPath,
		baseArgs:   filteredArgs,
		doneChan:   make(chan struct{}),
	}
}

func (w *workerProcess) Spec() appconfig.RemoteSourceSpec {
	return w.spec
}

func (w *workerProcess) SocketPath() string {
	return w.socketPath
}

func (w *workerProcess) Status() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	pid := 0
	if w.cmd != nil && w.cmd.Process != nil {
		pid = w.cmd.Process.Pid
	}

	return WorkerStatus{
		Spec:       w.spec,
		Running:    w.running,
		Pid:        pid,
		SocketPath: w.socketPath,
		LastError:  w.lastError,
		LastScrape: w.lastScrape,
	}
}

func (w *workerProcess) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.running = true
	w.mu.Unlock()

	go w.runLoop(workerCtx)
	return nil
}

func (w *workerProcess) runLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := w.spawn(ctx)
		if err != nil {
			w.mu.Lock()
			w.lastError = err.Error()
			w.mu.Unlock()
			slog.Error("Worker process error",
				slog.String("alias", w.spec.Alias),
				slog.String("uri", w.spec.URI),
				slog.String(logging.ErrorKey, err.Error()),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			slog.Info("Restarting worker process",
				slog.String("alias", w.spec.Alias),
				slog.String("uri", w.spec.URI),
			)
		}
	}
}

func (w *workerProcess) spawn(ctx context.Context) error {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}

	args := append([]string{}, w.baseArgs...)
	args = append(args,
		"--remote-hostengine-info", w.spec.URI,
		"--internal-worker",
		"--internal-worker-socket", w.socketPath,
		"--internal-worker-alias", w.spec.Alias,
	)

	// Clean up any stale socket file before starting
	_ = os.Remove(w.socketPath)

	cmd := exec.CommandContext(ctx, exe, args...)

	// Filter out DCGM_REMOTE_HOSTENGINE_INFO from child environment
	// so it does not conflict with the explicitly assigned worker CLI flag.
	env := os.Environ()
	childEnv := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, "DCGM_REMOTE_HOSTENGINE_INFO=") {
			childEnv = append(childEnv, e)
		}
	}
	cmd.Env = childEnv

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to open stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to open stderr pipe: %w", err)
	}

	go streamLogs("stdout", w.spec.Alias, stdout)
	go streamLogs("stderr", w.spec.Alias, stderr)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}

	w.mu.Lock()
	w.cmd = cmd
	w.mu.Unlock()

	slog.Info("Worker process started",
		slog.String("alias", w.spec.Alias),
		slog.String("uri", w.spec.URI),
		slog.Int("pid", cmd.Process.Pid),
	)

	waitErr := cmd.Wait()

	w.mu.Lock()
	w.cmd = nil
	if waitErr != nil {
		w.lastError = waitErr.Error()
	}
	w.mu.Unlock()

	return waitErr
}

func (w *workerProcess) Stop() error {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.running = false
		if w.cancel != nil {
			w.cancel()
		}
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Signal(os.Interrupt)
		}
		w.mu.Unlock()

		_ = os.Remove(w.socketPath)
		close(w.doneChan)
	})
	return nil
}

func streamLogs(streamName string, alias string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		slog.Debug("Worker output",
			slog.String("worker", alias),
			slog.String("stream", streamName),
			slog.String("msg", line),
		)
	}
}

// filterWorkerArgs removes -r, --remote-hostengine-info, and internal worker flags from parent args
func filterWorkerArgs(args []string) []string {
	var filtered []string
	if len(args) <= 1 {
		return filtered
	}

	rawArgs := args[1:]
	skipNext := false
	for i := 0; i < len(rawArgs); i++ {
		if skipNext {
			skipNext = false
			continue
		}
		arg := rawArgs[i]

		if arg == "-r" || arg == "--remote-hostengine-info" {
			skipNext = true
			continue
		}
		if strings.HasPrefix(arg, "-r=") || strings.HasPrefix(arg, "--remote-hostengine-info=") {
			continue
		}
		if arg == "--internal-worker" ||
			arg == "--internal-worker-socket" ||
			arg == "--internal-worker-alias" {
			continue
		}
		if strings.HasPrefix(arg, "--internal-worker-socket=") ||
			strings.HasPrefix(arg, "--internal-worker-alias=") {
			continue
		}

		filtered = append(filtered, arg)
	}
	return filtered
}
