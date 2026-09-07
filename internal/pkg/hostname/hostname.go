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

package hostname

import (
	"net"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/appconfig"
	osinterface "github.com/NVIDIA/dcgm-exporter/internal/pkg/os"
)

var os osinterface.OS = osinterface.RealOS{}

// GetHostname return a hostname where metric was collected.
func GetHostname(config *appconfig.Config) (string, error) {
	if config.WorkerHostAlias != "" {
		return config.WorkerHostAlias, nil
	}
	if config.Kubernetes {
		/* in kubernetes, the remote hostname is generic and local, so it's not useful */
		return getLocalHostname()
	}
	if config.UseRemoteHE {
		return parseRemoteHostname(config)
	}
	return getLocalHostname()
}

// DeriveRemoteSource parses a raw remote source string (e.g. "vm1=vsock://3:5555" or "10.0.0.1:5555")
// into a RemoteSourceSpec with alias, connection URI, and source transport type.
// If no alias is provided, it defaults to the derived hostname for the given URI.
func DeriveRemoteSource(raw string) (appconfig.RemoteSourceSpec, error) {
	raw = strings.TrimSpace(raw)
	var alias, uri string
	if parts := strings.SplitN(raw, "=", 2); len(parts) == 2 {
		alias = strings.TrimSpace(parts[0])
		uri = strings.TrimSpace(parts[1])
	} else {
		uri = raw
	}

	sourceType := "bare"
	derivedHost := uri

	if u, err := url.Parse(uri); err == nil && u.Scheme != "" {
		scheme := strings.ToLower(u.Scheme)
		sourceType = scheme
		switch scheme {
		case "tcp":
			if h := u.Hostname(); h != "" {
				derivedHost = h
			}
		case "unix":
			base := filepath.Base(u.Path)
			base = strings.TrimSuffix(base, ".sock")
			if base == "" || base == "/" || base == "." {
				derivedHost = "unix-socket"
			} else {
				derivedHost = "unix-" + base
			}
		case "vsock":
			if cid := u.Hostname(); cid != "" {
				derivedHost = "vsock-cid-" + cid
			} else {
				derivedHost = "vsock"
			}
		default:
			derivedHost = uri
		}
	} else {
		sourceType = "bare"
		if h, _, err := net.SplitHostPort(uri); err == nil && h != "" {
			derivedHost = h
		} else {
			derivedHost = uri
		}
	}

	if alias == "" {
		alias = derivedHost
	}

	return appconfig.RemoteSourceSpec{
		Raw:        raw,
		Alias:      alias,
		URI:        uri,
		SourceType: sourceType,
	}, nil
}

func parseRemoteHostname(config *appconfig.Config) (string, error) {
	if host, ok, err := parseRemoteHostnameURI(config.RemoteHEInfo); ok || err != nil {
		return host, err
	}

	// Extract the hostname or IP address part from the appconfig.RemoteHEInfo
	// This handles inputs like "localhost:5555", "example.com:5555", or "192.168.1.1:5555"
	host, _, err := net.SplitHostPort(config.RemoteHEInfo)
	if err != nil {
		// If there's an error, it might be because there's no port in the appconfig.RemoteHEInfo
		// In that case, use the appconfig.RemoteHEInfo as is
		host = config.RemoteHEInfo
	}
	return normalizeRemoteHostname(host)
}

func parseRemoteHostnameURI(remoteHEInfo string) (string, bool, error) {
	u, err := url.Parse(remoteHEInfo)
	if err != nil {
		return "", false, nil
	}

	switch strings.ToLower(u.Scheme) {
	case "vsock", "unix":
		hostname, err := getLocalHostname()
		return hostname, true, err
	case "tcp":
		host := u.Hostname()
		if host == "" {
			return "", false, nil
		}
		hostname, err := normalizeRemoteHostname(host)
		return hostname, true, err
	}

	return "", false, nil
}

func normalizeRemoteHostname(host string) (string, error) {
	if isLoopbackHost(host) {
		return getLocalHostname()
	}
	return host, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func getLocalHostname() (string, error) {
	if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
		return nodeName, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return hostname, nil
}
