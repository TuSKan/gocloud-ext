// Copyright 2026 The gocloud-ext Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package useragent appends a product token identifying this project to the
// User-Agent header of outgoing requests.
//
// It plays the same role as gocloud.dev/internal/useragent, which is
// unreachable from outside that module, but sends its own token: these drivers
// are not part of go-cloud and should not claim to be when a server operator
// is reading their logs.
package useragent

import (
	"maps"
	"net/http"
)

// prefix identifies this project in the User-Agent header.
const prefix = "gocloud-ext"

// version is bumped at release. It is deliberately coarse: this exists so a
// server operator can tell who is talking to them, not for feature detection.
const version = "0.1.0"

func userAgentString(api string) string {
	return prefix + "/" + api + "/" + version
}

// userAgentTransport wraps an http.RoundTripper, adding a User-Agent header to
// each request.
type userAgentTransport struct {
	base http.RoundTripper
	api  string
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid mutating the caller's.
	newReq := *req
	newReq.Header = make(http.Header)
	maps.Copy(newReq.Header, req.Header)
	// Append rather than replace, to preserve other information.
	newReq.Header.Set("User-Agent", req.UserAgent()+" "+userAgentString(t.api))
	return t.base.RoundTrip(&newReq)
}

// HTTPClient returns a copy of client whose requests carry the product token
// for api. The caller's client is not modified.
func HTTPClient(client *http.Client, api string) *http.Client {
	c := *client
	base := c.Transport
	if base == nil {
		// A nil Transport means the client uses http.DefaultTransport
		// implicitly; wrapping it as-is would panic on the first request.
		base = http.DefaultTransport
	}
	c.Transport = &userAgentTransport{base: base, api: api}
	return &c
}
