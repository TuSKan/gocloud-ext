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

package httpblob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"gocloud.dev/gcerrors"
)

const attrsExt = ".attrs"

var errAttrsExt = fmt.Errorf("file extension %q is reserved", attrsExt)

// defaultContentType is used when nothing better is known about a blob.
const defaultContentType = "application/octet-stream"

// xattrs stores extended attributes for an object. The format is like
// filesystem extended attributes, see
// https://www.freedesktop.org/wiki/CommonExtendedAttributes.
//
// It is deliberately identical to the struct used by fileblob and sftpblob, so
// that a bucket written by one of them and served over HTTP is readable here.
type xattrs struct {
	CacheControl       string            `json:"user.cache_control"`
	ContentDisposition string            `json:"user.content_disposition"`
	ContentEncoding    string            `json:"user.content_encoding"`
	ContentLanguage    string            `json:"user.content_language"`
	ContentType        string            `json:"user.content_type"`
	Metadata           map[string]string `json:"user.metadata"`
	MD5                []byte            `json:"md5"`
}

// getAttrs fetches the sidecar for the object at objURL, which must already be
// escaped. A sidecar that is missing, or that isn't one of ours, is not an
// error; it yields zero attributes, and the caller falls back to whatever the
// response headers say. Other failures are reported, since they say something
// about the server rather than about the blob.
func (b *bucket) getAttrs(ctx context.Context, objURL string) (xattrs, error) {
	if !b.useSidecar() {
		return xattrs{}, nil
	}
	resp, err := b.do(ctx, func() (*http.Request, error) {
		return b.newRequest(ctx, http.MethodGet, objURL+attrsExt, nil)
	})
	if err != nil {
		if b.ErrorCode(err) == gcerrors.NotFound {
			return xattrs{}, nil
		}
		return xattrs{}, err
	}
	defer resp.Body.Close()

	var xa xattrs
	// The sidecar is written by us and is small; cap the read so a
	// misconfigured server can't stream us an unbounded body.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSidecarSize))
	if err != nil {
		return xattrs{}, err
	}
	if err := json.Unmarshal(body, &xa); err != nil {
		// The response wasn't a sidecar we wrote. Servers commonly answer a
		// missing path with a friendly HTML page and a 200 rather than a 404,
		// and failing every read of a perfectly good blob because its optional
		// metadata is unreadable would be far worse than reporting no
		// metadata. Fall back to the response headers.
		return xattrs{}, nil
	}
	return xa, nil
}

// maxSidecarSize bounds how much of a sidecar response we will read.
const maxSidecarSize = 1 << 20

// putAttrs writes the sidecar for the object at objURL, which must already be
// escaped.
func (b *bucket) putAttrs(ctx context.Context, objURL string, xa xattrs) error {
	if !b.useSidecar() {
		return nil
	}
	body, err := json.Marshal(xa)
	if err != nil {
		return err
	}
	resp, err := b.do(ctx, func() (*http.Request, error) {
		req, err := b.newRequest(ctx, http.MethodPut, objURL+attrsExt, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// useSidecar reports whether this bucket reads and writes .attrs sidecars.
func (b *bucket) useSidecar() bool {
	return b.opts.Protocol == ProtocolWebDAV && b.opts.Metadata == MetadataInSidecar
}

// fetchAttrs starts fetching the sidecar for objURL and returns a function that
// waits for the result. Reads need both the object and its sidecar, and neither
// depends on the other, so overlapping them halves the latency of a read.
// The returned function may be called exactly once; callers must call it even
// on an error path so the request is accounted for.
func (b *bucket) fetchAttrs(ctx context.Context, objURL string) func() (xattrs, error) {
	if !b.useSidecar() {
		return func() (xattrs, error) { return xattrs{}, nil }
	}
	type result struct {
		xa  xattrs
		err error
	}
	ch := make(chan result, 1)
	go func() {
		xa, err := b.getAttrs(ctx, objURL)
		ch <- result{xa, err}
	}()
	return func() (xattrs, error) {
		r := <-ch
		return r.xa, r.err
	}
}

// contentTypeFor picks the ContentType to report. A stored value wins: servers
// type-sniff from the file extension and will disagree with what was written.
func contentTypeFor(xa xattrs, header http.Header) string {
	return firstNonEmpty(xa.ContentType, header.Get("Content-Type"), defaultContentType)
}
