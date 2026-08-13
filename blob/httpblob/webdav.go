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
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/TuSKan/gocloud-ext/internal/escape"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

// WebDAV methods (RFC 4918).
const (
	methodPropfind = "PROPFIND"
	methodMkcol    = "MKCOL"
	methodCopy     = "COPY"
	methodMove     = "MOVE"

	// depth0 and depth1 are the only Depth values httpblob sends.
	// "Depth: infinity" is optional for servers to support and unbounded in
	// size; see ListPaged.
	depth0 = "0"
	depth1 = "1"
)

// propfindBody asks for exactly the properties a driver.ListObject needs.
const propfindBody = `<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:resourcetype/>
    <D:getcontentlength/>
    <D:getlastmodified/>
    <D:getetag/>
    <D:getcontenttype/>
  </D:prop>
</D:propfind>`

// multistatus is the RFC 4918 §14.16 DAV:multistatus element.
type multistatus struct {
	XMLName   xml.Name      `xml:"DAV: multistatus"`
	Responses []davResponse `xml:"DAV: response"`
}

type davResponse struct {
	Href     string        `xml:"DAV: href"`
	PropStat []davPropStat `xml:"DAV: propstat"`
}

type davPropStat struct {
	Prop   davProp `xml:"DAV: prop"`
	Status string  `xml:"DAV: status"`
}

type davProp struct {
	ResourceType     davResourceType `xml:"DAV: resourcetype"`
	GetContentLength string          `xml:"DAV: getcontentlength"`
	GetLastModified  string          `xml:"DAV: getlastmodified"`
	GetETag          string          `xml:"DAV: getetag"`
	GetContentType   string          `xml:"DAV: getcontenttype"`
}

type davResourceType struct {
	Collection *struct{} `xml:"DAV: collection"`
}

// ok reports whether this propstat carries a 2xx status, i.e. whether its
// properties were actually found.
func (ps davPropStat) ok() bool {
	// Status is a status-line such as "HTTP/1.1 200 OK".
	fields := strings.Fields(ps.Status)
	if len(fields) < 2 {
		return false
	}
	code, err := strconv.Atoi(fields[1])
	return err == nil && code >= 200 && code < 300
}

// davEntry is one resource discovered by a PROPFIND, with its path relative to
// the bucket root, still in escaped-key form.
type davEntry struct {
	path         string
	isCollection bool
	size         int64
	modTime      string
	etag         string
	contentType  string
}

// header synthesizes the equivalent HTTP headers for this entry, for As.
func (e davEntry) header() http.Header {
	h := http.Header{}
	if e.etag != "" {
		h.Set("ETag", e.etag)
	}
	if e.modTime != "" {
		h.Set("Last-Modified", e.modTime)
	}
	if e.contentType != "" {
		h.Set("Content-Type", e.contentType)
	}
	if !e.isCollection {
		h.Set("Content-Length", strconv.FormatInt(e.size, 10))
	}
	return h
}

// propfind issues a PROPFIND at rawURL with the given Depth and returns the
// resources it reports, with paths relative to the bucket root.
func (b *bucket) propfind(ctx context.Context, rawURL, depth string, before func(asFunc func(any) bool) error) ([]davEntry, error) {
	req, err := b.newRequest(ctx, methodPropfind, rawURL, strings.NewReader(propfindBody))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(propfindBody))
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	req.Header.Set("Depth", depth)
	if err := callBefore(before, req); err != nil {
		return nil, err
	}

	resp, err := b.do(ctx, func() (*http.Request, error) {
		r := req.Clone(ctx)
		r.Body = io.NopCloser(strings.NewReader(propfindBody))
		return r, nil
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ms multistatus
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, fmt.Errorf("httpblob: decoding PROPFIND response from %s: %w", rawURL, err)
	}

	entries := make([]davEntry, 0, len(ms.Responses))
	for _, r := range ms.Responses {
		path, ok := b.relativePath(r.Href)
		if !ok {
			continue
		}
		e := davEntry{path: path}
		for _, ps := range r.PropStat {
			if !ps.ok() {
				continue
			}
			if ps.Prop.ResourceType.Collection != nil {
				e.isCollection = true
			}
			if v := ps.Prop.GetContentLength; v != "" {
				if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
					e.size = n
				}
			}
			if v := ps.Prop.GetLastModified; v != "" {
				e.modTime = strings.TrimSpace(v)
			}
			if v := ps.Prop.GetETag; v != "" {
				e.etag = strings.TrimSpace(v)
			}
			if v := ps.Prop.GetContentType; v != "" {
				e.contentType = strings.TrimSpace(v)
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// stat returns the WebDAV properties of a single resource.
//
// It replaces HEAD under ProtocolWebDAV for the same number of requests, and
// answers a question HEAD cannot: whether the key names a collection. Servers
// disagree wildly on what GET/HEAD does to a collection — x/net/webdav answers
// 405, Apache mod_dav serves an autoindex with 200, or 403 with
// "Options -Indexes" — and none of those means "this is a blob". resourcetype
// does.
func (b *bucket) stat(ctx context.Context, objURL string) (davEntry, error) {
	entries, err := b.propfind(ctx, objURL, depth0, nil)
	if err != nil {
		return davEntry{}, err
	}
	for _, e := range entries {
		if e.isCollection {
			// In blob terms a collection is simply not there.
			return davEntry{}, &Error{
				Method:     methodPropfind,
				URL:        objURL,
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Body:       "the key names a collection, not a blob",
			}
		}
		return e, nil
	}
	return davEntry{}, &Error{
		Method:     methodPropfind,
		URL:        objURL,
		StatusCode: http.StatusNotFound,
		Status:     "404 Not Found",
		Body:       "no properties returned",
	}
}

// relativePath converts a DAV:href into a bucket-relative path in escaped-key
// form. It reports false for hrefs outside the bucket, and for the bucket root
// itself.
func (b *bucket) relativePath(href string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return "", false
	}
	// u.Path is the percent-decoded form, which is what our escaped keys look
	// like on the wire.
	p := u.Path
	base := b.baseURL.Path
	if base != "" {
		if !strings.HasPrefix(p, base) {
			return "", false
		}
		p = p[len(base):]
	}
	p = strings.TrimPrefix(p, "/")
	// Collections are reported with a trailing slash; keys never have one.
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return "", false
	}
	return p, true
}

// ListPaged implements driver.Bucket.
//
// The tree is walked depth-first with "Depth: 1", stopping as soon as a page is
// full and skipping subtrees that cannot contribute to it. The cost of a page
// is therefore proportional to the page, not to the size of the bucket. A
// single "Depth: infinity" PROPFIND would fetch a whole subtree in one request,
// but it has to be re-fetched for every page, and RFC 4918 §9.1 lets servers
// refuse it outright — Apache mod_dav does by default.
func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	if b.opts.Protocol != ProtocolWebDAV {
		return nil, b.unimplemented("listing")
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	l := &lister{
		b:         b,
		prefix:    opts.Prefix,
		delimiter: opts.Delimiter,
		pageSize:  pageSize,
		pageToken: string(opts.PageToken),
		before:    opts.BeforeList,
	}
	// Start at the deepest collection that can contain the prefix; nothing
	// above it can match.
	root := ""
	if i := strings.LastIndex(opts.Prefix, "/"); i >= 0 {
		root = escape.KeyEscape(opts.Prefix[:i])
	}
	if err := l.visit(ctx, root); err != nil {
		return nil, err
	}
	return l.page(), nil
}

// lister accumulates one page of List results while walking the collection
// tree.
type lister struct {
	b         *bucket
	prefix    string
	delimiter string
	pageSize  int
	pageToken string
	// before is the BeforeList hook, which must run exactly once; the first
	// PROPFIND takes it.
	before func(asFunc func(any) bool) error

	objects    []*driver.ListObject
	lastPrefix string
	full       bool
}

type listChild struct {
	key     string
	sortKey string
	entry   davEntry
}

func (l *lister) takeBefore() func(asFunc func(any) bool) error {
	before := l.before
	l.before = nil
	return before
}

// visit lists one collection and recurses into the ones that can still
// contribute to the page.
func (l *lister) visit(ctx context.Context, dir string) error {
	entries, err := l.b.propfind(ctx, l.b.pathURL(dir), depth1, l.takeBefore())
	if err != nil {
		if l.b.ErrorCode(err) == gcerrors.NotFound {
			// The collection doesn't exist, or vanished mid-walk. Either way
			// it contributes nothing; List is only eventually consistent.
			return nil
		}
		return err
	}

	children := make([]listChild, 0, len(entries))
	for _, e := range entries {
		// A Depth: 1 PROPFIND reports the collection itself; skip it so the
		// walk terminates.
		if e.path == dir || isInternalPath(e.path) {
			continue
		}
		key := escape.KeyUnescape(e.path)
		// A collection sorts as though its key ended in "/", because every key
		// beneath it does. That is what orders "t-/t." before "t/t/t".
		sortKey := key
		if e.isCollection {
			sortKey += "/"
		}
		children = append(children, listChild{key: key, sortKey: sortKey, entry: e})
	}
	sort.Slice(children, func(i, j int) bool { return children[i].sortKey < children[j].sortKey })

	for _, c := range children {
		if l.full {
			return nil
		}
		if !c.entry.isCollection {
			header := c.entry.header()
			l.emit(c.key, &driver.ListObject{
				Key:     c.key,
				ModTime: parseModTime(header),
				Size:    c.entry.size,
				AsFunc:  headerAsFunc(header),
			})
			continue
		}
		if !l.descend(c.key) {
			continue
		}
		if l.collapses(c.key) {
			// Everything under this collection folds into a single "directory"
			// result, so the subtree only has to be probed, not enumerated.
			ok, err := l.hasKey(ctx, c.entry.path)
			if err != nil {
				return err
			}
			if ok {
				l.emit(c.key+"/", nil)
			}
			continue
		}
		if err := l.visit(ctx, c.entry.path); err != nil {
			return err
		}
	}
	return nil
}

// hasKey reports whether a collection's subtree holds at least one blob.
// A "directory" result must not be reported for a subtree with no keys in it:
// deleting a blob leaves its collection behind, and an empty collection is not
// a directory as far as the blob API is concerned. The search stops at the
// first key found, so a populated directory costs a single request.
func (l *lister) hasKey(ctx context.Context, dir string) (bool, error) {
	entries, err := l.b.propfind(ctx, l.b.pathURL(dir), depth1, nil)
	if err != nil {
		if l.b.ErrorCode(err) == gcerrors.NotFound {
			return false, nil
		}
		return false, err
	}
	var subdirs []string
	for _, e := range entries {
		if e.path == dir || isInternalPath(e.path) {
			continue
		}
		if !e.isCollection {
			return true, nil
		}
		subdirs = append(subdirs, e.path)
	}
	for _, sub := range subdirs {
		ok, err := l.hasKey(ctx, sub)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

// emit applies the prefix, delimiter, page token and page size rules to one
// candidate key, mirroring memblob's ListPaged, which is the reference for
// these semantics. obj is the result to add when the key is not folded into a
// "directory"; it may be nil when the caller knows folding is certain.
func (l *lister) emit(key string, obj *driver.ListObject) {
	if !strings.HasPrefix(key, l.prefix) {
		return
	}
	if l.delimiter != "" {
		rest := key[len(l.prefix):]
		if idx := strings.Index(rest, l.delimiter); idx != -1 {
			dir := l.prefix + rest[:idx+len(l.delimiter)]
			// All keys in a "directory" collapse to one result.
			if dir == l.lastPrefix {
				return
			}
			obj = &driver.ListObject{Key: dir, IsDir: true}
			l.lastPrefix = dir
		}
	}
	if obj == nil {
		return
	}
	if l.pageToken != "" && obj.Key <= l.pageToken {
		return
	}
	if len(l.objects) == l.pageSize {
		l.full = true
		return
	}
	l.objects = append(l.objects, obj)
}

// descend reports whether a collection's subtree can still contribute results.
// Every key beneath a collection starts with its key plus "/", which is what
// makes both of these tests sound.
func (l *lister) descend(collectionKey string) bool {
	sub := collectionKey + "/"
	// The subtree is only relevant if it overlaps the prefix.
	if !strings.HasPrefix(sub, l.prefix) && !strings.HasPrefix(l.prefix, sub) {
		return false
	}
	// Skip subtrees lying entirely at or before the page token. A token inside
	// the subtree means there is more of it left to return.
	if l.pageToken != "" && l.pageToken >= sub && !strings.HasPrefix(l.pageToken, sub) {
		return false
	}
	return true
}

// collapses reports whether every key under a collection folds into a single
// "directory" result, letting the walk skip the subtree entirely. This holds
// only for the "/" delimiter, the one that lines up with the collection
// hierarchy; any other delimiter needs the keys themselves.
func (l *lister) collapses(collectionKey string) bool {
	sub := collectionKey + "/"
	return l.delimiter == "/" && strings.HasPrefix(sub, l.prefix) && len(sub) > len(l.prefix)
}

func (l *lister) page() *driver.ListPage {
	page := &driver.ListPage{Objects: l.objects}
	// A token is only set when the walk stopped early, i.e. there is more.
	if l.full && len(l.objects) > 0 {
		page.NextPageToken = []byte(l.objects[len(l.objects)-1].Key)
	}
	return page
}

// isInternalPath reports whether a path holds httpblob's own bookkeeping rather
// than a blob: an attributes sidecar, or an in-progress write.
func isInternalPath(path string) bool {
	return strings.HasSuffix(path, attrsExt) || strings.Contains(path, tempInfix)
}

// ensureCollections creates any missing ancestor collections of escapedPath.
// WebDAV PUT fails with 409 when the parent collection does not exist, so this
// is the equivalent of MkdirAll.
func (b *bucket) ensureCollections(ctx context.Context, escapedPath string) error {
	segments := strings.Split(escapedPath, "/")
	if len(segments) < 2 {
		return nil
	}
	b.mkcolMu.Lock()
	defer b.mkcolMu.Unlock()
	for i := 1; i < len(segments); i++ {
		dir := strings.Join(segments[:i], "/")

		b.mu.Lock()
		known := b.knownCollections[dir]
		b.mu.Unlock()
		if known {
			continue
		}
		if err := b.mkcol(ctx, b.pathURL(dir)); err != nil {
			return err
		}
		b.mu.Lock()
		b.knownCollections[dir] = true
		b.mu.Unlock()
	}
	return nil
}

// mkcol creates a collection. An existing collection is not an error: RFC 4918
// §9.3.1 has the server answer 405 when the resource already exists.
func (b *bucket) mkcol(ctx context.Context, rawURL string) error {
	resp, err := b.do(ctx, func() (*http.Request, error) {
		return b.newRequest(ctx, methodMkcol, rawURL, nil)
	})
	if err != nil {
		if b.ErrorCode(err) == gcerrors.Unimplemented {
			// 405: already exists.
			return nil
		}
		return err
	}
	return resp.Body.Close()
}

// move renames srcURL to dstURL. When overwrite is false the server answers 412
// if the destination exists, which is how WriterOptions.IfNotExist is enforced.
func (b *bucket) move(ctx context.Context, srcURL, dstURL string, overwrite bool) error {
	resp, err := b.do(ctx, func() (*http.Request, error) {
		req, err := b.newRequest(ctx, methodMove, srcURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Destination", dstURL)
		req.Header.Set("Overwrite", overwriteHeader(overwrite))
		return req, nil
	})
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// copyOne copies srcURL to dstURL, overwriting the destination.
func (b *bucket) copyOne(ctx context.Context, srcURL, dstURL string, before func(asFunc func(any) bool) error) error {
	req, err := b.newRequest(ctx, methodCopy, srcURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Destination", dstURL)
	req.Header.Set("Overwrite", overwriteHeader(true))
	// The blobs we copy are never collections, but be explicit: RFC 4918 §9.8.3
	// leaves the default Depth for COPY as "infinity".
	req.Header.Set("Depth", "0")
	if err := callBefore(before, req); err != nil {
		return err
	}
	resp, err := b.do(ctx, func() (*http.Request, error) { return req.Clone(ctx), nil })
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func overwriteHeader(overwrite bool) string {
	if overwrite {
		return "T"
	}
	return "F"
}
