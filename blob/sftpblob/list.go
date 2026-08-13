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

package sftpblob

import (
	"context"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/TuSKan/gocloud-ext/internal/escape"
	"github.com/pkg/sftp"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

// ListPaged implements driver.Bucket.
//
// The tree is walked depth-first, stopping as soon as a page is full and
// skipping subtrees that cannot contribute to it, so the cost of a page is
// proportional to the page rather than to the size of the bucket. The previous
// implementation re-walked the tree from the prefix root for every page and
// issued an extra round trip per object to read its sidecar purely for an MD5,
// which put a thousand extra requests behind a default-sized page.
func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	l := &lister{
		b:         b,
		ctx:       ctx,
		prefix:    opts.Prefix,
		delimiter: opts.Delimiter,
		pageSize:  pageSize,
		pageToken: string(opts.PageToken),
		before:    opts.BeforeList,
	}
	// Start at the deepest directory that can contain the prefix; nothing
	// above it can match.
	root := ""
	if i := strings.LastIndex(opts.Prefix, "/"); i >= 0 {
		root = escape.KeyEscape(opts.Prefix[:i])
	}
	if err := l.visit(root); err != nil {
		return nil, err
	}
	return l.page(), nil
}

// lister accumulates one page of List results while walking the directory tree.
type lister struct {
	b         *bucket
	ctx       context.Context
	prefix    string
	delimiter string
	pageSize  int
	pageToken string
	// before is the BeforeList hook, which the driver contract requires be
	// called exactly once. The previous implementation never called it at all.
	before func(asFunc func(any) bool) error

	objects    []*driver.ListObject
	lastPrefix string
	full       bool
}

type listChild struct {
	key     string
	sortKey string
	path    string
	info    fs.FileInfo
}

// readDir lists one directory, relative to the bucket root.
func (l *lister) readDir(dir string) ([]fs.FileInfo, error) {
	if l.b.readDir != nil {
		return l.b.readDir(l.b.fullDir(dir))
	}
	return l.b.client.ReadDir(l.b.fullDir(dir))
}

func (l *lister) takeBefore() error {
	if l.before == nil {
		return nil
	}
	before := l.before
	l.before = nil
	return before(func(i any) bool {
		if c, ok := i.(**sftp.Client); ok {
			*c = l.b.client
			return true
		}
		return false
	})
}

// visit lists one directory and recurses into the ones that can still
// contribute to the page.
func (l *lister) visit(dir string) error {
	if err := l.ctx.Err(); err != nil {
		return err
	}
	if err := l.takeBefore(); err != nil {
		return err
	}
	infos, err := l.readDir(dir)
	if err != nil {
		if l.b.ErrorCode(err) == gcerrors.NotFound {
			// The directory doesn't exist, or vanished mid-walk; either way it
			// contributes nothing. List is only eventually consistent.
			return nil
		}
		return err
	}

	children := make([]listChild, 0, len(infos))
	for _, info := range infos {
		rel := info.Name()
		if dir != "" {
			rel = dir + "/" + rel
		}
		if isInternalPath(rel) {
			continue
		}
		key := escape.KeyUnescape(rel)
		// A directory sorts as though its key ended in "/", because every key
		// beneath it does. That is what orders "t-/t." before "t/t/t"; sorting
		// by bare name gets it backwards, which the old walk then papered over
		// with a sort on every append.
		sortKey := key
		if info.IsDir() {
			sortKey += "/"
		}
		children = append(children, listChild{key: key, sortKey: sortKey, path: rel, info: info})
	}
	sort.Slice(children, func(i, j int) bool { return children[i].sortKey < children[j].sortKey })

	for _, c := range children {
		if l.full {
			return nil
		}
		asFunc := infoAsFunc(c.info)
		if !c.info.IsDir() {
			l.emit(c.key, &driver.ListObject{
				Key:     c.key,
				ModTime: c.info.ModTime(),
				Size:    c.info.Size(),
				AsFunc:  asFunc,
			}, asFunc)
			continue
		}
		if !l.descend(c.key) {
			continue
		}
		if l.collapses(c.key) {
			// Everything under this directory folds into a single "directory"
			// result, so the subtree only has to be probed, not enumerated.
			ok, err := l.hasKey(c.path)
			if err != nil {
				return err
			}
			if ok {
				l.emit(c.key+"/", nil, asFunc)
			}
			continue
		}
		if err := l.visit(c.path); err != nil {
			return err
		}
	}
	return nil
}

// hasKey reports whether a directory's subtree holds at least one blob.
// Deleting a blob leaves its directory behind, and an empty directory is not a
// "directory" as far as the blob API is concerned. The search stops at the
// first key it finds, so a populated directory costs a single request.
func (l *lister) hasKey(dir string) (bool, error) {
	infos, err := l.readDir(dir)
	if err != nil {
		if l.b.ErrorCode(err) == gcerrors.NotFound {
			return false, nil
		}
		return false, err
	}
	var subdirs []string
	for _, info := range infos {
		rel := dir + "/" + info.Name()
		if isInternalPath(rel) {
			continue
		}
		if !info.IsDir() {
			return true, nil
		}
		subdirs = append(subdirs, rel)
	}
	for _, sub := range subdirs {
		ok, err := l.hasKey(sub)
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
func (l *lister) emit(key string, obj *driver.ListObject, asFunc func(any) bool) {
	if !strings.HasPrefix(key, l.prefix) {
		return
	}
	if l.delimiter != "" {
		rest := key[len(l.prefix):]
		if idx := strings.Index(rest, l.delimiter); idx != -1 {
			dir := l.prefix + rest[:idx+len(l.delimiter)]
			if dir == l.lastPrefix {
				return
			}
			// A "directory" is synthesized, but on a filesystem it still has a
			// real FileInfo behind it, so As keeps working for it.
			obj = &driver.ListObject{Key: dir, IsDir: true, AsFunc: asFunc}
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

// descend reports whether a directory's subtree can still contribute results.
// Every key beneath a directory starts with its key plus "/", which is what
// makes both of these tests sound.
func (l *lister) descend(dirKey string) bool {
	sub := dirKey + "/"
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

// collapses reports whether every key under a directory folds into a single
// "directory" result, letting the walk skip the subtree. This holds only for
// the "/" delimiter, the one that lines up with the directory hierarchy; any
// other delimiter needs the keys themselves.
func (l *lister) collapses(dirKey string) bool {
	sub := dirKey + "/"
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

// infoAsFunc exposes a fs.FileInfo through the As mechanism.
func infoAsFunc(info fs.FileInfo) func(any) bool {
	return func(i any) bool {
		if p, ok := i.(*fs.FileInfo); ok {
			*p = info
			return true
		}
		return false
	}
}

// isInternalPath reports whether a path holds sftpblob's own bookkeeping rather
// than a blob: an attributes sidecar, or an in-progress write.
func isInternalPath(p string) bool {
	return strings.HasSuffix(p, attrsExt) || strings.Contains(p, tempInfix)
}

// fullDir turns a bucket-relative escaped path into a remote path.
func (b *bucket) fullDir(rel string) string {
	if rel == "" {
		return b.dir
	}
	return path.Join(b.dir, rel)
}
