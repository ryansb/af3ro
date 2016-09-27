// Copyright © 2014 Ryan Brown <sb@ryansb.com>
// Copyright © 2014 Steve Francia <spf@spf13.com>.
// Copyright © 2013 tsuru authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package af3ro provides an afero-compliant interface to AWS S3.

package af3ro

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/goamz/goamz/s3"
	"github.com/spf13/afero"
)

// Toss a compile error if interface isn't implemented
var _ afero.File = new(InMemoryFile)

type MemDir interface {
	Len() int
	Names() []string
	Files() []afero.File
	Add(afero.File)
	Remove(afero.File)
}

type InMemoryFile struct {
	at      int64
	name    string
	data    []byte
	memDir  MemDir
	dir     bool
	closed  bool
	mode    os.FileMode
	modtime time.Time
	bucket  *s3.Bucket
}

func MemFileCreate(name string, bucket *s3.Bucket) *InMemoryFile {
	return &InMemoryFile{
		name:    name,
		mode:    0640,
		modtime: time.Now(),
		bucket:  bucket,
	}
}

func (f *InMemoryFile) Open() error {
	atomic.StoreInt64(&f.at, 0)
	f.closed = false
	return nil
}

func (f *InMemoryFile) Sync() error {
        return nil
}

func (f *InMemoryFile) Close() (err error) {
	atomic.StoreInt64(&f.at, 0)
	f.closed = true

	hasher := md5.New()
	hasher.Write(f.data)
	expected := fmt.Sprintf("\"%x\"", hasher.Sum([]byte{}))
	etag, err := getEtag(f.Name(), f.bucket)
	if err != nil {
		fmt.Println("Failure getting file etag", f.Name(), "Error is", err)
		return err
	}

	if etag == string(expected) {
		// the file hasn't actually changed
		return nil
	}

	err = f.bucket.Put(
		f.Name(), f.data,
		"", // TODO: use content-type
		getACL(f.mode),
		s3.Options{},
	)
	if err != nil {
		fmt.Println("Failure writing file", f.Name(), "Error is", err)
	}

	return
}

func (f *InMemoryFile) Name() string {
	return f.name
}

func (f *InMemoryFile) Stat() (os.FileInfo, error) {
	return &InMemoryFileInfo{f}, nil
}

func (f *InMemoryFile) Readdir(count int) (res []os.FileInfo, err error) {
	files := f.memDir.Files()
	limit := len(files)

	if len(files) == 0 {
		return
	}

	if count > 0 {
		limit = count
	}

	if len(files) < limit {
		err = io.EOF
	}

	res = make([]os.FileInfo, f.memDir.Len())

	i := 0
	for _, file := range f.memDir.Files() {
		res[i], _ = file.Stat()
		i++
	}
	return res, nil
}

func (f *InMemoryFile) Readdirnames(n int) (names []string, err error) {
	fi, err := f.Readdir(n)
	names = make([]string, len(fi))
	for i, f := range fi {
		names[i] = f.Name()
	}
	return names, err
}

func (f *InMemoryFile) Read(b []byte) (n int, err error) {
	if f.closed == true {
		return 0, afero.ErrFileClosed
	}
	if len(f.data) == 0 {
		f.data, err = f.bucket.Get(f.Name())
		if err != nil {
			// failed to get data from s3
			return 0, err
		}
		atomic.StoreInt64(&f.at, 0)
	}
	if len(f.data)-int(f.at) >= len(b) {
		n = len(b)
	} else {
		n = len(f.data) - int(f.at)
		err = io.EOF
	}
	copy(b, f.data[f.at:f.at+int64(n)])
	atomic.AddInt64(&f.at, int64(n))
	return
}

func (f *InMemoryFile) ReadAt(b []byte, off int64) (n int, err error) {
	atomic.StoreInt64(&f.at, off)
	return f.Read(b)
}

func (f *InMemoryFile) Truncate(size int64) error {
	if f.closed == true {
		return afero.ErrFileClosed
	}
	if size < 0 {
		return afero.ErrOutOfRange
	}
	if size > int64(len(f.data)) {
		diff := size - int64(len(f.data))
		f.data = append(f.data, bytes.Repeat([]byte{00}, int(diff))...)
	} else {
		f.data = f.data[0:size]
	}
	return nil
}

func (f *InMemoryFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed == true {
		return 0, afero.ErrFileClosed
	}
	switch whence {
	case 0:
		atomic.StoreInt64(&f.at, offset)
	case 1:
		atomic.AddInt64(&f.at, int64(offset))
	case 2:
		atomic.StoreInt64(&f.at, int64(len(f.data))+offset)
	}
	return f.at, nil
}

func (f *InMemoryFile) Write(b []byte) (n int, err error) {
	n = len(b)
	cur := atomic.LoadInt64(&f.at)
	diff := cur - int64(len(f.data))
	var tail []byte
	if n+int(cur) < len(f.data) {
		tail = f.data[n+int(cur):]
	}
	if diff > 0 {
		f.data = append(bytes.Repeat([]byte{00}, int(diff)), b...)
		f.data = append(f.data, tail...)
	} else {
		f.data = append(f.data[:cur], b...)
		f.data = append(f.data, tail...)
	}

	atomic.StoreInt64(&f.at, int64(len(f.data)))
	return
}

func (f *InMemoryFile) WriteAt(b []byte, off int64) (n int, err error) {
	atomic.StoreInt64(&f.at, off)
	return f.Write(b)
}

func (f *InMemoryFile) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *InMemoryFile) Info() *InMemoryFileInfo {
	return &InMemoryFileInfo{file: f}
}

type InMemoryFileInfo struct {
	file *InMemoryFile
}

// Implements os.FileInfo
func (s *InMemoryFileInfo) Name() string       { return s.file.Name() }
func (s *InMemoryFileInfo) Mode() os.FileMode  { return s.file.mode }
func (s *InMemoryFileInfo) ModTime() time.Time { return s.file.modtime }
func (s *InMemoryFileInfo) IsDir() bool        { return s.file.dir }
func (s *InMemoryFileInfo) Sys() interface{}   { return nil }
func (s *InMemoryFileInfo) Size() int64 {
	if s.IsDir() {
		return int64(42)
	}
	return int64(len(s.file.data))
}
