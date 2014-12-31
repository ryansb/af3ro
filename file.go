// Copyright © 2014 Ryan Brown <sb@ryansb.com>
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
	"io"
	"os"
	"strings"

	"github.com/goamz/goamz/s3"
	"github.com/spf13/afero"
)

type S3File struct {
	sourceKey     string
	bucket        *s3.Bucket
	key           *s3.Key
	contentBuffer *bytes.Buffer
}

func (s S3File) Close() error {
	// can't close the cloud
	return nil
}

func (s S3File) Name() string {
	return s.bucket.URL(s.sourceKey)
}

func (s S3File) Read(p []byte) (n int, e error) {
	if s.contentBuffer == nil {
		s.contentBuffer = new(bytes.Buffer)
	}
	rdr, e := s.bucket.GetReader(s.sourceKey)
	if e != nil {
		return
	}
	// tee the read to a local buffer so we can read it back and use
	// it for seeking around
	tr := io.TeeReader(rdr, s.contentBuffer)
	return tr.Read(p)
}

func (s S3File) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, NotImplemented
}

func (s S3File) Readdir(count int) ([]os.FileInfo, error) {
	return nil, NotImplemented
}

func (s S3File) Readdirnames(n int) ([]string, error) {
	return nil, NotImplemented
}

func (s S3File) Seek(offset int64, whence int) (int64, error) {
	return 0, NotImplemented
}

func (s S3File) Write(p []byte) (n int, e error) {
	hasher := md5.New()
	hasher.Write(p)
	etag := hasher.Sum([]byte{})
	if s.key != nil && string(etag) == strings.Replace(s.key.ETag, "\"", "", 0) {
		// the md5 of the content to be written and the content already in S3 match.
		// pretend like it worked
		return len(p), nil
	}
	return len(p), s.bucket.Put(s.sourceKey, p, "", "", s3.Options{})
}

func (s S3File) WriteAt(p []byte, off int64) (n int, e error) {
	return 0, NotImplemented
}

func (s S3File) WriteString(p string) (n int, e error) {
	return s.Write([]byte(p))
}

func (s S3File) Stat() (os.FileInfo, error) {
	return nil, NotImplemented
}

func (s S3File) Truncate(size int64) error {
	return NotImplemented
}

var _ afero.File = new(S3File)
