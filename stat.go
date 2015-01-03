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
	"net/http"
	"os"
	"time"
)

var _ os.FileInfo = S3FileInfo{}

type S3FileInfo struct{ sourceFile *S3File }

func (s S3FileInfo) Name() string {
	return s.sourceFile.key.Key
}

func (s S3FileInfo) Size() int64 {
	return s.sourceFile.key.Size
}

func (s S3FileInfo) ModTime() time.Time {
	return time.Time{}
	t, err := time.Parse(http.TimeFormat, s.sourceFile.key.LastModified)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (s S3FileInfo) IsDir() bool {
	// the cloud has no directories
	return false
}

func (s S3FileInfo) Mode() os.FileMode {
	return 0777
}

func (s S3FileInfo) Sys() interface{} {
	return nil
}
