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
	"strconv"
	"strings"

	"github.com/goamz/goamz/s3"
	"github.com/spf13/afero"
)

func getEtag(name string, bucket *s3.Bucket) (string, error) {
	resp, err := headName(name, bucket)
	if err != nil {
		return "", err
	}
	return strings.Replace(
		resp.Header.Get("ETag"), "\"", "", 0,
	), nil
}

func keyIfExists(name string, bucket *s3.Bucket) (k *s3.Key, err error) {
	resp, err := headName(name, bucket)
	if err != nil {
		return nil, err
	}
	objSize, _ := strconv.Atoi(resp.Header.Get("Content-Length"))
	return &s3.Key{
		Key:          name,
		LastModified: resp.Header.Get("Last-Modified"),
		Size:         int64(objSize),
		ETag:         resp.Header.Get("ETag"),
	}, nil
}

func headName(name string, bucket *s3.Bucket) (*http.Response, error) {
	resp, err := bucket.Head(name, make(map[string][]string))
	if err != nil && err.Error() == "404 Not Found" {
		return nil, afero.ErrFileNotFound
	} else if err != nil {
		return nil, err
	}
	return resp, err
}

type PermU uint

const (
	uRead  PermU = (9 - 1 - iota) // Rwxrwxrwx
	uWrite                        // rWxrwxrwx
	uExec                         // rwXrwxrwx
	gRead                         // rwxRwxrwx
	gWrite                        // rwxrWxrwx
	gExec                         // rwxrwXrwx
	oRead                         // rwxrwxRwx
	oWrite                        // rwxrwxrWx
	oExec                         // rwxrwxrwX

)

func getACL(m os.FileMode) s3.ACL {
	// does nothing for exec permissions
	switch {
	case m&(1<<oWrite) != 0:
		return s3.PublicReadWrite
	case m&(1<<oRead) != 0:
		return s3.PublicRead
	case m&(1<<gWrite) != 0:
		return s3.BucketOwnerFull
	case m&(1<<gRead) != 0:
		return s3.BucketOwnerRead
	default:
		return s3.Private
	}
}
