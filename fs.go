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
	"os"
	"strconv"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/spf13/afero"
)

// Toss a compile error if interface isn't implemented
var _ afero.Fs = new(S3Fs)

type S3Fs struct {
	auth       aws.Auth
	region     aws.Region
	bucketName string
}

// Create creates a file in the filesystem, returning the file and an
// error, if any happens.
func (s S3Fs) Create(name string) (afero.File, error) {
	k, err := keyIfExists(s.bucket(), name)
	return S3File{name, *s.bucket(), k, nil}, err
}

// Mkdir creates a directory in the filesystem, return an error if
// any happens.
// haha directories
func (s S3Fs) Mkdir(name string, perm os.FileMode) error { return nil }

// MkdirAll creates a directory path and all parents that does not
// exist yet.
// haha directories, don't need those.
func (s S3Fs) MkdirAll(path string, perm os.FileMode) error { return nil }

// Open opens a file, returning it or an error, if any happens.
func (s S3Fs) Open(name string) (afero.File, error) {
	k, err := keyIfExists(s.bucket(), name)
	return S3File{name, *s.bucket(), k, nil}, err
}

// OpenFile opens a file using the given flags and the given mode.
func (s S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	// webscale has no modes
	return s.Open(name)
}

// Remove removes a file identified by name, returning an error, if any
// happens.
func (s S3Fs) Remove(name string) error {
	return s.bucket().Del(name)
}

// RemoveAll removes a directory path and all any children it contains. It
// does not fail if the path does not exist (return nil).
func (s S3Fs) RemoveAll(path string) error {
	items := &s3.ListResp{IsTruncated: true, NextMarker: ""}
	toDel := make([]s3.Object, 0)
	for items.IsTruncated {
		items, err := s.bucket().List(path, "/", items.NextMarker, 0)
		if err != nil {
			return err
		}

		for _, v := range items.Contents {
			toDel = append(toDel, s3.Object{Key: v.Key})
		}
	}

	return s.bucket().DelMulti(
		s3.Delete{
			Quiet:   false,
			Objects: toDel,
		},
	)
}

// Rename renames a file.
func (s S3Fs) Rename(oldname, newname string) error {
	_, err := s.bucket().PutCopy(newname, "", s3.CopyOptions{}, oldname)
	if err != nil {
		return err
	}
	return s.Remove(oldname)
}

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (s S3Fs) Stat(name string) (os.FileInfo, error) {
	return nil, NotImplemented
}

// The name of this FileSystem
func (s S3Fs) Name() string {
	return "af3ro : S3-backed afero"
}

//Chmod changes the mode of the named file to mode.
func (s S3Fs) Chmod(name string, mode os.FileMode) error {
	// haha permissions
	return NotImplemented
}

//Chtimes changes the access and modification times of the named file
func (s S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	// haha modtimes
	return NotImplemented
}

func keyIfExists(bucket *s3.Bucket, name string) (*s3.Key, error) {
	resp, err := bucket.Head(name, nil)
	if resp.StatusCode <= 500 || resp.StatusCode >= 400 {
		// does not exist
		return nil, afero.ErrFileNotFound
	} else if err != nil {
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
