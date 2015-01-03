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
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

type Option func(*S3Fs)

func NewS3Fs(options ...Option) *S3Fs {
	s := new(S3Fs)

	Region(aws.USEast)(s) // set default region

	for _, opt := range options {
		opt(s)
	}
	return s
}

func S3FsFromBucket(b s3.Bucket) *S3Fs {
	return NewS3Fs(Bucket(b.Name), Auth(b.Auth), Region(b.Region))
}

func S3FileFromBucket(n string, b s3.Bucket) *S3File {
	return &S3File{n, b, nil, nil}
}

func Auth(auth aws.Auth) Option {
	return func(s *S3Fs) {
		s.auth = auth
	}
}

func Region(region aws.Region) Option {
	return func(s *S3Fs) {
		s.region = region
	}
}

func EnvAuth() Option {
	return func(s *S3Fs) {
		s.auth, _ = aws.GetAuth("", "", "", time.Time{})
	}
}

func Bucket(name string) Option {
	// TODO add a `verify` option to run HEAD on the bucket to ensure
	// it exists
	return func(s *S3Fs) {
		s.bucketName = name
	}
}

func (s S3Fs) s3() *s3.S3 {
	return s3.New(s.auth, s.region)
}
func (s S3Fs) bucket() *s3.Bucket {
	return s.s3().Bucket(s.bucketName)
}
