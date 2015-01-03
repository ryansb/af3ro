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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"github.com/spf13/afero"
)

// Toss a compile error if interface isn't implemented
var _ afero.Fs = new(MemS3Fs)

var mux = &sync.Mutex{}

type MemS3Fs struct {
	auth       aws.Auth
	region     aws.Region
	bucketName string
	data       map[string]afero.File
	mutex      *sync.RWMutex
}

func (m *MemS3Fs) lock() {
	mx := m.getMutex()
	mx.Lock()
}
func (m *MemS3Fs) unlock()  { m.getMutex().Unlock() }
func (m *MemS3Fs) rlock()   { m.getMutex().RLock() }
func (m *MemS3Fs) runlock() { m.getMutex().RUnlock() }

func (m *MemS3Fs) getData() map[string]afero.File {
	if m.data == nil {
		m.data = make(map[string]afero.File)
	}
	return m.data
}

func (m *MemS3Fs) getMutex() *sync.RWMutex {
	mux.Lock()
	if m.mutex == nil {
		m.mutex = &sync.RWMutex{}
	}
	mux.Unlock()
	return m.mutex
}

type MemDirMap map[string]afero.File

func (m MemDirMap) Len() int            { return len(m) }
func (m MemDirMap) Add(f afero.File)    { m[f.Name()] = f }
func (m MemDirMap) Remove(f afero.File) { delete(m, f.Name()) }
func (m MemDirMap) Files() (files []afero.File) {
	for _, f := range m {
		files = append(files, f)
	}
	return files
}

func (m MemDirMap) Names() (names []string) {
	for x := range m {
		names = append(names, x)
	}
	return names
}

func (MemS3Fs) Name() string { return "MemS3Fs: s3-backed memfs" }

func (m *MemS3Fs) Create(name string) (afero.File, error) {
	m.lock()
	m.getData()[name] = MemFileCreate(name, m.bucket())
	m.unlock()
	m.registerDirs(m.getData()[name])
	return m.getData()[name], nil
}

func (m *MemS3Fs) registerDirs(f afero.File) {
	var x = f.Name()
	for x != "/" {
		f := m.registerWithParent(f)
		if f == nil {
			break
		}
		x = f.Name()
	}
}

func (m *MemS3Fs) unRegisterWithParent(f afero.File) afero.File {
	parent := m.findParent(f)
	pmem := parent.(*InMemoryFile)
	pmem.memDir.Remove(f)
	return parent
}

func (m *MemS3Fs) findParent(f afero.File) afero.File {
	dirs, _ := path.Split(f.Name())
	if len(dirs) > 1 {
		_, parent := path.Split(path.Clean(dirs))
		if len(parent) > 0 {
			pfile, err := m.Open(parent)
			if err != nil {
				return pfile
			}
		}
	}
	return nil
}

func (m *MemS3Fs) registerWithParent(f afero.File) afero.File {
	if f == nil {
		return nil
	}
	parent := m.findParent(f)
	if parent != nil {
		pmem := parent.(*InMemoryFile)
		pmem.memDir.Add(f)
	} else {
		pdir := filepath.Dir(path.Clean(f.Name()))
		m.Mkdir(pdir, 0777)
	}
	return parent
}

// Mkdir doesn't actually save anything to S3 unless they have
// contents. The cloud doesn't have directories.
func (m *MemS3Fs) Mkdir(name string, perm os.FileMode) error {
	m.rlock()
	d, ok := m.getData()[name]
	m.runlock()
	if ok {
		if imf, o := d.(*InMemoryFile); o && imf.memDir != nil {
			// the directory exists but that's ok
			return nil
		}
		return afero.ErrFileExists
	} else {
		m.lock()
		m.getData()[name] = &InMemoryFile{name: name, memDir: &MemDirMap{}, dir: true}
		m.unlock()
		m.registerDirs(m.getData()[name])
	}
	return nil
}

// MkdirAll doesn't actually save anything to S3 unless they have
// contents. The cloud doesn't have directories.
func (m *MemS3Fs) MkdirAll(path string, perm os.FileMode) error {
	return m.Mkdir(path, 0777)
}

func (m *MemS3Fs) Open(name string) (afero.File, error) {
	m.rlock()
	f, ok := m.getData()[name]
	ff, ok := f.(*InMemoryFile)
	if ok {
		ff.Open()
	}
	m.runlock()

	if ok {
		return f, nil
	} else {
		return nil, afero.ErrFileNotFound
	}
}

// OpenFile ignores the `flag` argument but respects `perm`
func (m *MemS3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	f, err := m.Open(name)
	if err != nil {
		return f, err
	}
	err = m.Chmod(f.Name(), perm)
	return f, err
}

// Removes file immediately from both S3 and the local cache
func (m *MemS3Fs) Remove(name string) error {
	m.rlock()
	defer m.runlock()

	m.bucket().Del(name)
	if _, ok := m.getData()["name"]; ok {
		m.lock()
		delete(m.getData(), name)
		m.unlock()
	}
	return nil
}

func (m *MemS3Fs) RemoveAll(path string) error {
	m.rlock()
	defer m.runlock()
	for p, _ := range m.getData() {
		if strings.HasPrefix(p, path) {
			m.runlock()
			m.lock()
			delete(m.getData(), p)
			m.unlock()
			m.rlock()
		}
	}
	items := &s3.ListResp{IsTruncated: true, NextMarker: ""}
	toDel := make([]s3.Object, 0)
	for items.IsTruncated {
		items, err := m.bucket().List(path, "/", items.NextMarker, 0)
		if err != nil {
			return err
		}

		for _, v := range items.Contents {
			toDel = append(toDel, s3.Object{Key: v.Key})
		}
	}
	return m.bucket().DelMulti(
		s3.Delete{
			Quiet:   false,
			Objects: toDel,
		},
	)
}

func (m *MemS3Fs) Rename(oldname, newname string) error {
	m.rlock()
	defer m.runlock()
	if _, ok := m.getData()[oldname]; ok {
		if _, ok := m.getData()[newname]; !ok {
			m.runlock()
			m.lock()
			m.getData()[newname] = m.getData()[oldname]
			delete(m.getData(), oldname)

			_, err := m.bucket().PutCopy(
				newname,
				s3.Private,
				s3.CopyOptions{},
				// PutCopy requires name in the format bucket/key...
				m.bucketName+"/"+oldname,
			)
			m.unlock()
			m.rlock()
			if err != nil {
				return err
			}
			err = m.Remove(oldname)
			if err != nil {
				return err
			}
		} else {
			return afero.ErrDestinationExists
		}
	} else {
		return afero.ErrFileNotFound
	}
	return nil
}

func (m *MemS3Fs) Stat(name string) (os.FileInfo, error) {
	f, err := m.Open(name)
	if err != nil {
		return nil, err
	}
	return &InMemoryFileInfo{file: f.(*InMemoryFile)}, nil
}

func (m *MemS3Fs) Chmod(name string, mode os.FileMode) error {
	f, ok := m.getData()[name]
	if !ok {
		return &os.PathError{"chmod", name, afero.ErrFileNotFound}
	}

	ff, ok := f.(*InMemoryFile)
	if ok {
		m.lock()
		ff.mode = mode
		m.unlock()
	} else {
		return errors.New("Unable to Chmod Memory File")
	}
	return nil
}

func (m *MemS3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	f, ok := m.getData()[name]
	if !ok {
		return &os.PathError{"chtimes", name, afero.ErrFileNotFound}
	}

	ff, ok := f.(*InMemoryFile)
	if ok {
		m.lock()
		ff.modtime = mtime
		m.unlock()
	} else {
		return errors.New("Unable to Chtime Memory File")
	}
	return nil
}

func (m *MemS3Fs) List() {
	for _, x := range m.data {
		y, _ := x.Stat()
		fmt.Println(x.Name(), y.Size())
	}
}
