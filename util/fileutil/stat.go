// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package fileutil

import (
	"io/fs"
	"os"
	"syscall"
)

const StatBlockSize = 512

func Stat(name string) (stat *syscall.Stat_t, err error) {
	info, err := os.Stat(name)
	if err != nil {
		return
	}
	stat = ConvertStat(info)
	return
}

func ConvertStat(info fs.FileInfo) (stat *syscall.Stat_t) {
	stat = info.Sys().(*syscall.Stat_t)
	return
}

func GetFilePhysicalSize(name string) (size int64, err error) {
	stat, err := Stat(name)
	if err != nil {
		return
	}
	size = stat.Blocks * StatBlockSize
	return
}
