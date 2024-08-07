// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"encoding/json"
	"fmt"
	"sort"
)

type StringSet map[string]struct{}

// returns StringSet as string slice.
func (set StringSet) ToSlice() []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// returns whether the set is empty or not.
func (set StringSet) IsEmpty() bool {
	return len(set) == 0
}

// adds string to the set.
func (set StringSet) Add(s string) {
	set[s] = struct{}{}
}

// removes string in the set.  It does nothing if string does not exist in the set.
func (set StringSet) Remove(s string) {
	delete(set, s)
}

// checks if string is in the set.
func (set StringSet) Contains(s string) bool {
	_, ok := set[s]
	return ok
}

// checks whether given set is equal to current set or not.
func (set StringSet) Equals(sset StringSet) bool {
	// If length of set is not equal to length of given set, the
	// set is not equal to given set.
	if len(set) != len(sset) {
		return false
	}

	// As both sets are equal in length, check each elements are equal.
	for k := range set {
		if _, ok := sset[k]; !ok {
			return false
		}
	}

	return true
}

// returns the intersection with given set as new set.
func (set StringSet) Intersection(sset StringSet) StringSet {
	nset := NewStringSet()
	for k := range set {
		if _, ok := sset[k]; ok {
			nset.Add(k)
		}
	}

	return nset
}

// returns the difference with given set as new set.
func (set StringSet) Difference(sset StringSet) StringSet {
	nset := NewStringSet()
	for k := range set {
		if _, ok := sset[k]; !ok {
			nset.Add(k)
		}
	}

	return nset
}

// returns the union with given set as new set.
func (set StringSet) Union(sset StringSet) StringSet {
	nset := NewStringSet()
	for k := range set {
		nset.Add(k)
	}

	for k := range sset {
		nset.Add(k)
	}

	return nset
}

// converts to JSON data.
func (set StringSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(set.ToSlice())
}

// parses JSON data and creates new set with it.
// If 'data' contains JSON string array, the set contains each string.
// If 'data' contains JSON string, the set contains the string as one element.
// If 'data' contains Other JSON types, JSON parse error is returned.
func (set *StringSet) UnmarshalJSON(data []byte) error {
	sl := []string{}
	var err error
	if err = json.Unmarshal(data, &sl); err == nil {
		*set = make(StringSet)
		for _, s := range sl {
			set.Add(s)
		}
	} else {
		var s string
		if err = json.Unmarshal(data, &s); err == nil {
			*set = make(StringSet)
			set.Add(s)
		}
	}

	return err
}

// returns printable string of the set.
func (set StringSet) String() string {
	return fmt.Sprintf("%s", set.ToSlice())
}

// creates new string set.
func NewStringSet() StringSet {
	return make(StringSet)
}

// creates new string set with given string values.
func CreateStringSet(sl ...string) StringSet {
	set := make(StringSet)
	for _, k := range sl {
		set.Add(k)
	}
	return set
}

// returns copy of given set.
func CopyStringSet(set StringSet) StringSet {
	nset := NewStringSet()
	for k, v := range set {
		nset[k] = v
	}
	return nset
}
