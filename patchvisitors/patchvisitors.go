// This package handles the concrete Visitors responsible for reading in the list of patch
// operations from a PATCH method's request body and modifying the contents of a document
package patchvisitors

import (
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/ml575/database-project/jsondata"
)

// A PatchOpListVisitor extracts the list of patch operations to be performed on a document's contents
// from a JSONValue struct using the visitor pattern; if the JSONValue struct being visited
// is not at least an array on the outermost layer, an error is returned. It has a Map, Slice, Bool,
// Float64, String, and Null methods, so that it matches to the Visitor interface.
type PatchOpListVisitor struct {
}

// NewPatchOpListVisitor creates a new patchOpListVisitor for use in the visitor pattern.
func NewPatchOpListVisitor() *PatchOpListVisitor {
	return &PatchOpListVisitor{}
}

// Processes JSON Map; returns error, as list of patch operations should not come in the form of a map.
func (v PatchOpListVisitor) Map(m map[string]jsondata.JSONValue) ([]jsondata.JSONValue, error) {
	return nil, errors.New("patch operations should not come as map")
}

// Process JSON slice of patch operations; this is exactly what we are searching for, so return it.
func (v PatchOpListVisitor) Slice(s []jsondata.JSONValue) ([]jsondata.JSONValue, error) {
	return s, nil
}

// Processes JSON bool; returns error, as list of patch operations should not come in the form of a bool.
func (v PatchOpListVisitor) Bool(b bool) ([]jsondata.JSONValue, error) {
	return nil, errors.New("patch operations should not come as bool")
}

// Processes JSON float; returns error, as list of patch operations should not come in the form of a float.
func (v PatchOpListVisitor) Float64(f float64) ([]jsondata.JSONValue, error) {
	return nil, errors.New("patch operations should not come as float64")
}

// Processes JSON string; returns error, as list of patch operations should not come in the form of a string.
func (v PatchOpListVisitor) String(s string) ([]jsondata.JSONValue, error) {
	return nil, errors.New("patch operations should not come as string")
}

// Processes JSON null value; returns error, as list of patch operations should not come in the form of null.
func (v PatchOpListVisitor) Null() ([]jsondata.JSONValue, error) {
	return nil, errors.New("patch operations should not come as null")
}

// A PatchOp contains the details of a patch operation to be performed on the contents of a document. More
// specifically, it contains the name of the operation, the path of the operation, and the value of the
// operation. It has GetOp, GetPath, and GetValue methods, which are getter methods for each respective
// field.
type PatchOp struct {
	op    string             // The name of the operation to be performed
	path  string             // The path of the operation
	value jsondata.JSONValue // The value of the operation
}

// GetOp retrieves the op property of a given patchOp.
func (p PatchOp) GetOp() string {
	return p.op
}

// GetPath retrieves the path property of a given patchOp.
func (p PatchOp) GetPath() string {
	return p.path
}

// GetValue retrieves the value property of a given patchOp.
func (p PatchOp) GetValue() jsondata.JSONValue {
	return p.value
}

// Creates a new patchop, taking a string to be op, a string to be path, and a jsondata.JsonValue to be value
func NewPatchOp(op string, path string, value jsondata.JSONValue) *PatchOp {
	return &PatchOp{op: op, path: path, value: value}
}

// An interface for patchop methods, allowing main to pass in a handler.patchop as a generic
type PatchOper interface {
	GetOp() string
	GetPath() string
	GetValue() jsondata.JSONValue
}

// An interface for patchop creation, allowing main to set the factory to return a handler.patchop
type PatchOpFactory[p PatchOper] interface {
	NewPatchOp(op string, path string, value jsondata.JSONValue) p
}

// A PatchVisitor extracts the name, path, and value of a patch operation from a JSONValue struct using the visitor pattern;
// if the JSONValue struct is not a map on the outermost layer, an error is returned. If the map is lacking the "op", "path",
// or "value" properties, an error is returned. If either of the values of the "op" or "path" properties are not strings, an
// error is returned. If no error is returned, then a patchOp struct containing the aforementioned values is returned. Contains
// the opFind and pathFind flags to be used in the visitor pattern to indicate if we are currently searching for
// the value for the "op" or "path" fields. It has a Map, Slice, Bool, Float64, String, and Null methods, so that it matches
// to the Visitor interface.
type PatchVisitor[p PatchOper, pf PatchOpFactory[p]] struct {
	opFind       bool // The flag used to indicate if we're looking for the value of the "op" property
	pathFind     bool // The flag used to indicate if we're looking for the value of the "path" property
	patchFactory pf
}

// NewPatchVisitor creates a new patchVisitor for use in the visitor pattern.
func NewPatchVisitor[p PatchOper, pf PatchOpFactory[p]](patchFactory pf) PatchVisitor[p, pf] {
	return PatchVisitor[p, pf]{opFind: false, pathFind: false, patchFactory: patchFactory}
}

// Process JSON Map by iterating through map and calling Accept on the values whose keys
// are "op" or "path"; stores the values whose keys are "op", "path", and "value" inside
// a patchOp struct and returns it. If the map is missing any of those 3 keys, an
// error is returned. If there is an error retrieving the value mapped to one of those 3
// keys, an error is returned.
func (v PatchVisitor[p, pf]) Map(m map[string]jsondata.JSONValue) (p, error) {

	// Below covers cases where op, path, or value aren't specified in patch operation
	_, ok := m["op"]
	if !ok {
		var j jsondata.JSONValue
		return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation missing \"op\" property")
	}
	_, ok = m["path"]
	if !ok {
		var j jsondata.JSONValue
		return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation missing \"path\" property")
	}
	_, ok = m["value"]
	if !ok {
		var j jsondata.JSONValue
		return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation missing \"value\" property")
	}

	var op string
	var path string
	var value jsondata.JSONValue

	for key, val := range m {
		if key == "op" {
			v.opFind = true
			opHolder, err := jsondata.Accept(val, v)
			if err != nil {
				var j jsondata.JSONValue
				return v.patchFactory.NewPatchOp("", "", j), errors.New("value of \"op\" property not string")
			}
			v.opFind = false
			op = opHolder.GetOp()
		} else if key == "path" {
			v.pathFind = true
			pathHolder, err := jsondata.Accept(val, v)
			if err != nil {
				var j jsondata.JSONValue
				return v.patchFactory.NewPatchOp("", "", j), errors.New("value of \"path\" property not string")
			}
			v.pathFind = false
			path = pathHolder.GetPath()
		} else if key == "value" {
			value = val
		}
	}

	return v.patchFactory.NewPatchOp(op, path, value), nil
}

// Processes JSON slice; returns error, as a patch operation should not come in the form of a slice.
func (v PatchVisitor[p, pf]) Slice(s []jsondata.JSONValue) (p, error) {
	// Patch operation shouldn't come as slice

	var j jsondata.JSONValue
	return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation should not come as slice")
}

// Processes JSON bool; returns error, as a patch operation should not come in the form of a bool.
func (v PatchVisitor[p, pf]) Bool(b bool) (p, error) {
	// Patch operations shouldn't come as bool

	var j jsondata.JSONValue
	return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation should not come as bool")
}

// Processes JSON float; returns error, as a patch operation should not come in the form of a float.
func (v PatchVisitor[p, pf]) Float64(f float64) (p, error) {
	// Patch operations shouldn't come as float

	var j jsondata.JSONValue
	return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation should not come as float64")
}

// Process JSON string; if opFind and pathFind are both false, it means that the patch
// operation came as just a string, which is incorrect and thus returns an error.
// If the opFind flag is set to True, we return a patchOp with its "op" field set
// to s. // If the pathFind flag is set to True, we return a patchOp with its "path"
// field set to s.
func (v PatchVisitor[p, pf]) String(s string) (p, error) {
	// Covers case where patch operation is a just a string; this is invalid
	var err error = nil
	if !v.opFind && !v.pathFind {
		err = errors.New("patch operation should not come as string")
	} else if v.opFind {
		var j jsondata.JSONValue
		return v.patchFactory.NewPatchOp(s, "", j), err

	} else {
		var j jsondata.JSONValue
		return v.patchFactory.NewPatchOp("", s, j), err
	}

	var j jsondata.JSONValue
	return v.patchFactory.NewPatchOp("", "", j), err
}

// Processes JSON null; returns error, as a patch operation should not come in the form of a null.
func (v PatchVisitor[p, pf]) Null() (p, error) {
	// Patch operations shouldn't come as null

	var j jsondata.JSONValue
	return v.patchFactory.NewPatchOp("", "", j), errors.New("patch operation should not come as null")
}

// A DocVisitor modifies a JSONValue by applying a patch operation through the visitor pattern. The
// details of the patch operation are stored in the "op", "path", and "value" fields of the struct,
// which tell the type of operation, where in the JSONValue that operation should be executed, and the
// value associated with the operation. If the patch operation is not applied at the "current" path,
// the "path" field will be modified to go "down" one path element, at which it will be passed in to
// Accept to continue the visitor pattern. The "first" field denotes whether or not the DocVisitor is
// currently at the "start" of the original "path" used at the beginning of the visitor pattern. It has
// a Map, Slice, Bool, Float64, String, and Null methods, so that it matches to the Visitor interface.
type DocVisitor struct {
	op    string             // The name of the operation being patched in by the visitor pattern.
	path  string             // The jsonpointer path specifying the element of the JSON value to be modified.
	value jsondata.JSONValue // The value associated with the current operation being patched.
	first bool               // A flag denoting whether or not the docVisitor is currently at the "start" of the original "path".
}

// NewDocVisitor creates a new docVisitor for use in the visitor pattern.
func NewDocVisitor(op string, path string, value jsondata.JSONValue) *DocVisitor {
	return &DocVisitor{op: op, path: path, value: value, first: true}
}

// Process JSON Map in the docVisitor visitor pattern. If the current "path" field in the docVisitor is the
// last path segment and the "op" field in the docVisitor is ObjectAdd, adds a new key-value pair to m, where
// the key is the last path segment in the "path" field and the value is the "value" field in the docVisitor.
// If there are no more path segments left in the docVisitor's "path" field, return an error. Otherwise, modify
// the "path" to omit the current "top-most" path segment and call Accept using this new value for the docVisitor's
// "path" field. If there are errors in these nested Accept calls, return an error. Also return an error if the
// docVisitor's "op" field is none of "ArrayAdd", "ArrayRemove", or "ObjectAdd". If element of JSONValue to be
// modified is successfully found and patch is carried out, return NewJSONValue of m, which reflects the updates
// made to m.
func (v DocVisitor) Map(m map[string]jsondata.JSONValue) (jsondata.JSONValue, error) {
	slog.Debug("It's a map")

	v, splitPaths, err := handlePathStart(v)
	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())
	}

	if len(splitPaths) == 0 {
		// Error out; path ending in object is failure for all ops
		slog.Debug("Error: path ends in map")
		return jsondata.JSONValue{}, errors.New("error applying patches: path ends in map")

	}

	if v.op == "ArrayAdd" || v.op == "ArrayRemove" {

		// at least one more path left, search for next path as key
		res, err := mapAcceptNextPath(v, m, splitPaths)
		if err != nil {
			return jsondata.JSONValue{}, errors.New(err.Error())
		}

		return res, nil

	} else if v.op == "ObjectAdd" {
		// If one path left, we add in this map
		if len(splitPaths) == 1 {

			res, err := doObjectAdd(v, m, splitPaths)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, err

		} else {

			// More than one path left, we search for next path
			res, err := mapAcceptNextPath(v, m, splitPaths)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}
	} else {
		slog.Debug("Error: invalid patch operation")
		return jsondata.JSONValue{}, errors.New("error applying patches: invalid patch operation")

	}
}

// Process JSON slice in the docVisitor visitor pattern. If the current "path" field in the docVisitor is the empty
// string and the "op" field in the docVisitor is ArrayAdd or ArrayRemove, carries out the corresponding
// operation (adding/removing the value in the docVisitor's "value" field to s); if the "op" field is ObjectAdd, raise
// an error. Otherwise, if there are still more path segments to traverse in the docVisitor's "path" field, modify
// the "path" to omit the current "top-most" path segment and call Accept using this new value for the docVisitor's
// "path" field. If there are errors in these nested Accept calls, return an error. Also return an error if the
// docVisitor's "op" field is none of "ArrayAdd", "ArrayRemove", or "ObjectAdd". If element of JSONValue to be
// modified is successfully found and patch is carried out, return NewJSONValue of s, which reflects the updates
// made to s.
func (v DocVisitor) Slice(s []jsondata.JSONValue) (jsondata.JSONValue, error) {
	slog.Debug("It's a slice")
	var splitPaths []string

	v, splitPaths, err := handlePathStart(v)
	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())
	}

	if v.op == "ArrayAdd" {
		if len(splitPaths) == 0 {

			res, err := doArrayAdd(v, s)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		} else {

			res, err := sliceAcceptNextPath(v, s, splitPaths)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}

	} else if v.op == "ArrayRemove" {
		if len(splitPaths) == 0 {

			res, err := doArrayRemove(v, s)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		} else {

			res, err := sliceAcceptNextPath(v, s, splitPaths)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}

	} else if v.op == "ObjectAdd" {
		if len(splitPaths) == 0 {
			// Error out; ObjectAdd path ends in slice
			slog.Debug("Error: ObjectAdd path ends in slice")
			return jsondata.JSONValue{}, errors.New("error applying patches: ObjectAdd path ends in slice")

		} else {

			res, err := sliceAcceptNextPath(v, s, splitPaths)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}

	} else {
		slog.Debug("Error: invalid patch operation")
		return jsondata.JSONValue{}, errors.New("error applying patches: invalid patch operation")

	}

}

// Processes JSON bool; returns error, as this suggests the current element in the JSONValue traversal
// is a bool and we can neither apply a patch operation to a bool nor can we traverse further down the path
// from here (since a bool is a single value)
func (v DocVisitor) Bool(b bool) (jsondata.JSONValue, error) {
	// Patch operations shouldn't come as bool
	slog.Debug("Error: found bool along path")
	return jsondata.JSONValue{}, errors.New("error applying patches: found bool along path")
}

// Processes JSON float64; returns error, as this suggests the current element in the JSONValue traversal is
// a float64 and we can neither apply a patch operation to a float64 nor can we traverse further down the path
// from here (since a float64 is a single value)
func (v DocVisitor) Float64(f float64) (jsondata.JSONValue, error) {
	// Patch operations shouldn't come as float
	slog.Debug("Error: found float64 along path")
	return jsondata.JSONValue{}, errors.New("error applying patches: found float64 along path")
}

// Processes JSON string; returns error, as this suggests the current element in the JSONValue traversal
// is a string and we can neither apply a patch operation to a string nor can we traverse further down the path
// from here (since a string is a single value)
func (v DocVisitor) String(s string) (jsondata.JSONValue, error) {
	// Covers case where patch operation is a just a string; this is invalid
	slog.Debug("Error: found string along path")
	return jsondata.JSONValue{}, errors.New("error applying patches: found string along path")
}

// Processes JSON null; returns error, as this suggests the current element in the JSONValue traversal
// is a null and we can neither apply a patch operation to a null nor can we traverse further down the path
// from here (since a null is a single value)
func (v DocVisitor) Null() (jsondata.JSONValue, error) {
	// Patch operations shouldn't come as null
	slog.Debug("Error: found null along path")
	return jsondata.JSONValue{}, errors.New("error applying patches: found null along path")
}

// handlePathStart is a helper function to handle different edge cases regarding the start of the
// docVisitor's "path" field. If the docVisitor is at the start of its original path, it checks that
// the path starts with a "/" and throws an error if it doesn't; if the docVisitor isn't at the start
// of its original path but the current path being traversed is the empty string, return an empty slice
// of strings; otherwise, returns a slice of strings representing the docVisitor's "path" field being
// split by the "/" character.
func handlePathStart(v DocVisitor) (DocVisitor, []string, error) {
	var splitPaths []string

	if v.first || v.path != "" {
		splitPaths = strings.Split(v.path, "/")
	} else {
		splitPaths = make([]string, 0)
	}

	if v.first && splitPaths[0] != "" {
		// Error out; path should always start with /
		slog.Debug("Error: Path should start with /")
		return v, splitPaths, errors.New("error applying patches: path should always start with /")

	} else if v.first {
		v.first = false
		splitPaths = splitPaths[1:]
	}

	return v, splitPaths, nil
}

// mapAcceptNextPath is a helper function that calls the Accept method on the next path segment in the
// patch operation's path for maps. If the next path segment is missing as a key in the map,
// return an error. If the Accept call or any of its nested Accept calls return an error, return that
// error. If the patch operation goes through and a JSONValue in m is modified, return m wrapped in
// a NewJSONValue, which will reflect the changes made to m.
func mapAcceptNextPath(v DocVisitor, m map[string]jsondata.JSONValue, splitPaths []string) (jsondata.JSONValue, error) {
	next_path := splitPaths[0]
	next_path = strings.ReplaceAll(next_path, "~1", "/")
	next_path = strings.ReplaceAll(next_path, "~0", "~")
	slog.Debug(next_path)

	for key, val := range m {
		if key == next_path {
			splitPaths = splitPaths[1:]
			v.path = strings.Join(splitPaths, "/")

			slog.Debug("Delegate further down")
			acceptRes, err := jsondata.Accept(val, v)
			if err != nil {
				slog.Debug("Object Error: from further down given path (currently in a map)")
				return jsondata.JSONValue{}, errors.New(err.Error())
			}
			m[key] = acceptRes

			res, err := jsondata.NewJSONValue(m)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}
	}

	// if succesfully exits for loop, means path was NOT find as a key => error out
	slog.Debug("Error: key not found in map")
	return jsondata.JSONValue{}, errors.New("key not found in map")
}

// mapAcceptNextPath is a helper function that calls the Accept method on the next path segment in the
// patch operation's path for slices. If the next path segment cannot be converted to an index for the slice,
// return an error. If the converted index is out of bounds for s, return an error. If the Accept call or any
// of its nested Accept calls return an error, return that error. If the patch operation goes through and a
// JSONValue in s is modified, return s wrapped in a NewJSONValue, which will reflect the changes made to s.
func sliceAcceptNextPath(v DocVisitor, s []jsondata.JSONValue, splitPaths []string) (jsondata.JSONValue, error) {
	idx, err := strconv.Atoi(splitPaths[0])
	if err != nil {
		// error out, non-convertible string is not valid array index
		slog.Debug("invalid index")
		return jsondata.JSONValue{}, errors.New("error applying patches: invalid index")
	}

	if idx >= len(s) {
		// error out, index out of bounds
		slog.Debug("indexOutOfBounds")
		return jsondata.JSONValue{}, errors.New("error applying patches: index exceeds array length")
	}

	// Next path is valid index; remove next path from splitPaths and pass to correct index
	splitPaths = splitPaths[1:]
	v.path = strings.Join(splitPaths, "/")

	slog.Debug("Delegate further down")
	acceptRes, err := jsondata.Accept(s[idx], v)
	if err != nil {
		slog.Debug("ArrayAdd Error: from further down given path (currently in slice)")
		return jsondata.JSONValue{}, errors.New(err.Error())
	}
	s[idx] = acceptRes

	res, err := jsondata.NewJSONValue(s)
	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())
	}

	return res, nil
}

// Adds a new value to s, where the value is the "value" field in v. Does nothing if the value already exists in
// s. After adding (or not adding) s, re-wraps s in a JSONValue struct using NewJSONValue and returns it. Throws
// an error if there are any issues re-wrapping s.
func doArrayAdd(v DocVisitor, s []jsondata.JSONValue) (jsondata.JSONValue, error) {
	// Add to current slice if not already there
	for idx := 0; idx < len(s); idx++ {
		if s[idx].Equal(v.value) {
			slog.Debug("value already exists in array; this is ok")
			res, err := jsondata.NewJSONValue(s)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil // value is already in array
		}
	}

	// value is not already in array; add it
	newArr := make([]jsondata.JSONValue, 0)
	newArr = append(newArr, s...)
	newArr = append(newArr, v.value)
	slog.Debug("value does not exist in array; this is what we want")
	res, err := jsondata.NewJSONValue(newArr)
	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())
	}

	return res, nil
}

// Removes a value from s, where the value is the "value" field in v. Does nothing if the value doesn't exist in
// s. After removing (or not removing) s, re-wraps s in a JSONValue struct using NewJSONValue and returns it. Throws
// an error if there are any issues re-wrapping s.
func doArrayRemove(v DocVisitor, s []jsondata.JSONValue) (jsondata.JSONValue, error) {
	// Remove from current slice
	for removeIdx := 0; removeIdx < len(s); removeIdx++ {
		if s[removeIdx].Equal(v.value) {
			newArr := make([]jsondata.JSONValue, 0)
			newArr = append(newArr, s[:removeIdx]...)
			newArr = append(newArr, s[removeIdx+1:]...)
			slog.Debug("value exists in array; this is what we want")
			res, err := jsondata.NewJSONValue(newArr)
			if err != nil {
				return jsondata.JSONValue{}, errors.New(err.Error())
			}

			return res, nil

		}
	}
	slog.Debug("value does not exist in array; this is ok")
	res, err := jsondata.NewJSONValue(s)
	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())

	}

	return res, nil // value is not in array
}

// Adds a new key-value pair to m, where the key is the first (and only) element of splitPaths, and the value
// is the "value" field in v. Does nothing if the key already exists in m. After adding (or not adding) m,
// re-wraps m in a JSONValue struct using NewJSONValue and returns it. Throws an error if there are any issues
// re-wrapping m.
func doObjectAdd(v DocVisitor, m map[string]jsondata.JSONValue, splitPaths []string) (jsondata.JSONValue, error) {
	splitPaths[0] = strings.ReplaceAll(splitPaths[0], "~1", "/")
	splitPaths[0] = strings.ReplaceAll(splitPaths[0], "~0", "~")
	_, ok := m[splitPaths[0]] // Check if key exists before adding key-value pair
	if ok {
		slog.Debug("Key already exists in object; this is ok")
	}
	if !ok {
		slog.Debug("Key does not exist in object; this is ok")
		m[splitPaths[0]] = v.value // If key doesn't exist, add key-value pair
	}
	res, err := jsondata.NewJSONValue(m) // Create new JSONValue of edited m and return it

	if err != nil {
		return jsondata.JSONValue{}, errors.New(err.Error())
	}
	return res, nil
}
