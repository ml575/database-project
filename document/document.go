// This package defines documents and operations on documents
package document

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// This interface implements all the methods we need for the Collections we will store inside a Document.
type Collectioner interface {
}

// The indexer interface is the requirments for the dbindedex used to store a documents collections. An indexer must be able
// to find a collection based on a key (returning the collection and an ok bool). It must be able to upsert with a string key and
// check funchtion (returning a collection and err), and remove based on a key (returning a collection and an ok bool)
type Indexer[C Collectioner] interface {
	Find(key string) (C, bool)
	CallUpsert(key string, check func(string, C, bool) (C, error)) (C, error)
	Remove(key string) (C, bool)
}

// This is a struct representing a document. It contains a name string, a data slice of bytes, an dbindexer of collections, and a metadata struct
// A document should be created using the New function.
type Document[C Collectioner] struct {
	name     string
	data     []byte
	colSet   Indexer[C]
	metadata metadata
}

// This is a struct representing metadata. It contains a createdAt int of the time in miliseconds, a createdby string representing a username
// a lastModifiedAt time in miliseconds, and a lastModifiedBy username string. A zero value metadata struct is ready to use.
type metadata struct {
	CreatedAt      int64  `json:"createdAt"`
	CreatedBy      string `json:"createdBy"`
	LastModifiedAt int64  `json:"lastModifiedAt"`
	LastModifiedBy string `json:"lastModifiedBy"`
}

// just used to create the correct json response based on the document contents. It has a path string, a document byte array, and a metadata struct
// a zero value struct is ready to use.
type jsonDocumentFormat struct {
	Path string          `json:"path"`
	Doc  json.RawMessage `json:"doc"`
	Meta metadata        `json:"meta"`
}

// Creates a new document, with time as the current time in miliseconds, returns a pointer to the document.
func NewDocument[C Collectioner](name string, collectionIndex Indexer[C], data []byte, creator string) *Document[C] {
	time := time.Now().UnixMilli()

	d := Document[C]{
		name:     name,
		data:     data,
		metadata: metadata{CreatedAt: time, CreatedBy: creator, LastModifiedAt: time, LastModifiedBy: creator},
		colSet:   collectionIndex,
	}
	return &d
}

// This function modifies the metadata of a document. It sets the time field to the current time and puts the input
// name as the last modified by field.
func (d *Document[C]) ModifyMetadata(modifyer string) {
	time := time.Now().UnixMilli()
	slog.Debug(fmt.Sprintf("document edited. Modifyer: %s, Modified at: %d", modifyer, time))
	d.metadata.LastModifiedBy = modifyer
	d.metadata.LastModifiedAt = time

}

// a getter for the a document. Returns a byte array.
func (d *Document[C]) GetData() (data []byte) {
	return d.data
}

// This function creates a Json repsresentation of a document. It returns a slice of bytes and an error.
func (d *Document[C]) ReplaceData(data []byte) {
	slog.Info("document data overwritten")
	d.data = data
}

// This function creates a Json repsresentation of a document. It returns a slice of bytes and an error.
func (d *Document[C]) DocumentJsonMake(fullPath string) ([]byte, error) {
	returnStruct := jsonDocumentFormat{Path: fullPath, Doc: d.data, Meta: d.metadata}
	return json.Marshal(returnStruct)
}

// This function finds a collection based on its name. Calls dbIndex find
// returns a collection and an ok bool. Relies on dbIndex for concurrency saftey.
func (d *Document[C]) FindCollection(name string) (C, bool) {
	element, ok := d.colSet.Find(name)
	return element, ok
}

// This function updates or inserts a collection based on check function. It calls dbIndex uperst with the provided update check function
// returns a collection and an err. Relies on dbIndex for concurrency saftey.
func (d *Document[C]) PutCollection(name string, check func(string, C, bool) (C, error)) (C, error) {
	return d.colSet.CallUpsert(name, check)
}

// This function deletes a collection based on its name. Calls dbIndex find
// returns a collection and an ok bool. Relies on dbIndex for concurrency saftey.
func (d *Document[C]) DeleteCollection(name string) (C, bool) {
	return d.colSet.Remove(name)
}

// This function returns the name of a document as a string.
func (d *Document[C]) GetName() string {
	return d.name
}

// This function returns a copy of a document.
func (d *Document[C]) Copy() any {
	newDoc := Document[C]{
		name:     d.name,
		data:     d.data,
		colSet:   d.colSet,
		metadata: d.metadata,
	}
	return &newDoc
}
