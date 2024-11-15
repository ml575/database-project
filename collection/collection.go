// This package defines collections and operations on databases/collections.
package collection

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// This interface implements the methods needed by the Documents that a Collection holds.
type Documenter interface {
	DocumentJsonMake(fullPath string) ([]byte, error)
	GetName() string
	Copy() any
}

// The indexer interface is the requirments for the dbindedex used to store a collection's documents. An indexer must be able
// to find a collection based on a key (returning the document and an ok bool). It must be able to upsert with a string key and
// a check function (returning a document and err), and remove based on a key (returning a document and an ok bool)
type Indexer[D Documenter] interface {
	Find(key string) (D, bool)
	Remove(key string) (D, bool)
	CallUpsert(key string, check func(string, D, bool) (D, error)) (D, error)
	Query(ctx context.Context, start string, end string, copier func(val D) any) (resultKeys []string, resultValues []D, err error)
}

// This is a struct representing a database/collection. It contains a name string, a map of document names to documenters, and a read write mutex.
// A collection should be created using the NewCollection funciton.
type Collection[D Documenter] struct {
	name        string
	docSet      Indexer[D]
	subscribers map[chan any](chan string)
	subMtx      sync.RWMutex
}

// This creates a new collection with the name provided by a string parameter
func NewCollection[D Documenter](name string, dbIndex Indexer[D]) *Collection[D] {
	return &Collection[D]{name: name, docSet: dbIndex, subscribers: make(map[chan any]chan string)}
}

// Creates a slice of bytes which is json representation of the collection created by iterating over all the documents in the collection.
// Relies on dbIndex Query method for concurrency saftey. Takes a context.Context to fail after the passing of deadline, a start string
// and a end string and will return based on documents with keys between these values (inclusive) also takes a string for the full path.
func (d *Collection[D]) CollectionJsonMake(ctx context.Context, start string, end string, fullPath string) ([]byte, error) {
	toReturn := make([]json.RawMessage, 0)
	docs := d.QueryDocuments(ctx, start, end)
	if docs == nil {
		return nil, errors.New(`"failed to query documents"`)
	}
	for _, document := range docs {
		jsonDoc, err := document.DocumentJsonMake(fullPath + document.GetName())
		if err != nil {
			return nil, err
		}
		toReturn = append(toReturn, jsonDoc)
	}
	return json.Marshal(toReturn)
}

// Searches for a document of the name provided by a string parameter. Returns the document and a boolean representing if the document was found.
// Relies on dbIndex find method for concurrency saftey
func (d *Collection[D]) FindDocument(name string) (D, bool) {
	return d.docSet.Find(name)
}

// This function returns all the documents between start and end in the collection as a list of documenters.
// Relies on dbIndex Query method for concurrency saftey. Takes a context.Context to fail after the passing of deadline
// or to let the query know when to close, a start string and a end string and will return based a list of documents with
// keys between these values (inclusive). It returns null if it could not query properly.
func (d *Collection[D]) QueryDocuments(ctx context.Context, start string, end string) []D {
	copyFunc := func(doc D) any {
		return doc.Copy()
	}

	_, docList, err := d.docSet.Query(ctx, start, end, copyFunc)
	if err != nil {
		return nil
	}

	return docList
}

// This function updates or inserts a document based on check function. It calls dbIndex uperst with the provided update check function
// returns a document and an err. Relies on dbIndex for concurrency saftey
func (d *Collection[D]) PutDocument(name string, check func(string, D, bool) (D, error)) (D, error) {
	return d.docSet.CallUpsert(name, check)
}

// This function deletes a document from the collection based on its name. Calls dbIndex find
// returns a document and an ok bool. Relies on dbIndex for concurrency saftey
func (d *Collection[D]) DeleteDocument(name string) (D, bool) {
	return d.docSet.Remove(name)
}

// Returns the name of a document as a string.
func (d *Collection[D]) GetName() string {
	return d.name
}

// This function adds a subscriber to our map of subscribers where it maps the actual channel that passes
// messages to the channel that indicated if a subscriber is closed/done.
func (d *Collection[D]) AddSubscriber(byteChannel chan any, doneChannel chan string) {
	d.subMtx.Lock()
	defer d.subMtx.Unlock()
	d.subscribers[byteChannel] = doneChannel
}

// This function removes subscribers based on the data channel passed in.
func (d *Collection[D]) DeleteSubscriber(channel chan any) {
	d.subMtx.Lock()
	defer d.subMtx.Unlock()
	delete(d.subscribers, channel)
}

// This function returns a copy of the map of all the subscribers in this collection.
func (d *Collection[D]) AllSubscribers() map[chan any](chan string) {
	d.subMtx.RLock()
	defer d.subMtx.RUnlock()
	copy := make(map[chan any]chan string)
	for ch1, ch2 := range d.subscribers {
		copy[ch1] = ch2
	}
	return copy
}
