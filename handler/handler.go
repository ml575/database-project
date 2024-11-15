// This package initializes the map of names to databases and implements the server's method handlers.
// The method handlers write responses back to clients.
package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ml575/database-project/jsondata"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// This is an interface for a NewCollection method that returns a Collectioner.
type CollectionFactory interface {
	NewCollection(name string) Collectioner
}

// This is an interface for a NewDocument method that returns a Documenter.
type DocumentFactory interface {
	NewDocument(name string, data []byte, creator string) Documenter
}

// This is an interface for a NewPatchOpListVisitor method that returns a PatchOpListVisitor.
type PatchOpListVisitorFactory interface {
	NewPatchOpListVisitor() PatchOpListVisitor
}

// This is an an interface defining all the methods a Path Op Visitor can do.
type PatchOpListVisitor interface {
	Bool(b bool) ([]jsondata.JSONValue, error)
	Float64(f float64) ([]jsondata.JSONValue, error)
	Map(m map[string]jsondata.JSONValue) ([]jsondata.JSONValue, error)
	Null() ([]jsondata.JSONValue, error)
	Slice(s []jsondata.JSONValue) ([]jsondata.JSONValue, error)
	String(s string) ([]jsondata.JSONValue, error)
}

// This is an interface for a NewPatchVisitor method that returns a PatchVisitor.
type PatchVisitorFactory interface {
	NewPatchVisitor(PatchOpFactory) PatchVisitor
}

// This is an interface that defines all the methods that a PathVisitor can do.
type PatchVisitor interface {
	Bool(b bool) (PatchOper, error)
	Float64(f float64) (PatchOper, error)
	Map(m map[string]jsondata.JSONValue) (PatchOper, error)
	Null() (PatchOper, error)
	Slice(s []jsondata.JSONValue) (PatchOper, error)
	String(s string) (PatchOper, error)
}

// This is an interfave that defines all the methods that a PathOper can do.
type PatchOper interface {
	GetOp() string
	GetPath() string
	GetValue() jsondata.JSONValue
}

// This is an interface for a NewPatchOp method that returns a PatchOper.
type PatchOpFactory interface {
	NewPatchOp(op string, path string, value jsondata.JSONValue) PatchOper
}

// This is an interface for a NewDocVisitor method that returns a DocVisitor.
type DocVisitorFactory interface {
	NewDocVisitor(string, string, jsondata.JSONValue) DocVisitor
}

// This is an interface for all the operations that a DocVisitor can do.
type DocVisitor interface {
	Bool(b bool) (jsondata.JSONValue, error)
	Float64(f float64) (jsondata.JSONValue, error)
	Map(m map[string]jsondata.JSONValue) (jsondata.JSONValue, error)
	Null() (jsondata.JSONValue, error)
	Slice(s []jsondata.JSONValue) (jsondata.JSONValue, error)
	String(s string) (jsondata.JSONValue, error)
}

// This interface defines the functionality of a document.
type Documenter interface {
	DocumentJsonMake(fullPath string) ([]byte, error)
	FindCollection(name string) (Collectioner, bool)
	PutCollection(name string, check func(key string, currValue Collectioner, exists bool) (Collectioner, error)) (Collectioner, error)
	DeleteCollection(name string) (Collectioner, bool)
	GetName() string
	ModifyMetadata(modifyer string)
	ReplaceData(data []byte)
	GetData() []byte
	Copy() any
}

// This is an interface that matches to collections.
type Collectioner interface {
	CollectionJsonMake(ctx context.Context, start string, end string, fullPath string) ([]byte, error)
	FindDocument(name string) (Documenter, bool)
	PutDocument(name string, check func(key string, currValue Documenter, exists bool) (Documenter, error)) (Documenter, error)
	DeleteDocument(name string) (Documenter, bool)
	GetName() string
	QueryDocuments(ctx context.Context, start string, end string) []Documenter
	AddSubscriber(byteChannel chan any, doneChannel chan string)
	DeleteSubscriber(channel chan any)
	AllSubscribers() map[chan any](chan string)
}

// This is an interface with methods pertaining to authorization.
type Auther interface {
	AddToken(username string) string
	IsTokenValid(token string) (string, bool)
	DeleteToken(token string) bool
}

// Requirments for a dbindex unsed to store top level databases. Dependency injected. Must be able to find and remove
// based on the name of the database. Must be able to upsert with a check function
type DbIndexer interface {
	Find(key string) (Collectioner, bool)
	Remove(key string) (Collectioner, bool)
	CallUpsert(key string, check func(key string, currValue Collectioner, exists bool) (Collectioner, error)) (Collectioner, error)
}

type DocIndexer interface {
	Find(key string) (Documenter, bool)
	Remove(key string) (Documenter, bool)
	CallUpsert(key string, check func(key string, currValue Documenter, exists bool) (Documenter, error)) (Documenter, error)
}

// This is a helper function for throwing http errors. It takes a response writer, error message, and string.
// It sets the response writer header and writes the proper error to the error.
func errorHelper(w http.ResponseWriter, err string, code int) {
	h := w.Header()
	h.Del("Content-Length")
	h.Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintln(w, err)
	slog.Error(fmt.Sprintf("error %s code %d", err, code))
}

// Wraps dbIndex, also holding a document factory, a database factory, an authery and a pointer to a json schema.
// Should be created using the New function.
type DatabaseIndex struct {
	dbIndex             DbIndexer // TODO maybe just change to list and match up with db struct in db package
	colFactory          CollectionFactory
	docFactory          DocumentFactory
	docVisitorFactory   DocVisitorFactory
	patchVisitorFactory PatchVisitorFactory
	patchOpListFactory  PatchOpListVisitorFactory
	patchOpFactory      PatchOpFactory
	auth                Auther
	schema              *jsonschema.Schema
}

// This is just used so we can turn a path into a correctly formatted json object for put to return
type jsonPutMessageFormat struct {
	Uri string `json:"uri"`
}

// This is just used so we can turn a username into a correctly formatted json object
type jsonAuthInputFormat struct {
	Username string `json:"username"`
}

// This is just used so we can turn a token into a correctly formatted json object
type jsonAuthOutputFormat struct {
	Token string `json:"token"`
}

// Creates a handler to handle requests made to the server,
// takes a collection factory, a document factory, an auther, and a pointer to a schema and creates a databseIndex with these values.
// creates a http.ServeMux and sets requests to pass to proper handler methods. Returns this mux as a httpHandler
func New(inColFactory CollectionFactory, docFactory DocumentFactory, auth Auther,
	schema *jsonschema.Schema, dbindexer DbIndexer,
	patchOpListFactory PatchOpListVisitorFactory,
	patchVisitorFactory PatchVisitorFactory,
	docVisitorFactory DocVisitorFactory,
	patchOpFactory PatchOpFactory) http.Handler {

	var dbMap DatabaseIndex = DatabaseIndex{dbIndex: dbindexer,
		colFactory: inColFactory, docFactory: docFactory, auth: auth, schema: schema,
		patchOpListFactory: patchOpListFactory, patchVisitorFactory: patchVisitorFactory,
		docVisitorFactory: docVisitorFactory, patchOpFactory: patchOpFactory}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/", dbMap.get)
	mux.HandleFunc("PUT /v1/", dbMap.put)
	mux.HandleFunc("OPTIONS /v1/", dbMap.options)
	mux.HandleFunc("DELETE /v1/", dbMap.delete)
	mux.HandleFunc("POST /v1/", dbMap.post)
	mux.HandleFunc("POST /auth", dbMap.authorization)
	mux.HandleFunc("DELETE /auth", dbMap.logout)
	mux.HandleFunc("OPTIONS /auth", dbMap.authOptions)
	mux.HandleFunc("PATCH /v1/", dbMap.patch)
	slog.Info("new handler created")

	return mux
}

// This function handles new auth requests and writes back the user's token.
func (d *DatabaseIndex) authorization(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if r.Header.Get("Content-Type") != "application/json" {
		errorHelper(w, `"content type must be application/json"`, http.StatusBadRequest)
		slog.Error(`"content type must be application/json"`)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		errorHelper(w, `"can't read body"`, http.StatusBadRequest)
		slog.Error("can't read body")
		return
	}

	var authJson jsonAuthInputFormat
	err = json.Unmarshal(body, &authJson)
	if err != nil {
		errorHelper(w, `"error unmarshaling username"`, http.StatusBadRequest)
		slog.Error("error unmarshalling username")
		return
	}

	token := d.auth.AddToken(authJson.Username)
	output := jsonAuthOutputFormat{token}
	encoded, err := json.Marshal(output)
	if err != nil {
		errorHelper(w, `"error marshaling token"`, http.StatusBadRequest)
		slog.Error("error marshalling token")
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(encoded)
}

// This helper function can be used to determing the username associated with a token
// and whether it is valid or not.
func (d *DatabaseIndex) checkAuthorization(token string) (string, bool) {
	if token == "" {
		return "", false
	}

	if len(token) < len("Bearer ") || token[:len("Bearer ")] != "Bearer " {
		return "", false
	}

	name, ok := d.auth.IsTokenValid(token[len("Bearer "):])
	if !ok {
		return "", false
	}

	return name, true
}

// This function handles logging out if the user has a valid token.
func (d *DatabaseIndex) logout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	authToken := r.Header.Get("Authorization")
	if authToken == "" {
		errorHelper(w, `"no authorization token"`, http.StatusUnauthorized)
		return
	}

	if len(authToken) < len("Bearer ") || authToken[:len("Bearer ")] != "Bearer " {
		errorHelper(w, `"invalid authorization header format"`, http.StatusUnauthorized)
		return
	}

	if !d.auth.DeleteToken(authToken[len("Bearer "):]) {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Helper method to take a list of document/database/collection names, and traverses it finding the listed items
// stops once the end of the list is reached or it fails to find one of the listed items. Returns the a boolean that
// is true if the last found item is a collection, the last found document, the last found collection, the index of the last found item, and an error
func (d *DatabaseIndex) lastRealItem(splitpaths []string) (bool, Documenter, Collectioner, int, error) {

	colLastFound := false
	var curCol Collectioner
	var curDoc Documenter

	if len(splitpaths) > 1 && len(splitpaths)%2 == 1 {
		return false, nil, nil, -1, errors.New(`"bad path"`)
	}

	for i, elementName := range splitpaths {

		if (i != len(splitpaths)-1 || i == 0) && elementName == "" {
			return false, nil, nil, -1, errors.New(`" // not allowed"`)
		}

		if !colLastFound {

			if i == 0 {
				db, ok := d.dbIndex.Find(elementName)

				if !ok {
					// the first database in the path doesnt exist
					slog.Debug(fmt.Sprintf("traversing across path %v, last found item at index %d", splitpaths, i-1))
					return colLastFound, nil, nil, i - 1, nil
				}
				curCol = db

			} else {
				col, ok := curDoc.FindCollection(elementName)

				if !ok {
					// ran into a non-existent collection
					slog.Debug(fmt.Sprintf("traversing across path %v, last found item at index %d", splitpaths, i-1))
					return colLastFound, curDoc, nil, i - 1, nil
				}
				curCol = col

			}
			colLastFound = true

		} else {
			doc, ok := curCol.FindDocument(elementName)

			if !ok {
				//ran into a non existent document
				return true, curDoc, curCol, i - 1, nil
			}
			curDoc = doc
			colLastFound = false
		}
	}
	slog.Debug(fmt.Sprintf("traversing across path %v, last found item at index %d", splitpaths, len(splitpaths)-1))
	return colLastFound, curDoc, curCol, len(splitpaths) - 1, nil
}

// Method handler for options requests, takes a ResponseWriter and a Request
func (t *DatabaseIndex) options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Allow", "GET,PUT,POST,DELETE,PATCH")
	w.Header().Set("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,PATCH")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Last-Event-ID")
	w.WriteHeader(http.StatusOK)
}

// Method handler for authOptions requests, takes a ResponseWriter and a Request
func (t *DatabaseIndex) authOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Allow", "POST,DELETE")
	w.Header().Set("Access-Control-Allow-Methods", "POST,DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.WriteHeader(http.StatusOK)
}

// Helper function to convert of url into a slice of the collection/document names in the path.
// Takes the pathURL and returns a slice of strings. Makes sure the path starts with /v1/ and something else following
func parseUrl(path string) ([]string, error) {

	splitPaths := strings.Split(path, "/")

	// Check for starting slash
	nextPathElement, splitPaths, ok := frontPop(splitPaths)
	if nextPathElement != "" || !ok {
		return nil, errors.New(`"invalid path, path should start with the form '/v1/...'"`)
	}

	// Check for v1/
	nextPathElement, splitPaths, ok = frontPop(splitPaths)
	if nextPathElement != "v1" || !ok {
		return nil, errors.New(`"invalid path, path should start with the form '/v1/...'"`)
	}

	// splitPaths should now be a slice of the remaining path segments, starting with the database
	if len(splitPaths) == 0 {
		return nil, errors.New(`"invalid path, no database specified"`)
	} else {
		return splitPaths, nil
	}

}

// helper function to perform pop operation on the first element in a slice; returns the first element (if any),
// a slice containing the rest of the elements, and a boolean indicating whether or not the first element exists
func frontPop(pathElements []string) (item1 string, remaining []string, ok bool) {
	if len(pathElements) == 0 {
		return "", pathElements, false
	} else {
		return pathElements[0], pathElements[1:], true
	}
}

// Helper function to send notifications for subscriptions. Handles formatting the message and
// sending it to subscribers.
func notificationHelper(newDocName string, lastCol Collectioner, jsonDoc json.RawMessage) {
	var eventAndData bytes.Buffer
	eventAndData.WriteString("event: update\ndata: ")
	var id bytes.Buffer
	id.WriteString(fmt.Sprintf("\nid: %d\n\n", time.Now().UnixMilli()))
	message := make([]byte, 0)
	message = append(message, eventAndData.Bytes()...)
	message = append(message, jsonDoc...)
	message = append(message, id.Bytes()...)
	slog.Info(fmt.Sprintf("attempting to notify subscribers in collection %s", lastCol.GetName()))
	notifySubscriptions(newDocName, lastCol, message)
}
