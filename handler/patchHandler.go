// This package initializes the map of names to databases and implements the server's method handlers.
// The method handlers write responses back to clients, with responses differing based on the method
// being handled.
package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ml575/database-project/jsondata"
)

// A jsonPatchMessageFormat holds the components of the response body to be written.
// These components are the fields, which are: uri, a string representing the full path to the document
// that was patched; patchFailed, a boolean value that is true if the patch failed for any reason or
// false otherwise; and message, which is a string providing information about why the patch failed
// (if it failed), and the string "patch applied" otherwise.
type jsonPatchMessageFormat struct {
	Uri         string `json:"uri"`
	PatchFailed bool   `json:"patchFailed"`
	Message     string `json:"message"`
}

// patch is the method handler for patch requests of documents, collections, and databases.
// It takes in a ResponseWriter and a pointer to a Request. It assumes that the document and
// database put methods are concurrent safe. At the end of the method, given that no errors
// occurred, a response is written containing the full path to the document being patched,
// a boolean representing whether or not the patch failed, and a message elaborating on the
// patch failure or success. An error occurs if the method is unauthorized, if the request
// URL is not to an existing document, if there is an error reading in the request body,
// if there is an error marshaling the updated document data, or if there is an error
// formatting the updated data for subscriptions.
func (d *DatabaseIndex) patch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	username, validLogin := d.checkAuthorization(r.Header.Get("Authorization"))
	if !validLogin {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		return
	}

	// splitPaths is a slice of the path segments, (guaranteed to start w/ database name by parseUrl())
	splitPaths, err := parseUrl(r.URL.Path)
	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		return
	}

	// endsOnCol means that the last valid item is a collection
	endsOnCol, _, lastCol, lastGoodIndex, err := d.lastRealItem(splitPaths)
	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(splitPaths)%2 == 0 && splitPaths[len(splitPaths)-1] != "" {
		// checking content type for document only
		if r.Header.Get("Content-Type") != "application/json" {
			errorHelper(w, `"content type must be application/json"`, http.StatusBadRequest)
			slog.Error(`"content type must be application/json"`)
			return
		}
	} else {
		errorHelper(w, `"not document path"`, http.StatusBadRequest)
		slog.Error(`"not document path"`)
		return
	}

	retStatus := http.StatusCreated
	patchFailed := false
	message := ""

	// Verify that the URL path points to an existing document

	if endsOnCol {
		if lastGoodIndex == len(splitPaths)-1 {
			// If very last element is aready exisitng database/collection
			errorHelper(w, `"patch only supported on documents"`, http.StatusBadRequest)
			return
		} else if lastGoodIndex < len(splitPaths)-2 {
			// Fails to find a collection's child (a document) early on in path (not in last two elements)
			errorHelper(w, `"Containing document does not exist"`, http.StatusNotFound)
			return
		} else {
			// Else we know the second to last element is a collection, and the last element is a nonexistent document
			// In patch, this errors out b/c we can only edit, not create
			errorHelper(w, `"not found"`, http.StatusNotFound)
			return
		}
	} else {
		if lastGoodIndex == len(splitPaths)-1 {
			// last valid item is a document at the end of the path - we can patch this
			encoded, err := io.ReadAll(r.Body)
			if err != nil {
				errorHelper(w, `"unable to read request body"`, http.StatusBadRequest)
				return
			}

			docName := splitPaths[len(splitPaths)-1]
			if docName == "" {
				errorHelper(w, `"document name too short"`, http.StatusBadRequest)
				return
			}
			retStatus = http.StatusOK

			// Function to be passed into PutDocument as argument; carries out the patch operations, reading them in from the
			// patch body and modifying the specified document accordingly. Also ensures the atomicity of the patch method by
			// being passed in to PutDocument. Throws an error if the document doesn't exist or if there are issues unmarshaling
			// patch operation data or document data, or if new document data doesn't conform to our schema. Also returns the
			// document being modified in all cases.
			funcVar := func(key string, currValue Documenter, exists bool) (Documenter, error) {
				if exists {
					// Retrieve data of document
					docData := currValue.GetData()

					// Unmarshal data into JSONValue struct

					var docJson jsondata.JSONValue
					err = json.Unmarshal(docData, &docJson)
					if err != nil {
						// errorHelper(w, `"unable to unmarshal document data into JSONValue"`, http.StatusBadRequest)
						return currValue, errors.New(`"unable to unmarshal document data into JSONValue"`)
					}

					var jsonPatchOps jsondata.JSONValue
					unmarshal_err := json.Unmarshal(encoded, &jsonPatchOps)
					if unmarshal_err != nil {
						// errorHelper(w, `"unable to unmarshal encoded request body into JSONValue"`, http.StatusBadRequest)
						return currValue, errors.New(`"unable to unmarshal encoded request body into JSONValue"`)
					}

					//patchOperationsVisitor := patchvisitors.NewPatchOpListVisitor()
					patchOperationsVisitor := d.patchOpListFactory.NewPatchOpListVisitor()

					patchOperationsList, err := jsondata.Accept(jsonPatchOps, patchOperationsVisitor)
					slog.Debug("first visitor")
					if err != nil {
						retStatus = http.StatusBadRequest
						patchFailed = true
						message = err.Error()
					} else {

						patchVisitor := d.patchVisitorFactory.NewPatchVisitor(d.patchOpFactory) //patchvisitors.NewPatchVisitor()

						for _, operation := range patchOperationsList {

							patchOperation, err := jsondata.Accept(operation, patchVisitor)
							slog.Debug("second visitor")
							if err != nil {
								retStatus = http.StatusBadRequest
								patchFailed = true
								message = err.Error()
								break
							}

							// docVisitor := patchvisitors.NewDocVisitor(patchOperation.GetOp(),
							// 	patchOperation.GetPath(),
							// 	patchOperation.GetValue())

							docVisitor := d.docVisitorFactory.NewDocVisitor(patchOperation.GetOp(),
								patchOperation.GetPath(),
								patchOperation.GetValue())

							docJson, err = jsondata.Accept(docJson, docVisitor)
							slog.Debug("third visitor")
							if err != nil {
								patchFailed = true
								message = err.Error()
								break
							}
						}
					}

					if message == "" {
						message = "patch applied"
					}

					if !patchFailed {
						validateErr := docJson.Validate(d.schema)
						if validateErr != nil {
							// errorHelper(w, `"Request does not conform to database schema"`, http.StatusBadRequest)
							return currValue, errors.New(`"Request does not conform to database schema"`)
						}

						newDocData, err := json.Marshal(docJson)
						if err != nil {
							// errorHelper(w, `"error marshaling newDocData"`, http.StatusBadRequest)
							return currValue, errors.New(`"error marshaling newDocData"`)
						}

						currValue.ModifyMetadata(username)
						currValue.ReplaceData(newDocData)

						urlPath := r.URL.Path[4:]
						urlPath = urlPath[strings.Index(urlPath, "/"):]
						newDocJson, err := currValue.DocumentJsonMake(urlPath)
						if err != nil {
							return nil, errors.New(`"unable to format document for subscriptions"`)
						}

						notificationHelper(key, lastCol, newDocJson)
					}

					return currValue, nil

				} else {
					return currValue, errors.New(`"document does not exist"`)
				}
			}

			_, err = lastCol.PutDocument(docName, funcVar)
			if err != nil {
				if err.Error() == `"document does not exist"` {
					errorHelper(w, err.Error(), http.StatusNotFound)
					return
				}
				errorHelper(w, err.Error(), http.StatusBadRequest)
				return
			}

		} else if lastGoodIndex == -1 && len(splitPaths) == 1 {
			// patching a collection; this is not supported
			errorHelper(w, `"not found"`, http.StatusNotFound)
			return
		} else if lastGoodIndex == -1 {
			// can't find first database
			errorHelper(w, `"containing database does not exists"`, http.StatusNotFound)
			return
		} else if lastGoodIndex < len(splitPaths)-3 {
			// missing collection somewhere in the middle of the path (not in last three spots)
			errorHelper(w, `"containing collection does not exists"`, http.StatusNotFound)
			return
		} else if lastGoodIndex == len(splitPaths)-3 && splitPaths[len(splitPaths)-1] != "" {
			// document in third to last spot, and does not end with a trailing slash (missing the last collection)
			errorHelper(w, `"containing collection does not exists"`, http.StatusNotFound)
			return

		}
	}

	var jsonStr []byte
	patchMessage := jsonPatchMessageFormat{Uri: r.URL.Path, PatchFailed: patchFailed, Message: message}
	jsonStr, err = json.Marshal(patchMessage)
	if err != nil {
		errorHelper(w, `"unable to format uri"`, http.StatusBadRequest)
		return
	}

	w.Header().Set("Location", r.URL.Path)
	w.WriteHeader(retStatus)
	w.Write(jsonStr)
}
