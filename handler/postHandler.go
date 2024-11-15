package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ml575/database-project/jsondata"
)

// Method handler for post requests of documents, collections, and databases, takes a ResponseWriter and Request
// relies on document and collection put methods to be concurrent safe.
func (d *DatabaseIndex) post(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	username, validLogin := d.checkAuthorization(r.Header.Get("Authorization"))
	if !validLogin {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		return
	}

	encoded, err := io.ReadAll(r.Body)
	if err != nil {
		msg := `"unable to read request body"`
		errorHelper(w, msg, http.StatusBadRequest)
		return
	}

	// Create a JSONValue out of the slice of bytes read in from request body
	// fmt.Println("new document; encoded: ", encoded)
	var jsonRep jsondata.JSONValue

	unmarshal_err := json.Unmarshal(encoded, &jsonRep)
	if unmarshal_err != nil {
		errorHelper(w, `"unable to unmarshal encoded request body into JSONValue"`, http.StatusBadRequest)
		return
	}

	// Validate the encoded data
	validateErr := jsonRep.Validate(d.schema)
	if validateErr != nil {
		errorHelper(w, `"Request does not conform to database schema"`, http.StatusBadRequest)
		return
	}

	validJson := encodeCheck{Data: encoded}
	_, err = json.Marshal(validJson)
	if err != nil {
		errorHelper(w, `"invalid json encoding"`, http.StatusBadRequest)
		return
	}

	// splitPaths is a slice of the path segments, (guaranteed to start w/ database name by parseUrl())
	splitPaths, err := parseUrl(r.URL.Path)
	docName := ""
	retStatus := http.StatusCreated

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		return
	}

	endsOnCol, _, lastCol, lastGoodIndex, err := d.lastRealItem(splitPaths)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(splitPaths)%2 == 0 && splitPaths[len(splitPaths)-1] == "" {
		// checking content type for document only
		if r.Header.Get("Content-Type") != "application/json" {
			errorHelper(w, `"content type must be application/json"`, http.StatusBadRequest)
			slog.Error(`"content type must be application/json"`)
			return
		}
	} else {
		errorHelper(w, `"not collection path"`, http.StatusBadRequest)
		slog.Error(`"not collection path"`)
		return
	}

	if endsOnCol {
		//if we find a collection in the second to last spot (and this is not the first spot) and it is followed by a blank string, post
		if lastGoodIndex == len(splitPaths)-2 && splitPaths[len(splitPaths)-1] == "" {

			beenPlaced := false

			for !beenPlaced {

				timeStr := strconv.FormatInt(time.Now().UnixMilli(), 10)
				docName = timeStr

				funcVar := func(key string, currValue Documenter, exists bool) (Documenter, error) {
					if exists {
						return currValue, errors.New(`"document already exists"`)
					} else {
						newDoc := d.docFactory.NewDocument(key, encoded, username)
						urlPath := r.URL.Path[4:]
						urlPath = urlPath[strings.Index(urlPath, "/"):] + key
						newDocJson, err := newDoc.DocumentJsonMake(urlPath)
						if err != nil {
							return nil, errors.New(`"unable to format new document for subscriptions"`)
						}

						notificationHelper(key, lastCol, newDocJson)
						return newDoc, nil
					}
				}

				doc, err := lastCol.PutDocument(docName, funcVar)
				if err != nil && doc != nil {
					continue
				} else if err != nil {
					errorHelper(w, err.Error(), http.StatusNotFound)
					return
				} else {
					beenPlaced = true
				}
			}

			//lastDb.PutDocument(docName, funcVar)
			//tried to post with a bad document name following database/collection
		} else if lastGoodIndex == len(splitPaths)-2 {
			errorHelper(w, `"bad path"`, http.StatusBadRequest)
			return
		} else {
			errorHelper(w, `"document not found"`, http.StatusNotFound)
			return
		}

	} else {
		// if we end on a document give bad request
		if lastGoodIndex == len(splitPaths)-1 {
			errorHelper(w, `"bad request"`, http.StatusBadRequest)
			return
			// if we end on a document otherwise its a collection not found error
		} else {
			errorHelper(w, `"collection not found"`, http.StatusNotFound)
			return
		}

	}

	var jsonStr []byte
	putMessage := jsonPutMessageFormat{Uri: (r.URL.Path + docName)}
	jsonStr, err = json.Marshal(putMessage)
	if err != nil {
		msg := `"unable to format uri"`
		errorHelper(w, msg, http.StatusBadRequest)
		return
	}

	w.Header().Set("Location", r.URL.Path)
	w.WriteHeader(retStatus)
	w.Write(jsonStr)
}
