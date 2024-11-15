package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ml575/database-project/jsondata"
)

// This struct lets us extract the body data and check whether it is of proper json format.
type encodeCheck struct {
	Data json.RawMessage `json:"data"`
}

// Method handler for post requests of documents, collections, and databases, takes a ResponseWriter and Request
// relies on document and database put methods to be concurrent safe.
func (d *DatabaseIndex) put(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	username, validLogin := d.checkAuthorization(r.Header.Get("Authorization"))
	if !validLogin {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		slog.Error("unauthorized")
		return
	}

	// splitPaths is a slice of the path segments, (guaranteed to start w/ database name by parseUrl())
	splitPaths, err := parseUrl(r.URL.Path)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(splitPaths) > 1 && len(splitPaths)%2 == 0 && splitPaths[len(splitPaths)-1] != "" {
		// checking content type for document only
		if r.Header.Get("Content-Type") != "application/json" {
			errorHelper(w, `"content type must be application/json"`, http.StatusBadRequest)
			slog.Error("content type must be application/json")
			return
		}
	}

	var encoded []byte
	if len(splitPaths) != 1 && splitPaths[len(splitPaths)-1] != "" {
		encoded, err = io.ReadAll(r.Body)
		if err != nil {
			errorHelper(w, `"unable to read request body"`, http.StatusBadRequest)
			slog.Error("unable to read request body")
			return
		}
		validJson := encodeCheck{Data: encoded}
		_, err = json.Marshal(validJson)
		if err != nil {
			errorHelper(w, `"invalid json encoding"`, http.StatusBadRequest)
			slog.Error("invalid json encoding")
			return
		}
	}

	if len(splitPaths) == 2 && splitPaths[1] == "" {
		errorHelper(w, `"Bad Path"`, http.StatusBadRequest)
		slog.Error("bad path")
		return
	}

	endsOnCol, lastDoc, lastCol, lastGoodIndex, err := d.lastRealItem(splitPaths)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		slog.Error(err.Error())
		return
	}

	retStatus := http.StatusCreated
	modeQuery := r.URL.Query().Get("mode")
	if modeQuery != "" && modeQuery != "overwrite" && modeQuery != "nooverwrite" {
		errorHelper(w, `"mode of incorrect format"`, http.StatusBadRequest)
		slog.Error("mode of incorrect format")
		return
	}

	if endsOnCol {
		//very last element is aready exisitng database/collection
		if lastGoodIndex == len(splitPaths)-1 {
			errorHelper(w, `"database already exists"`, http.StatusBadRequest)
			slog.Error("db already exists")
			return
			//fails to find a database's child (a dcument) early on in path (not in last two elements)
		} else if lastGoodIndex < len(splitPaths)-2 {
			errorHelper(w, `"Containing document does not exist"`, http.StatusNotFound)
			slog.Error("containing doc does not exist")
			return
			//else we know the second to last element is a database, and we need to add a document based on the last element
		} else {
			// if a document already exists with the same name, retrieve the time it was created
			docName := splitPaths[len(splitPaths)-1]
			if docName == "" {
				//400, this is sometimes a bad path and sometimes a already exists error (but 400 either way)
				errorHelper(w, `"bad path"`, http.StatusBadRequest)
				slog.Error("bad path")
				return
			}

			// Create a JSONValue out of the slice of bytes read in from request body
			slog.Debug("new document")
			var jsonRep jsondata.JSONValue

			unmarshal_err := json.Unmarshal(encoded, &jsonRep)
			if unmarshal_err != nil {
				errorHelper(w, `"unable to unmarshal encoded request body into JSONValue"`, http.StatusBadRequest)
				slog.Error("unable to unmarshal encoded request body into JSONValue")
				return
			}

			// Validate the encoded data
			validateErr := jsonRep.Validate(d.schema)
			if validateErr != nil {
				errorHelper(w, `"Request does not conform to database schema"`, http.StatusBadRequest)
				slog.Error("Request does not conform to database schema")
				return
			}

			funcVar := func(key string, currValue Documenter, exists bool) (Documenter, error) {
				if exists {
					currValue.ModifyMetadata(username)
					currValue.ReplaceData(encoded)
					retStatus = http.StatusOK

					urlPath := r.URL.Path[4:]
					urlPath = urlPath[strings.Index(urlPath, "/"):]
					newDocJson, err := currValue.DocumentJsonMake(urlPath)
					if err != nil {
						return nil, errors.New(`"unable to format document for subscriptions"`)
					}

					slog.Info("replaced document data")

					notificationHelper(key, lastCol, newDocJson)

					return currValue, nil
				} else {
					doc := d.docFactory.NewDocument(key, encoded, username)

					urlPath := r.URL.Path[4:]
					urlPath = urlPath[strings.Index(urlPath, "/"):]
					newDocJson, err := doc.DocumentJsonMake(urlPath)
					if err != nil {
						return nil, errors.New(`"unable to format new document for subscriptions"`)
					}

					slog.Info("created new document")

					notificationHelper(key, lastCol, newDocJson)

					return doc, nil
				}
			}
			_, err = lastCol.PutDocument(docName, funcVar)
			if err != nil {
				errorHelper(w, err.Error(), http.StatusBadRequest)
				slog.Error(err.Error())
				return
			}
		}

	} else {
		// last good item is a document at the end of the path... we need to overwrite it
		if lastGoodIndex == len(splitPaths)-1 {
			// If mode is set to nooverwrite, we send error 412
			if modeQuery == "nooverwrite" {
				errorHelper(w, `"document already exists"`, http.StatusPreconditionFailed)
				slog.Error("document already exists")
				return
			}

			docName := splitPaths[len(splitPaths)-1]
			if docName == "" {
				errorHelper(w, `"document name too short"`, http.StatusBadRequest)
				slog.Error("doc name too short")
				return
			}
			retStatus = http.StatusOK

			// Create a JSONValue out of the slice of bytes read in from request body
			slog.Debug(fmt.Sprintf("overwrite document; encoded: %v", encoded))
			var jsonRep jsondata.JSONValue

			unmarshal_err := json.Unmarshal(encoded, &jsonRep)
			if unmarshal_err != nil {
				errorHelper(w, `"unable to unmarshal encoded request body into JSONValue"`, http.StatusBadRequest)
				slog.Error("cannot unmarshal encoded request into JSONValue")
				return
			}

			// Validate the encoded data
			validateErr := jsonRep.Validate(d.schema)
			if validateErr != nil {
				errorHelper(w, `"Request does not conform to database schema"`, http.StatusBadRequest)
				slog.Error("Request does not conform to database schema")
				return
			}

			funcVar := func(key string, currValue Documenter, exists bool) (Documenter, error) {
				if exists {
					currValue.ModifyMetadata(username)
					currValue.ReplaceData(encoded)
					retStatus = http.StatusOK

					urlPath := r.URL.Path[4:]
					urlPath = urlPath[strings.Index(urlPath, "/"):]
					newDocJson, err := currValue.DocumentJsonMake(urlPath)
					if err != nil {
						return nil, errors.New(`"unable to format document for subscriptions"`)
					}

					slog.Info("modified document")

					notificationHelper(key, lastCol, newDocJson)

					return currValue, nil
				} else {
					// Should be impossible.
					doc := d.docFactory.NewDocument(key, encoded, username)

					urlPath := r.URL.Path[4:]
					urlPath = urlPath[strings.Index(urlPath, "/"):]
					newDocJson, err := doc.DocumentJsonMake(urlPath)
					if err != nil {
						return nil, errors.New(`"unable to format new document for subscriptions"`)
					}

					slog.Info("created new document")

					notificationHelper(key, lastCol, newDocJson)

					return doc, nil
				}
			}
			_, err = lastCol.PutDocument(docName, funcVar)
			if err != nil {
				errorHelper(w, err.Error(), http.StatusBadRequest)
				slog.Error(err.Error())
				return
			}
			// we're just putting a database
		} else if lastGoodIndex == -1 && len(splitPaths) == 1 {
			dbName := splitPaths[0]
			if dbName == "" {
				errorHelper(w, `"bad path"`, http.StatusBadRequest)
				slog.Error("bad path")
				return
			}

			funcVar := func(key string, currValue Collectioner, exists bool) (Collectioner, error) {
				if exists {
					return currValue, errors.New(`"database already exists"`)
				} else {
					return d.colFactory.NewCollection(dbName), nil
				}

			}
			_, err = d.dbIndex.CallUpsert(dbName, funcVar)
			if err != nil {
				errorHelper(w, err.Error(), http.StatusBadRequest)
				slog.Error(err.Error())
				return
			}
			// can't find first database
		} else if lastGoodIndex == -1 {
			errorHelper(w, `"containing database does not exist"`, http.StatusNotFound)
			slog.Error("containing database does not exist")
			return
			// missing collection somewhere in the middle of the path (not in last three spots)
		} else if lastGoodIndex < len(splitPaths)-3 {
			errorHelper(w, `"containing collection does not exist"`, http.StatusNotFound)
			slog.Error("containing collection does not exist")
			return
			// document in third to last spot, and does not end with a trailing slash (missing the last database)
		} else if lastGoodIndex == len(splitPaths)-3 && splitPaths[len(splitPaths)-1] != "" {
			errorHelper(w, `"containing collection does not exist"`, http.StatusNotFound)
			slog.Error("containing collection does not exist")
			return
			//ends with good document and non-existent collection name with no slash

		} else {
			colName := splitPaths[len(splitPaths)-2]

			funcVar := func(key string, currValue Collectioner, exists bool) (Collectioner, error) {
				if exists {
					return currValue, errors.New(`"already exists"`)
				} else {
					return d.colFactory.NewCollection(colName), nil
				}
			}
			_, err = lastDoc.PutCollection(colName, funcVar)
			if err != nil {
				errorHelper(w, err.Error(), http.StatusBadRequest)
				slog.Error(err.Error())
				return
			}
		}

	}

	var jsonStr []byte
	putMessage := jsonPutMessageFormat{Uri: r.URL.Path}
	jsonStr, err = json.Marshal(putMessage)
	if err != nil {
		errorHelper(w, `"unable to format uri"`, http.StatusBadRequest)
		slog.Error("unable to format uri")
		return
	}

	w.Header().Set("Location", r.URL.Path)
	w.WriteHeader(retStatus)
	w.Write(jsonStr)
}
