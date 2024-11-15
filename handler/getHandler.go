package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// Method handler for get requests of documents, collections, and databases, takes a ResponseWriter and Request
// relies on document and database find methods to be concurrent safe.
func (d *DatabaseIndex) get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	_, validLogin := d.checkAuthorization(r.Header.Get("Authorization"))
	if !validLogin {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		slog.Error("unauthorized")
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode != "" && mode != "subscribe" {
		errorHelper(w, `"invalid query parameter"`, http.StatusBadRequest)
		slog.Error("invalid mode")
		return
	}

	if mode == "subscribe" {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}

	// splitPaths is a slice of the path segments, (guaranteed to start w/ database name by parseUrl())
	splitPaths, err := parseUrl(r.URL.Path)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		slog.Error("error parsing path for get request")
		return
	}
	slog.Debug(fmt.Sprintf("get request path parsed to %v", splitPaths))

	endsOnCol, lastDoc, lastCol, lastGoodIndex, err := d.lastRealItem(splitPaths)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		slog.Error("error parsing path for get request")
		return
	}
	slog.Debug(fmt.Sprintf("last found real item is at index %d in path %v", lastGoodIndex, splitPaths))

	intervalQuery := r.URL.Query().Get("interval")

	var jsonStr []byte

	if endsOnCol {
		// // if the last item is a database, give insufficient path length
		if lastGoodIndex == len(splitPaths)-1 {
			errorHelper(w, `"insufficient path length"`, http.StatusBadRequest)
			slog.Error("get request ended in a database")
			return

			// if the last found collection is the second to last item followed by a blank string, get the collection
		} else if lastGoodIndex == len(splitPaths)-2 && splitPaths[len(splitPaths)-1] == "" {

			if mode == "subscribe" {
				createAndHandleSubscription(w, r, "", lastCol)
				return
			}

			var low string
			var high string
			if intervalQuery == "" {
				intervalQuery = "[,]"
			}
			if intervalQuery[0] != []byte("[")[0] || intervalQuery[len(intervalQuery)-1] != []byte("]")[0] ||
				len(strings.Split(intervalQuery, ",")) != 2 {
				errorHelper(w, `"malformed interval query parameter"`, http.StatusBadRequest)
				slog.Error("invalid interval query")
				return
			} else {
				interval := strings.Split(intervalQuery, ",")
				low = interval[0][1:]
				high = interval[1][:(len(interval[1]) - 1)]
				if high == "" {
					high = "\U0010FFFF"
				}
			}

			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			jsonStr, err = lastCol.CollectionJsonMake(r.Context(), low, high, urlPath)
			if err != nil {
				errorHelper(w, `"error formatting return json"`, http.StatusBadRequest)
				slog.Error("error formatting return json")
				return
			}

			//otherwise, a document not found error
		} else {
			errorHelper(w, `"Document does not exist"`, http.StatusNotFound)
			slog.Error("non existent document")
			return
		}
	} else {
		// if the last found thing is a document and it is not at the very end, give collection does not exist
		// right now this will probably trigger even if the thing at the very end is a trailing slash
		if lastGoodIndex != len(splitPaths)-1 {
			errorHelper(w, `"Collection does not exist"`, http.StatusNotFound)
			slog.Error("collection does not exist")
			return
			// otherwise make a json of the last found document
		} else {
			if mode == "subscribe" {
				createAndHandleSubscription(w, r, lastDoc.GetName(), lastCol)
				return
			}
			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			jsonStr, err = lastDoc.DocumentJsonMake(urlPath)
			if err != nil {
				errorHelper(w, `"error formatting return json"`, http.StatusBadRequest)
				slog.Error("error formatting json")
				return
			}
		}

	}

	w.WriteHeader(http.StatusOK)
	w.Write(jsonStr)
}
