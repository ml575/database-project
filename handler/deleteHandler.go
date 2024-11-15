package handler

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// This function handles HTTP requests to delete documents and collections based on the provided url.
// Takes a response writer and a request. Relies on document and collection remove methods to be concurrent safe.
func (d *DatabaseIndex) delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	_, validLogin := d.checkAuthorization(r.Header.Get("Authorization"))
	if !validLogin {
		errorHelper(w, `"unauthorized"`, http.StatusUnauthorized)
		slog.Error("Unauthorized delete request")
		return
	}

	// splitPaths is a slice of the path segments, (guaranteed to start w/ database name by parseUrl())
	splitPaths, err := parseUrl(r.URL.Path)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		slog.Error("Error parsing delete request path")
		return
	}
	slog.Debug(fmt.Sprintf("delete request path parsed to %v", splitPaths))

	endsOnCol, lastDoc, lastCol, lastGoodIndex, err := d.lastRealItem(splitPaths)

	if err != nil {
		errorHelper(w, err.Error(), http.StatusBadRequest)
		slog.Error("Error parsing delete request path")
		return
	}

	if endsOnCol {
		// there is only one item in the path which is an existent database
		if len(splitPaths) == 1 {
			slog.Info(fmt.Sprintf("attempting to delte database %s", lastCol.GetName()))
			d.dbIndex.Remove(lastCol.GetName())
			//there is a collection name in the second to last spot and a blank spot at the end
		} else if (lastGoodIndex == len(splitPaths)-2) && (splitPaths[len(splitPaths)-1] == "") && (len(splitPaths) > 2) {
			_, ok := lastDoc.DeleteCollection(lastCol.GetName())
			slog.Info(fmt.Sprintf("attempting to delete collection %s", lastCol.GetName()))
			if !ok {
				errorHelper(w, `"could not delete collection"`, http.StatusBadRequest)
				slog.Error(fmt.Sprintf("error trying to delete collection %s", lastCol.GetName()))
				return
			}
			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			var message bytes.Buffer
			message.WriteString(fmt.Sprintf("event: delete\ndata: %q\nid: %d\n\n", urlPath, time.Now().UnixMilli()))
			notifySubscriptions("", lastCol, message.Bytes())
			// everything else that doesn't end in a found document gets a bad resource path
		} else {
			slog.Error("Not deleting database, not deleting collection, and document to delete not found")
			errorHelper(w, `"Document Not found"`, http.StatusNotFound)
			return
		}
	} else {
		//if the last found document is  the very last thing, we delete, otherwise give a bad resrouce path error
		if lastGoodIndex != len(splitPaths)-1 {
			slog.Error("last real item found is a document not at the end of the path")
			errorHelper(w, `"collection not found"`, http.StatusNotFound)
			return
		} else {
			slog.Info(fmt.Sprintf("attempting to delte document %s", lastDoc.GetName()))
			_, ok := lastCol.DeleteDocument(lastDoc.GetName())
			if !ok {
				errorHelper(w, `"could not delete document"`, http.StatusBadRequest)
				return
			}
			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			var message bytes.Buffer
			message.WriteString(fmt.Sprintf("event: delete\ndata: %q\nid: %d\n\n", urlPath, time.Now().UnixMilli()))
			notifySubscriptions(lastDoc.GetName(), lastCol, message.Bytes())
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
