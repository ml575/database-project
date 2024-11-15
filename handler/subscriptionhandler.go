package handler

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// This interface also implements flush.
type writeFlusher interface {
	http.ResponseWriter
	http.Flusher
}

// This is a struct that has the document name and message to write for a subscription event.
type chanMessage struct {
	docName string
	message []byte
}

// This function handles the creation of a subscriber. All subscribers are stored in their corresponding collection
// where individual document subscribers just have their "query range" set to only their document name.
func createAndHandleSubscription(w http.ResponseWriter, r *http.Request, docName string, collection Collectioner) {
	wf, ok := w.(writeFlusher)
	if !ok {
		slog.Error("error converting writer to writeFlusher")
		errorHelper(w, `"streaming unsupported"`, http.StatusInternalServerError)
		return
	}

	wf.WriteHeader(http.StatusOK)
	wf.Flush()

	intervalQuery := r.URL.Query().Get("interval")
	var low string
	var high string
	if intervalQuery == "" {
		slog.Debug("interval query was blank")
		intervalQuery = "[,]"
	}
	if intervalQuery[0] != []byte("[")[0] || intervalQuery[len(intervalQuery)-1] != []byte("]")[0] ||
		len(strings.Split(intervalQuery, ",")) != 2 {
		errorHelper(w, `"malformed interval query parameter"`, http.StatusBadRequest)
		slog.Error("interval query did not follow correct format")
		return
	} else {
		interval := strings.Split(intervalQuery, ",")
		low = interval[0][1:]
		high = interval[1][:(len(interval[1]) - 1)]
		if high == "" {
			high = "\U0010FFFF"
		}
	}

	if docName != "" {
		slog.Info("got a document subscriber for document " + r.URL.Path)
		// setting bounds to be just this document
		low = docName
		high = docName
		doc, ok := collection.FindDocument(docName)
		if ok {
			// getting full path after database
			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			encoded, err := doc.DocumentJsonMake(urlPath)
			if err != nil {
				errorHelper(w, `"error marshaling document for subscription"`, http.StatusBadRequest)
				slog.Error("could not marshal document for subscription")
				return
			}

			slog.Info("writing the existing state of the document as an update")
			var eventAndData bytes.Buffer
			eventAndData.WriteString("event: update\ndata: ")
			var id bytes.Buffer
			id.WriteString(fmt.Sprintf("\nid: %d\n\n", time.Now().UnixMilli()))
			message := make([]byte, 0)
			message = append(message, eventAndData.Bytes()...)
			message = append(message, encoded...)
			message = append(message, id.Bytes()...)
			wf.Write(message)
			wf.Flush()
		}
	} else {
		slog.Info("got a collection subscriber for collection " + r.URL.Path)
		// getting documents within the interval and writing events
		documents := collection.QueryDocuments(r.Context(), low, high)
		if documents == nil {
			errorHelper(w, `"error querying documents for subscription"`, http.StatusBadRequest)
			slog.Error("error querying documents for subscription")
			return
		}
		curTime := time.Now().UnixMilli()
		for i := 0; i < len(documents); i++ {
			var eventAndData bytes.Buffer
			eventAndData.WriteString("event: update\ndata: ")
			var id bytes.Buffer
			id.WriteString(fmt.Sprintf("\nid: %d\n\n", curTime))
			message := make([]byte, 0)
			message = append(message, eventAndData.Bytes()...)

			// getting full path name after database
			urlPath := r.URL.Path[4:]
			urlPath = urlPath[strings.Index(urlPath, "/"):]
			encoded, err := documents[i].DocumentJsonMake(urlPath + documents[i].GetName())
			if err != nil {
				errorHelper(w, `"error marshaling document for subscription"`, http.StatusBadRequest)
				slog.Error("could not marshal document for collection subscription")
				return
			}

			message = append(message, encoded...)
			message = append(message, id.Bytes()...)
			wf.Write(message)
			wf.Flush()
		}
	}

	// creating reader and done channels
	reader := make(chan any)
	done := make(chan string)
	// always close the done channel when finished
	defer close(done)
	// add the subscriber and always delete it at the end
	collection.AddSubscriber(reader, done)
	defer collection.DeleteSubscriber(reader)

	for {
		select {
		// subscriber closed
		case <-r.Context().Done():
			return
		// got data to be written
		case data := <-reader:
			if data == nil {
				errorHelper(w, `"input to channel of wrong type"`, http.StatusBadRequest)
				slog.Error("data sent through channel for subscription not of right type")
				return
			}
			formattedData, ok := data.(chanMessage)
			if !ok {
				errorHelper(w, `"input to channel of wrong type"`, http.StatusBadRequest)
				slog.Error("data sent through channel for subscription not of right type")
				return
			}
			if formattedData.docName >= low && formattedData.docName <= high {
				wf.Write(formattedData.message)
				wf.Flush()
			}
		// pinging every 15 seconds if nothing happens
		case <-time.After(15 * time.Second):
			comment := ": keep alive\n\n"
			var evt bytes.Buffer
			evt.WriteString(comment)
			wf.Write(evt.Bytes())
			wf.Flush()
		}
	}
}

// This function tells the subscribers in a collection about an event that happened to a document.
func notifySubscriptions(docName string, collection Collectioner, message []byte) {
	collectionSubs := collection.AllSubscribers()
	for byteChan, doneChan := range collectionSubs {
		newMessage := chanMessage{docName: docName, message: message}
		select {
		// subscriber closed before we could write
		case <-doneChan:
			continue
		// writing to subscriber
		case byteChan <- newMessage:
			continue
		}
	}
}
