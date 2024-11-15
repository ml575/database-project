// Package main processes command line args starts a server, and initiates listen and serve.
// Additionally, NewCollection and NewDocument methods are injected into the handler package.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ml575/database-project/auth"
	"github.com/ml575/database-project/collection"
	"github.com/ml575/database-project/document"
	"github.com/ml575/database-project/handler"
	"github.com/ml575/database-project/jsondata"
	"github.com/ml575/database-project/patchvisitors"
	"github.com/ml575/database-project/skipList"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// CollectionFactory is a type wrapper around the NewCollection function. It is used to create a NewColleciton function whose output is a Collectioner
type CollectionFactory func(name string, dbIndex collection.Indexer[handler.Documenter]) *collection.Collection[handler.Documenter]

// This is a function of CollectionFactory which takes the name of a collection as a string and returns a structure matching the Collectioner interface
func (d CollectionFactory) NewCollection(name string) handler.Collectioner {
	skipList := skipList.New[string, handler.Documenter](name, "", "\U0010FFFF")
	return d(name, skipList)
}

// DocumentFactory is a type wrapper around the NewDocument function. It is used to create a NewDocument function whose output is a Documenter
type DocumentFactory func(name string, collectionIndex document.Indexer[handler.Collectioner], data []byte, creator string) *document.Document[handler.Collectioner]

// This is a function of DocumentFactory which takes the name of a document, the document data,
// a flag signalling if the new document should use the createdAt time of a previous documen, and that original creation time
// it returns a structure matching the Documenter interface
func (d DocumentFactory) NewDocument(name string, data []byte, creator string) handler.Documenter {
	skipList := skipList.New[string, handler.Collectioner](name, "", "\U0010FFFF")
	return d(name, skipList, data, creator)
}

// PatchVisitorFactory is a type wrapper around the NewPatchVisitor function. It is used to create a NewPatchVisitor function whose output is a PatchVisitor
type PatchVisitorFactory func(patchFactory handler.PatchOpFactory) patchvisitors.PatchVisitor[handler.PatchOper, handler.PatchOpFactory]

// This is a function of PatchVisitorFactory which takes in a PatchOpFactory and returns a PatchVisitor.
func (p PatchVisitorFactory) NewPatchVisitor(patchFactory handler.PatchOpFactory) handler.PatchVisitor {
	return p(patchFactory)
}

// PatchOpFactory is a type wrapper around the NewPatchOp function. It is used to create a NewPatchOp function whose output is a PatchOper
type PatchOpFactory func(op string, path string, value jsondata.JSONValue) *patchvisitors.PatchOp

// This is a function of PatchOpFactory which takes in the operation string, path string, and the json value and returns a PatchOper
func (p PatchOpFactory) NewPatchOp(op string, path string, value jsondata.JSONValue) handler.PatchOper {
	return p(op, path, value)
}

// DocVisitorFactory is a type wrapper around the NewDocVisitor Function. It is used to create a NeDocVisitor function whose outputs is a DocVisitor.
type DocVisitorFactory func(op string, path string, value jsondata.JSONValue) *patchvisitors.DocVisitor

// This is a function of DocVisitorFactory which takes in the operation string, path string, and json value and returns a DocVisitor
func (p DocVisitorFactory) NewDocVisitor(op string, path string, value jsondata.JSONValue) handler.DocVisitor {
	return p(op, path, value)
}

// PatchOpListVisitorFactory is a type wrapper around the NewPatchOpListVisitor function. It is used to create a NewPatchOpListVisitor function and return
// a PatchOpListVisitor
type PatchOpListVisitorFactory func() *patchvisitors.PatchOpListVisitor

// This is a function of PatchOpListVisitorFactory. It returns a PatchOpListVisitor
func (p PatchOpListVisitorFactory) NewPatchOpListVisitor() handler.PatchOpListVisitor {
	return p()
}

// Running the server.
func main() {
	var server http.Server
	var port int
	var schemaFile string
	var tokensFile string
	var err error

	flag.IntVar(&port, "p", 3318, "This is the port the server listens to.")
	flag.StringVar(&schemaFile, "s", "", "This is the file containing the JSON schema "+
		"that all documents in the database must abide by.")
	flag.StringVar(&tokensFile, "t", "", "This is the file containing the mapping of usernames to string tokens.")

	flag.Parse()

	if schemaFile == "" {
		fmt.Println("No schema file provided")
		return
	}

	compiler := jsonschema.NewCompiler()

	schema, err := compiler.Compile(schemaFile)
	if err != nil {
		slog.Error("schema compilation error", "error", err)
		return
	}

	authMap := auth.NewAuth()
	if tokensFile != "" {
		data, err := os.ReadFile(tokensFile)
		if err != nil {
			fmt.Println("Cannot open tokens file")
		} else {
			nameToToken := make(map[string]string)
			err = json.Unmarshal(data, &nameToToken)
			if err != nil {
				fmt.Println("Cannot unmarshal tokens file")
			} else {
				expiry := time.Now().Add(time.Hour * 24)
				for name, token := range nameToToken {
					authMap.AddPair(name, token, expiry)
				}
			}
		}
	}

	newDb := collection.NewCollection[handler.Documenter]
	dbFactory := CollectionFactory(newDb)
	newDoc := document.NewDocument[handler.Collectioner]
	docFactory := DocumentFactory(newDoc)

	newVisitor := patchvisitors.NewPatchVisitor[handler.PatchOper, handler.PatchOpFactory]
	visitorFactory := PatchVisitorFactory(newVisitor)

	newDocVisitor := patchvisitors.NewDocVisitor
	docVisitorFactory := DocVisitorFactory(newDocVisitor)

	newPatchOpListVisitor := patchvisitors.NewPatchOpListVisitor
	patchOpListVisitorFactory := PatchOpListVisitorFactory(newPatchOpListVisitor)

	newPatchOp := patchvisitors.NewPatchOp
	patchOpFactory := PatchOpFactory(newPatchOp)

	server.Addr = ":" + strconv.Itoa(port)
	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")
	server.Handler = handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)
	fmt.Println(port, schemaFile, tokensFile)

	// The following code should go last and remain unchanged.
	// Note that you must actually initialize 'server' and 'port'
	// before this.  Note that the server is started below by
	// calling ListenAndServe.  You must not start the server
	// before this.

	// signal.Notify requires the channel to be buffered
	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt, syscall.SIGTERM)
	go func() {
		// Wait for Ctrl-C signal
		<-ctrlc
		server.Close()
	}()

	// Start server
	slog.Info("Listening", "port", port)
	err = server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		slog.Error("Server closed", "error", err)
	} else {
		slog.Info("Server closed", "error", err)
	}
}
