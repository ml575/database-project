package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/ml575/database-project/auth"
	"github.com/ml575/database-project/collection"
	"github.com/ml575/database-project/document"
	"github.com/ml575/database-project/handler"
	"github.com/ml575/database-project/patchvisitors"
	"github.com/ml575/database-project/skipList"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type dbResponse struct {
	Uri string
}

type metadata struct {
	CreatedAt      int64  `json:"createdAt"`
	CreatedBy      string `json:"createdBy"`
	LastModifiedAt int64  `json:"lastModifiedAt"`
	LastModifiedBy string `json:"lastModifiedBy"`
}

type docResponse struct {
	Path string   `json:"path"`
	Doc  testDoc  `json:"doc"`
	Meta metadata `json:"meta"`
}

type testDoc struct {
	Str string `json:"str"`
}

func TestSingleDatabaseAndDocument(t *testing.T) {

	log.SetOutput(io.Discard)

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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	req := httptest.NewRequest("GET", "/v1/db1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Check the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	var createResult dbResponse
	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	if createResult.Uri != "/v1/db1" {
		t.Errorf("Expected value \"/v1/db1\" but got %s", createResult.Uri)
	}

	req = httptest.NewRequest("GET", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	testDc := testDoc{Str: "testing"}
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	if createResult.Uri != "/v1/db1/dc1" {
		t.Errorf("Expected value \"/v1/db1/dc1\" but got %s", createResult.Uri)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	var testDocUnmarshaled docResponse
	err = json.Unmarshal(body, &testDocUnmarshaled)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	if testDocUnmarshaled.Doc != testDc || testDocUnmarshaled.Path != "/dc1" {
		t.Errorf("Stored document is incorrect, %v", testDocUnmarshaled)
	}

	req = httptest.NewRequest("GET", "/v1/db1/dc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}
}

func TestNestedPaths(t *testing.T) {

	//log.SetOutput(io.Discard)

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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	testDc := testDoc{"testing"}
	testDocMarshaled, err := json.Marshal(testDc)

	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	req := httptest.NewRequest("DELETE", "/v1/db1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1%2F/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc2/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/doesntExist/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/doesntExist/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abcd")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2/col3", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("GET", "/v1/db1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/doc2?mode=sub", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/?interval=[]", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/doc2", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/notreal/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/notreal/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/notreal/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/notreal/doc1/col1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/notreal/doc1/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/notReal", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/notReal/2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/notReal/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/notReal/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/notReal/col1/doc2/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/doesnt/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/doesnt", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/doesnt/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/doesnt/doc2/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/doesnt/doc2/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/doesnt/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/doesnt/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doesnt/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doesnt/col1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doesnt", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/doesnt/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/doesnt/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/doesnt", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/col1/doc2/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/doc2", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/doc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/db1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	var createResult dbResponse
	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	req = httptest.NewRequest("GET", createResult.Uri, nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	go main()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/doc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("PUT", "/v1/db1/doc1/col1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("POST", "/v1/db1/doc1/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	req = httptest.NewRequest("POST", "/v1/db1/doc1/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abcd")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/db1/doc1/col1/a", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", createResult.Uri, nil)
	req.Header.Set("Authorization", "Bearer")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", createResult.Uri, nil)
	req.Header.Set("Authorization", "Bearer aaaaaa")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", createResult.Uri, nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/db1/doesntexist/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/doesntExist/doc1/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/doesntExist/doc1/col1/", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/v1/doesntExist/doc1/col1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req = httptest.NewRequest("OPTIONS", "/v1/", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("OPTIONS", "/auth", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}
}

func TestConcurrentData(t *testing.T) {

	log.SetOutput(io.Discard)

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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201, but got %d", w.Code)
	}

	num201s := make(chan int)

	testDc := testDoc{"testing"}
	testDocMarshaled, err := json.Marshal(testDc)

	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	for iter := 0; iter < 100; iter++ {
		go func() {
			req := httptest.NewRequest("PUT", "/v1/db1/doc1", bytes.NewReader(testDocMarshaled))
			req.Header.Set("Authorization", "Bearer abc")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			resp := w.Result()
			if resp.StatusCode == 201 {
				num201s <- 1
			} else if resp.StatusCode == 200 {
				num201s <- 0
			} else {
				t.Errorf("got something other than 201 or 200")
			}
		}()
	}

	go func() {
		total := 0
		for iter := 0; iter < 100; iter++ {
			numFound := <-num201s
			total += numFound
		}

		if total != 1 {
			t.Errorf("expected one 201, got a different number")
		}
	}()

}

type authInput struct {
	Username string `json:"username"`
}

type authOutput struct {
	Token string `json:"token"`
}

func TestAuth(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	req := httptest.NewRequest("POST", "/auth", nil)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("POST", "/auth", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	input := authInput{"test"}
	marshaled, err := json.Marshal(input)
	if err != nil {
		t.Errorf("could not marshal auth input")
	}
	req = httptest.NewRequest("POST", "/auth", bytes.NewReader(marshaled))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}
	var output authOutput
	err = json.Unmarshal(body, &output)
	if err != nil {
		t.Errorf("could not unmarshal output")
	}

	req = httptest.NewRequest("DELETE", "/auth", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/auth", nil)
	req.Header.Set("Authorization", "Bearer "+"notreal")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/auth", nil)
	req.Header.Set("Authorization", "Bearer")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 401 {
		t.Errorf("Expected status code 401 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/auth", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/auth", nil)
	req.Header.Set("Authorization", "Bearer "+output.Token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}
}

func TestSubscription(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	req := httptest.NewRequest("GET", "/v1/db1/", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Check the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	var createResult dbResponse
	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	if createResult.Uri != "/v1/db1" {
		t.Errorf("Expected value \"/v1/db1\" but got %s", createResult.Uri)
	}

	req = httptest.NewRequest("GET", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Errorf("Expected status code 404 but got %d", w.Code)
	}

	testDc := testDoc{Str: "testing"}
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Error reading response body: %v", err)
	}

	err = json.Unmarshal(body, &createResult)
	if err != nil {
		t.Errorf("Error unmarshalling response body: %v", err)
	}

	if createResult.Uri != "/v1/db1/dc1" {
		t.Errorf("Expected value \"/v1/db1/dc1\" but got %s", createResult.Uri)
	}

	go func() {
		localreq := httptest.NewRequest("GET", "/v1/db1/dc1?mode=subscribe", nil)
		localreq.Header.Set("Authorization", "Bearer abc")
		localw := httptest.NewRecorder()
		handler.ServeHTTP(localw, localreq)
	}()
	time.Sleep(time.Millisecond)
	go func() {
		localreq := httptest.NewRequest("GET", "/v1/db1/?mode=subscribe", nil)
		localreq.Header.Set("Authorization", "Bearer abc")
		localw := httptest.NewRecorder()
		handler.ServeHTTP(localw, localreq)
	}()
	time.Sleep(time.Millisecond)
	go func() {
		localreq := httptest.NewRequest("GET", "/v1/db1/?mode=subscribe&interval=[d]", nil)
		localreq.Header.Set("Authorization", "Bearer abc")
		localw := httptest.NewRecorder()
		handler.ServeHTTP(localw, localreq)
	}()
	time.Sleep(time.Millisecond)

	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	req = httptest.NewRequest("PUT", "/v1/db1/dc1?mode=nooverwrite", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 412 {
		t.Errorf("Expected status code 412 but got %d", w.Code)
	}

	req = httptest.NewRequest("DELETE", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 204 {
		t.Errorf("Expected status code 204 but got %d", w.Code)
	}
}

func TestQueryCopy(t *testing.T) {
	dbFactory := CollectionFactory(collection.NewCollection[handler.Documenter])
	docFactory := DocumentFactory(document.NewDocument[handler.Collectioner])

	db := dbFactory.NewCollection("test")
	testDc := testDoc{Str: "testing"}
	encoded, _ := json.Marshal(testDc)
	funcVar := func(key string, currValue handler.Documenter, exists bool) (handler.Documenter, error) {
		if exists {
			currValue.ModifyMetadata("test")
			currValue.ReplaceData(encoded)
			return currValue, nil
		} else {
			doc := docFactory.NewDocument(key, encoded, "test")
			return doc, nil
		}
	}
	// put document and get it from query
	db.PutDocument("test", funcVar)
	docs := db.QueryDocuments(context.TODO(), "test", "test")
	var printDoc docResponse
	doc1, _ := docs[0].DocumentJsonMake("test")
	json.Unmarshal(doc1, &printDoc)
	time.Sleep(time.Millisecond * 2)
	// put again and remake json to see if content changed
	db.PutDocument("test", funcVar)
	var printDoc2 docResponse
	doc2, _ := docs[0].DocumentJsonMake("test")
	json.Unmarshal(doc2, &printDoc2)
	if printDoc.Meta != printDoc2.Meta {
		t.Errorf("metadatas different")
	}
	// query once again to see if new value appears now
	docs2 := db.QueryDocuments(context.TODO(), "test", "test")
	doc2, _ = docs2[0].DocumentJsonMake("test")
	json.Unmarshal(doc2, &printDoc2)
	if printDoc.Meta == printDoc2.Meta {
		t.Errorf("metadatas same")
	}
}

func TestSimplePatch(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
			"op": "ObjectAdd",
			"path": "/prop4",
			"value": 100
		}
	]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	var responseUnmarshaled map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc := `{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true,
		"prop4": 100
	}`
	var expectedPatchMap map[string]interface{}
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok := responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

}

func TestComplexPatch(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "/a~1b",
		  "value": [1, 5]
		},
		{
		  "op": "ArrayRemove",
		  "path": "/a~1b",
		  "value": 5
		},
		{
		  "op": "ArrayAdd",
		  "path": "/a~1b",
		  "value": 2
		},
		{
		  "op": "ArrayAdd",
		  "path": "/a~1b",
		  "value": {"inner1": "bang"}
		},
		{
		  "op": "ObjectAdd",
		  "path": "/a~1b/2/c~0d",
		  "value": {"inner2": "boom", "inner3": "pom"}
		},
		{
		  "op": "ObjectAdd",
		  "path": "/a~1b/2/c~0d/inner4",
		  "value": ["no", "yessir"]
		},
		{
		  "op": "ArrayRemove",
		  "path": "/a~1b/2/c~0d/inner4",
		  "value": "no"
		},
		{
		  "op": "ArrayAdd",
		  "path": "/a~1b/2/c~0d/inner4",
		  "value": "yes ma'am"
		}
	  ]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	var responseUnmarshaled map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc := `{
		"a/b": [
			1,
			2,
			{
				"c~d": {
					"inner2": "boom",
					"inner3": "pom",
					"inner4": [
						"yessir",
						"yes ma'am"
					]
				},
				"inner1": "bang"
			}
		],
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`
	var expectedPatchMap map[string]interface{}
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok := responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

}

func TestBadPatchPathEnds(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "",
		  "value": [1, 5]
		}
	  ]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	var responseUnmarshaled map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling PATCH response body")
	}

	patchFailed, ok := responseUnmarshaled["patchFailed"]
	if !ok {
		t.Errorf("no patchFailed field in unmarshaled")
	}

	message, ok := responseUnmarshaled["message"]
	if !ok {
		t.Errorf("no message field in unmarshaled")
	}

	if patchFailed != true || message != "error applying patches: path ends in map" {
		t.Errorf("actual patchFailed and message %v, %v and expected response %v, %v not the same",
			patchFailed, message, true, "error applying patches: path ends in map")
	}

}

func TestBadPatchRequest(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": ""
		}
	  ]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc = json.RawMessage(`
		{
		  "op": "ObjectAdd",
		  "path": "",
		  "value": "newVal"
		}
	  `)
	patchDocMarshaled, err = json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 400 {
		t.Errorf("Expected status code 400 but got %d", w.Code)
	}

}

func TestPatchBadPathIndex(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "/a~1b",
		  "value": [1, 5]
		},
		{
		  "op": "ArrayRemove",
		  "path": "/a~1b",
		  "value": 5
		},
		{
		  "op": "ArrayAdd",
		  "path": "/a~1b",
		  "value": 2
		},
		{
		  "op": "ArrayAdd",
		  "path": "/a~1b",
		  "value": {"inner1": "bang"}
		},
		{
		  "op": "ObjectAdd",
		  "path": "/a~1b/3/c~0d",
		  "value": {"inner2": "boom", "inner3": "pom"}
		}
	  ]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	var responseUnmarshaled map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc := `{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`
	var expectedPatchMap map[string]interface{}
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok := responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

}

func TestPatchBadPathLiterals(t *testing.T) {
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

	dbIndexDatabases := skipList.New[string, handler.Collectioner]("databaseList", "", "\U0010FFFF")

	compiler := jsonschema.NewCompiler()
	schema, _ := compiler.Compile("schema1.json")

	authMap := auth.NewAuth()
	authMap.AddPair("test", "abc", time.Now().Add(time.Hour))
	handler := handler.New(dbFactory, docFactory, authMap, schema, dbIndexDatabases, patchOpListVisitorFactory, visitorFactory, docVisitorFactory, patchOpFactory)

	// PUT a database
	req := httptest.NewRequest("PUT", "/v1/db1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PUT document request body
	testDc := json.RawMessage(`{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`)
	testDocMarshaled, err := json.Marshal(testDc)
	if err != nil {
		t.Errorf("Error marshaling test document: %v", err)
	}

	// PUT a document
	req = httptest.NewRequest("PUT", "/v1/db1/dc1", bytes.NewReader(testDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 201 {
		t.Errorf("Expected status code 201 but got %d", w.Code)
	}

	// Create PATCH document request body
	patchDc := json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "/prop3",
		  "value": [1, 5]
		}
	  ]`)
	patchDocMarshaled, err := json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	var responseUnmarshaled map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc := `{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`
	var expectedPatchMap map[string]interface{}
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok := responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

	// Create PATCH document request body
	patchDc = json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "/prop1",
		  "value": [1, 5]
		}
	  ]`)
	patchDocMarshaled, err = json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc = `{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok = responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

	// Create PATCH document request body
	patchDc = json.RawMessage(`[
		{
		  "op": "ObjectAdd",
		  "path": "/prop2",
		  "value": [1, 5]
		}
	  ]`)
	patchDocMarshaled, err = json.Marshal(patchDc)
	if err != nil {
		t.Errorf("Error marshaling patch operations: %v", err)
	}

	// PATCH a document
	req = httptest.NewRequest("PATCH", "/v1/db1/dc1", bytes.NewReader(patchDocMarshaled))
	req.Header.Set("Authorization", "Bearer abc")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	// GET the document we just PUT
	req = httptest.NewRequest("GET", "/v1/db1/dc1", nil)
	req.Header.Set("Authorization", "Bearer abc")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200 but got %d", w.Code)
	}

	err = json.Unmarshal(w.Body.Bytes(), &responseUnmarshaled)
	if err != nil {
		t.Errorf("Error unmarshaling GET response body")
	}

	// Get expected patch results
	expectedPatchDoc = `{
		"prop1": "hello",
		"prop2": 5,
		"prop3": true
	}`
	json.Unmarshal([]byte(expectedPatchDoc), &expectedPatchMap)

	doc, ok = responseUnmarshaled["doc"]
	if !ok {
		t.Errorf("no doc field in unmarshaled")
	}

	if !reflect.DeepEqual(doc, expectedPatchMap) {
		t.Errorf("actual %v and expected %v not the same", doc, expectedPatchDoc)
	}

}
