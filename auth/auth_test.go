package auth

import (
	"testing"
	"time"
)

func TestNewAuth(t *testing.T) {
	auth := NewAuth()
	if auth == nil {
		t.Errorf("Expected NewAuth to return a non-nil Auth instance")
	}
}

func TestAddToken(t *testing.T) {
	auth := NewAuth()
	username := "user"
	token := auth.AddToken(username)
	if token == "" {
		t.Errorf("expected nonempty token")
	}

	storedData, ok := auth.tokens.Load(token)
	if !ok {
		t.Errorf("expected token stored")
	}

	data := storedData.(nameAndExp)
	if data.name != username {
		t.Errorf("wanted username to be %s, but got %s", username, data.name)
	}
	if time.Now().After(data.expiry) {
		t.Errorf("wanted token expiry time to be in the future")
	}
}

func TestAddPair(t *testing.T) {
	auth := NewAuth()
	username := "user"
	token := "test"
	expiry := time.Now().Add(time.Hour)
	auth.AddPair(username, token, expiry)

	storedData, ok := auth.tokens.Load(token)
	if !ok {
		t.Errorf("expected token stored")
	}

	data := storedData.(nameAndExp)
	if data.name != username {
		t.Errorf("wanted username to be %s, but got %s", username, data.name)
	}
	if data.expiry != expiry {
		t.Errorf("wanted expiry time to be %v, but got %v", expiry, data.expiry)
	}
}

func TestIsTokenValid(t *testing.T) {
	auth := NewAuth()
	username := "user"
	token := auth.AddToken(username)

	name, isValid := auth.IsTokenValid(token)
	if !isValid {
		t.Errorf("wanted token to be valid")
	}
	if name != username {
		t.Errorf("wanted username to be %s, but got %s", username, name)
	}

	expiredToken := "expired"
	expiry := time.Now().Add(-time.Hour)
	auth.AddPair(username, expiredToken, expiry)

	_, isValid = auth.IsTokenValid(expiredToken)
	if isValid {
		t.Error("wanted token to be invalid due to expiration")
	}

	_, isValid = auth.IsTokenValid("doesntexist")
	if isValid {
		t.Error("wanted non-existent token to be invalid")
	}
}

func TestDeleteToken(t *testing.T) {
	auth := NewAuth()
	username := "user"
	token := auth.AddToken(username)

	isDeleted := auth.DeleteToken(token)
	if !isDeleted {
		t.Error("wanted to delete token")
	}

	isDeleted = auth.DeleteToken(token)
	if isDeleted {
		t.Error("token should not exist")
	}

	expiredToken := "expired"
	expiry := time.Now().Add(-time.Hour)
	auth.AddPair(username, expiredToken, expiry)
	isDeleted = auth.DeleteToken(expiredToken)
	if isDeleted {
		t.Error("expected false output")
	}
}
