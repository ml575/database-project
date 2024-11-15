// This package handles authentication for the database. It can generate, store, and delete tokens with certain time validities.
package auth

import (
	"math/rand"
	"sync"
	"time"
)

// This struct stores the sync map of tokens to their username and expiry time.
type Auth struct {
	tokens sync.Map
}

// This struct stores the name and expiry time.
type nameAndExp struct {
	name   string
	expiry time.Time
}

// This function creates the underlying map for the Auth structure.
func NewAuth() *Auth {
	return &Auth{}
}

// This is a helper function that can generate random tokens of a given length.
func generateRandomToken(length int) string {
	domain := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890.~-_"
	token := make([]byte, length)
	for i := range token {
		token[i] = domain[rand.Intn(len(domain))]
	}
	return string(token)
}

// This function takes in a username and generates a unique, random token for the user (which is returned). It is set to expire in an hour.
func (auth *Auth) AddToken(username string) string {
	tokenLength := 14
	for {
		expiry := time.Now().Add(time.Hour)
		token := generateRandomToken(tokenLength)
		newNameAndExpiry := nameAndExp{
			name:   username,
			expiry: expiry,
		}
		_, ok := auth.tokens.LoadOrStore(token, newNameAndExpiry)
		// keep generating tokens until we find one that is unique
		if !ok {
			return token
		}
	}
}

// This function allows adding tokens with corresponding usernames and times.
// It will overwrite existing pairs with the same token, so ideally use this only at the beginning
// when an Auth struct is created and no tokens exist.
func (auth *Auth) AddPair(username string, token string, time time.Time) {
	auth.tokens.Store(token, nameAndExp{username, time})
}

// This function takes in a token and returns the associated username and whether it is valid or not.
func (auth *Auth) IsTokenValid(token string) (string, bool) {
	data, ok := auth.tokens.Load(token)
	var nameAndExpiry nameAndExp
	if !ok {
		return "", false
	} else {
		nameAndExpiry = data.(nameAndExp)
	}
	if time.Now().After(nameAndExpiry.expiry) {
		return "", false
	}
	return nameAndExpiry.name, true
}

// This function deletes a given token from the token map making it no longer valid. It returns
// a bolean indicating whether this was an authorized delete or not.
func (auth *Auth) DeleteToken(token string) bool {
	data, ok := auth.tokens.LoadAndDelete(token)
	retStatus := false
	if ok {
		nameAndExp := data.(nameAndExp)
		if !time.Now().After(nameAndExp.expiry) {
			retStatus = true
		}
	}
	return retStatus
}
