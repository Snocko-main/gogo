package middleware

import (
	"errors"
	"fmt"
)

const minHMACSecretBytes = 32

func validateHMACSecret(component string, secret []byte) error {
	if len(secret) == 0 {
		return errors.New(component + " requires Secret")
	}
	if len(secret) < minHMACSecretBytes {
		return fmt.Errorf("%s Secret must be at least %d bytes", component, minHMACSecretBytes)
	}
	return nil
}
