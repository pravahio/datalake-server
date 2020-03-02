package auth

import (
	"fmt"
	"os"

	"github.com/pravahio/go-auth-provider/store"
)

func Validate(i interface{}) bool {
	cert := os.Getenv("AUTH_CERT_PATH")
	v, err := store.NewValidator(cert)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return v.DecodeAndValidate(i.(string))
}
