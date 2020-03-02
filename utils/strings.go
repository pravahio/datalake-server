package utils

import (
	"strings"
)

func GetCollectionFromChannel(channel string) string {
	s := strings.Replace(channel, "/", "", 1)
	return strings.ToLower(s)
}

func AreAllKeysInMap(keys []string, m map[string]interface{}) bool {
	for _, y := range keys {
		if _, ok := m[y]; !ok {
			return false
		}
	}
	return true
}
