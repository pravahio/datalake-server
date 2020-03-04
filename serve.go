package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	logging "github.com/ipfs/go-log"
	"github.com/pravahio/datalake-server/auth"
	"github.com/pravahio/datalake-server/db"
	"github.com/pravahio/datalake-server/utils"
)

var (
	log = logging.Logger("serve")
)

type handlerFunc func(http.ResponseWriter, *http.Request)

var (
	router = map[string]handlerFunc{
		"/get":       handleGet,
		"/aggregate": handleAgg,
	}
	mdb *db.Database
)

func handleGet(w http.ResponseWriter, req *http.Request) {
	log.Info("Handling a /get request")
	raw, err := preCheck(w, req, nil)
	if err != nil {
		w.Write(jsonErrResponse(err.Error()))
		return
	}

	qp := db.CreateQueryParam(raw)

	res, err := mdb.Get(context.Background(), qp)
	if err != nil {
		log.Error(err)
		w.WriteHeader(501)
		w.Write(jsonErrResponse(err.Error()))
		return
	} else {
		log.Info("Served /get request")
		w.Write([]byte(res))
	}

}

func handleAgg(w http.ResponseWriter, req *http.Request) {
	raw, err := preCheck(w, req, []string{"pipeline"})
	if err != nil {
		w.Write(jsonErrResponse(err.Error()))
		return
	}

	qp := db.CreateQueryParam(raw)

	res, err := mdb.Aggregate(context.Background(), qp, raw["pipeline"])
	if err != nil {
		w.Write(jsonErrResponse(err.Error()))
		return
	} else {
		w.Write([]byte(res))
	}
}

func preCheck(w http.ResponseWriter, req *http.Request, requiredKeys []string) (map[string]interface{}, error) {
	rk := []string{"channel", "access_token"}
	rk = append(rk, requiredKeys...)

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	var raw map[string]interface{}
	json.Unmarshal(body, &raw)

	w.Header().Set("Content-Type", "application/json")

	if !utils.AreAllKeysInMap(rk, raw) {
		return nil, errors.New("All keys are not present")
	}

	if !auth.Validate(raw["access_token"]) {
		return nil, errors.New("Validation Failed")
	}

	return raw, nil
}

func jsonErrResponse(s string) []byte {
	return []byte(fmt.Sprintf("{\"error\": \"%s\"}", s))
}

func main() {
	mdbTemp, err := db.NewDatabase()
	if err != nil {
		panic(err)
	}
	mdb = mdbTemp

	setupLogging()

	setHandlers()

	host := os.Getenv("LISTEN_HOST")
	port := os.Getenv("LISTEN_PORT")

	log.Infof("Listening on %s:%s", host, port)
	err = http.ListenAndServe(host+":"+port, nil)
	if err != nil {
		log.Error(err)
	}
}

func setHandlers() {
	for k, v := range router {
		http.HandleFunc(k, v)
	}
}

func setupLogging() {
	logging.SetLogLevel("serve", "DEBUG")
	logging.SetLogLevel("db", "DEBUG")
}
