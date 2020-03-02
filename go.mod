module github.com/pravahio/datalake-server

go 1.12

require (
	github.com/DataDog/zstd v1.4.4 // indirect
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/ipfs/go-log v1.0.2
	github.com/pravahio/go-auth-provider v0.0.0
	github.com/tidwall/pretty v1.0.1 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	go.mongodb.org/mongo-driver v1.2.0
	golang.org/x/crypto v0.0.0-20191227163750-53104e6ec876 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

replace github.com/pravahio/go-auth-provider v0.0.0 => ../../auth/go-auth-provider
