package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	logging "github.com/ipfs/go-log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	log       = logging.Logger("db")
	mongoPath = os.Getenv("PRAVAH_DB_HOST")
)

const (
	dbName = "datalake"
)

type Database struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewDatabase creates a new DB instance
func NewDatabase() (*Database, error) {
	username := os.Getenv("PRAVAH_DB_USERNAME")
	password := os.Getenv("PRAVAH_DB_PASSWORD")

	c, err := mongo.NewClient(options.Client().SetAuth(options.Credential{
		Username:   username,
		Password:   password,
		AuthSource: dbName,
	}).ApplyURI(mongoPath))
	if err != nil {
		return nil, err
	}

	err = c.Connect(context.Background())
	if err != nil {
		panic(err)
	}

	db := c.Database(dbName)

	return &Database{
		client:   c,
		database: db,
	}, nil
}

func (db *Database) Get(ctx context.Context, query QueryParam) (string, error) {

	q, err := query.ParseToBSON()
	if err != nil {
		return "", err
	}

	colName, err := query.GetCollectionName()
	if err != nil {
		return "", err
	}
	collection := db.database.Collection(colName)

	cur, err := collection.Find(ctx, q)
	if err != nil {
		return "", err
	}

	defer cur.Close(ctx)

	var joint []string
	for cur.Next(ctx) {
		var decodedData bson.M
		err = cur.Decode(&decodedData)
		if err != nil {
			return "", err
		}

		c, err := bson.MarshalExtJSON(decodedData, false, true)
		if err != nil {
			return "", err
		}
		joint = append(joint, string(c))
	}

	return "[" + strings.Join(joint, ",") + "]", nil
}

func (db *Database) Latest(ctx context.Context, query QueryParam) (string, error) {

	colName, err := query.GetCollectionName()
	if err != nil {
		return "", err
	}
	collection := db.database.Collection(colName)

	cnt, ok := query.CustomQuery["count"]
	if ok {
		res, err := db.latestMoreThanOne(ctx, collection, int64(cnt.(float64)))
		if err != nil {
			return "", err
		}
		return res, nil
	}

	var decodedData bson.M
	opts := options.FindOne().SetSort(bson.M{"$natural": -1})
	err = collection.FindOne(ctx, bson.M{}, opts).Decode(&decodedData)
	if err != nil {
		return "", err
	}

	c, err := bson.MarshalExtJSON(decodedData, false, true)
	if err != nil {
		return "", err
	}

	return string(c), nil
}

func (db *Database) latestMoreThanOne(ctx context.Context, collection *mongo.Collection, cnt int64) (string, error) {
	opts := options.Find()
	opts.SetSort(bson.M{"$natural": -1})
	opts.SetLimit(cnt)
	cur, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		return "", err
	}

	defer cur.Close(ctx)

	var joint []string
	for cur.Next(ctx) {
		var decodedData bson.M
		err = cur.Decode(&decodedData)
		if err != nil {
			return "", err
		}

		c, err := bson.MarshalExtJSON(decodedData, false, true)
		if err != nil {
			return "", err
		}
		joint = append(joint, string(c))
	}

	return "[" + strings.Join(joint, ",") + "]", nil
}

func (db *Database) Aggregate(ctx context.Context, query QueryParam, pipeline interface{}) (string, error) {

	var bdoc []bson.D
	p, err := json.Marshal(pipeline)
	if err != nil {
		log.Info(err)
		return "", err
	}
	log.Info(string(p))
	err = bson.UnmarshalExtJSON(p, false, &bdoc)
	if err != nil {
		log.Info(err)
		return "", err
	}

	colName, err := query.GetCollectionName()
	if err != nil {
		log.Info(err)
		return "", err
	}
	collection := db.database.Collection(colName)

	fmt.Println(bdoc)
	cur, err := collection.Aggregate(ctx, mongo.Pipeline(bdoc))
	if err != nil {
		log.Info(err)
		return "", err
	}

	defer cur.Close(ctx)

	var joint []string
	for cur.Next(ctx) {
		var decodedData bson.M
		err = cur.Decode(&decodedData)
		if err != nil {
			return "", err
		}

		c, err := bson.MarshalExtJSON(decodedData, false, true)
		if err != nil {
			return "", err
		}
		joint = append(joint, string(c))
	}

	return "[" + strings.Join(joint, ",") + "]", nil
}
