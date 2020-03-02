package db

import (
	"errors"
	"time"

	"github.com/pravahio/datalake-server/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type QueryParam struct {
	Collection  string
	CustomQuery map[string]interface{}
	PastHours   int
	PastMinutes int
	PastSeconds int
	PastDays    int
	StartTime   time.Time
	EndTime     time.Time
}

func CreateQueryParam(raw map[string]interface{}) QueryParam {
	qp := QueryParam{
		Collection: raw["channel"].(string),
	}

	if q, ok := raw["query"]; ok {
		qp.CustomQuery = q.(map[string]interface{})
	}

	if v, ok := raw["past_hours"]; ok {
		qp.PastHours = int(v.(float64))
	}
	if v, ok := raw["past_minutes"]; ok {
		qp.PastMinutes = int(v.(float64))
	}
	if v, ok := raw["past_seconds"]; ok {
		qp.PastSeconds = int(v.(float64))
	}
	if v, ok := raw["past_days"]; ok {
		qp.PastDays = int(v.(float64))
	}

	return qp
}

func (qp *QueryParam) ParseToBSON() (bson.M, error) {
	bsonQuery := bson.M{}
	for k, v := range qp.CustomQuery {
		bsonQuery[k] = v
	}
	/* err := bson.UnmarshalExtJSON([]byte(qp.CustomQuery), false, &bsonQuery)
	if err != nil {
		return nil, err
	} */
	bsonQuery["_id"] = bson.M{
		"$gte": qp.getObject(),
	}

	return bsonQuery, nil
}

func (qp *QueryParam) getObject() primitive.ObjectID {
	t := time.Now().Add(
		time.Duration(-qp.PastHours)*time.Hour +
			time.Duration(-qp.PastMinutes)*time.Minute +
			time.Duration(-qp.PastSeconds)*time.Second +
			time.Duration(-qp.PastDays*24)*time.Hour)

	return primitive.NewObjectIDFromTimestamp(t)
}

// GetCollectionName return name of the collection to fetch data from.
func (qp *QueryParam) GetCollectionName() (string, error) {
	if qp.Collection == "" {
		return "", errors.New("No collection name")
	}
	return utils.GetCollectionFromChannel(qp.Collection), nil
}
