package mongo

import (
	"context"
	"fmt"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/klog/v2"
)

type MG struct {
	config dbconfig.Config
}

func (mg *MG) Init(dbConfig dbconfig.Config) error {
	mg.config = dbConfig
	return nil
}

func (mg *MG) Connect() error {
	var err error
	klog.Info("mongodb connecting...")

	client, err := getMongodbClient(mg.config)
	if err != nil {
		return err
	}
	// list all databases to check the connection is ok
	filter := bson.D{{}}
	_, err = client.ListDatabaseNames(context.TODO(), filter)
	if err != nil {
		klog.Error(err, "failed to list databases")
		return err
	}
	return nil
}

func (mg *MG) Inject() error {
	var err error
	klog.Info("mongodb quiesce in progress")
	client, err := getMongodbClient(mg.config)
	if err != nil {
		return err
	}
	db := client.Database("admin")
	result := db.RunCommand(context.TODO(), bson.D{{Key: "fsync", Value: 1}, {Key: "lock", Value: true}})
	if result.Err() != nil {
		klog.Error(result.Err(), fmt.Sprintf("failed to quiesce %s", mg.config.Host))
		return result.Err()
	}

	return nil
}

func (mg *MG) Unquiesce() error {
	klog.Info("mongodb unquiesce in progress")
	client, err := getMongodbClient(mg.config)
	if err != nil {
		return err
	}
	db := client.Database("admin")
	result := db.RunCommand(context.TODO(), bson.D{{Key: "fsyncUnlock", Value: 1}})
	if result.Err() != nil {
		klog.Error(result.Err(), fmt.Sprintf("failed to unquiesce %s", mg.config.Host))
		return result.Err()
	}

	return nil
}

func getMongodbClient(dbConfig dbconfig.Config) (*mongo.Client, error) {
	host := fmt.Sprintf("mongodb://%s:%s@%s",
		dbConfig.Username,
		dbConfig.Password,
		dbConfig.Host)
	clientOptions := options.Client().ApplyURI(host)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		klog.Error(err, fmt.Sprintf("failed to connect mongodb %s", dbConfig.Host))
		return client, err
	}
	return client, nil
}
