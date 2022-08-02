package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/klog/v2"
)

type MG struct {
	mgclient *mongo.Client
	config   dbconfig.Config
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
	}
	mg.mgclient = client
	return err
}

func (mg *MG) Disconnect() error {
	err := mg.mgclient.Disconnect(context.TODO())
	return err
}

func (mg *MG) Inject() error {
	var err error
	defer mg.Disconnect()

	err = mg.Connect()
	if err != nil {
		klog.Error(err)
		return err
	}
	klog.Info("mongodb injection in progress")

	err = mg.initTable()
	if err != nil {
		klog.Error(err)
		return err
	}

	var i int64
	totalLoop := mg.config.NumLoops
	if totalLoop == 0 {
		totalLoop = int64(^uint64(0) >> 1)
	}
	klog.Info(fmt.Sprintf("total cycle is: %d", totalLoop))
	for i = 0; i < totalLoop; i++ {
		timeString := time.Now().Format("2006-01-02 15:04:05")
		oneDoc := MongoFields{
			Loop:      i,
			Timestamp: timeString,
		}
		for _, db := range mg.config.Databases {
			err = mg.insertDB(db, oneDoc)
			if err != nil {
				return err
			}
		}

		klog.Info(fmt.Sprintf("loop %d: %s", i, timeString))
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (mg *MG) initTable() error {
	ctx := context.TODO()
	for _, dbname := range mg.config.Databases {
		filter := bson.M{"name": dbname}
		dbList, err := mg.mgclient.ListDatabaseNames(ctx, filter)
		if err != nil {
			return err
		}
		if len(dbList) > 0 {
			db := mg.mgclient.Database(dbname)
			filter = bson.M{"name": mg.config.TableName}
			tables, err := db.ListCollectionNames(ctx, filter)
			if err != nil {
				return err
			}
			if len(tables) > 0 { // table exists
				if !mg.config.OverWrite {
					klog.Info("continue using exist table")
					return nil
				} else {
					// remove old table
					c := db.Collection(mg.config.TableName)
					err = c.Drop(ctx)
					return err
				}
			}
			//create new table/collection
			err = db.CreateCollection(ctx, mg.config.TableName)
			return err
		} else {
			return fmt.Errorf("failed to find database %s", dbname)
		}
	}
	return nil
}

type MongoFields struct {
	Loop      int64
	Timestamp string
}

func (mg *MG) insertDB(dbname string, oneDoc MongoFields) error {
	ctx := context.TODO()
	db := mg.mgclient.Database(dbname)
	c := db.Collection(mg.config.TableName)
	_, err := c.InsertOne(ctx, oneDoc)
	return err
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
