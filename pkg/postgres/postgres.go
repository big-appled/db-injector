package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	_ "github.com/lib/pq"
	"k8s.io/klog/v2"
)

type PG struct {
	config dbconfig.Config
	db     *sql.DB
	dbList []*sql.DB
}

func (pg *PG) Init(dbConfig dbconfig.Config) error {
	pg.config = dbConfig
	return nil
}

func (pg *PG) Connect() error {
	var err error
	klog.Info("postgres connecting")

	connectionConfigStrings := pg.getConnectionString()
	if len(connectionConfigStrings) == 0 {
		return fmt.Errorf("no database found in %s", pg.config.Host)
	}

	for i := 0; i < len(connectionConfigStrings); i++ {
		pg.db, err = sql.Open("postgres", connectionConfigStrings[i])
		if err != nil {
			klog.Error(err, "cannot connect to postgres")
			return err
		}

		err = pg.db.Ping()
		if err != nil {
			klog.Error(err, fmt.Sprintf("cannot connect to postgres database %s", pg.config.Databases[i]))
			return err
		}
		pg.db.Close()
	}
	return nil
}

func (pg *PG) Disconnect() error {
	for _, pg.db = range pg.dbList {
		pg.db.Close()
	}
	return nil
}

func (pg *PG) Inject() error {
	var err error

	err = pg.Connect()
	if err != nil {
		klog.Error(err)
		return err
	}
	err = pg.initTable()
	if err != nil {
		klog.Error(err)
		return err
	}

	klog.Info("postgres inject in progress...")

	var i int64
	totalLoop := pg.config.NumLoops
	if totalLoop == 0 {
		totalLoop = int64(^uint64(0) >> 1)
	}
	klog.Info(fmt.Sprintf("total cycle is: %d", totalLoop))
	for i = 0; i < totalLoop; i++ {
		for _, pg.db = range pg.dbList {
			_, err = pg.db.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (%d);", pg.config.TableName, i))
			if err != nil {
				klog.Error(err)
			}
			timeString := time.Now().Format("2006-01-02 15:04:05")
			klog.Info(fmt.Sprintf("loop %d: %s", i, timeString))
			time.Sleep(1 * time.Second)
		}
	}
	pg.Disconnect()
	return nil
}

func (pg *PG) initTable() error {
	var err error
	connectionConfigStrings := pg.getConnectionString()
	if len(connectionConfigStrings) == 0 {
		return fmt.Errorf("no database found in %s", pg.config.Host)
	}
	for _, dbconn := range connectionConfigStrings {
		pg.db, err = sql.Open("postgres", dbconn)
		if err != nil {
			klog.Error(err, "cannot connect to postgres")
			return err
		}
		pg.dbList = append(pg.dbList, pg.db)

		queryStr := fmt.Sprintf("select * from %s;", pg.config.TableName)

		_, queryErr := pg.db.Query(queryStr)
		if queryErr == nil { // table exists
			if !pg.config.OverWrite {
				klog.Info("continue using exist table")
				return nil
			}
			// delete the old one
			pg.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", pg.config.TableName))
		}

		//create new table
		_, err := pg.db.Exec(fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL PRIMARY KEY, insert_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP)", pg.config.TableName))
		if err != nil {
			return err
		}
	}

	return nil
}

func (pg *PG) getConnectionString() []string {
	var dbname string
	var connstr []string

	if len(pg.config.Databases) == 0 {
		klog.Error(fmt.Errorf("no database found in %s", pg.config.Host), "")
		return connstr
	}

	for i := 0; i < len(pg.config.Databases); i++ {
		dbname = pg.config.Databases[i]
		connstr = append(connstr, fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", pg.config.Host, pg.config.Username, pg.config.Password, dbname))
	}
	return connstr
}
