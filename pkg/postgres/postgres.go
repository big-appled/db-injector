package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	_ "github.com/lib/pq"
	"k8s.io/klog/v2"
)

type PG struct {
	config dbconfig.Config
	db     *sql.DB
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

func (pg *PG) Inject() error {
	var err error
	klog.Info("postgres quiesce in progress...")

	backupName := "test"
	fastStartString := "true"

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

		queryStr := fmt.Sprintf("select pg_start_backup('%s', %s);", backupName, fastStartString)

		result, queryErr := pg.db.Query(queryStr)

		if queryErr != nil {
			if strings.Contains(queryErr.Error(), "backup is already in progress") {
				pg.db.Close()
				continue
			}
			klog.Error(queryErr, "could not start postgres backup")
			return queryErr
		}

		var snapshotLocation string
		result.Next()

		scanErr := result.Scan(&snapshotLocation)
		if scanErr != nil {
			klog.Error(scanErr, "Postgres backup apparently started but could not understand server response")
			return scanErr
		}
		klog.Info(fmt.Sprintf("Successfully reach consistent recovery state at %s", snapshotLocation))
		pg.db.Close()
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
