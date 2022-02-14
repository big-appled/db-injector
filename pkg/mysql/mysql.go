package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	"k8s.io/klog/v2"
)

type MYSQL struct {
	config dbconfig.Config
	db     *sql.DB
}

func (m *MYSQL) Init(dbConfig dbconfig.Config) error {
	m.config = dbConfig
	dbs := m.config.Databases
	if len(dbs) == 0 {
		err := fmt.Errorf("no database specified")
		klog.Error(err, "")
		return err
	}
	return nil
}

func (m *MYSQL) Connect() error {
	var err error
	klog.Info("mysql init")
	dbs := m.config.Databases
	if len(dbs) == 0 {
		err = fmt.Errorf("no database specified")
		klog.Error(err, "")
		return err
	}
	for _, database := range dbs {
		dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", m.config.Username, m.config.Password, "tcp", m.config.Host, database)
		m.db, err = sql.Open("mysql", dsn)
		if err != nil {
			klog.Error(err, fmt.Sprintf("failed to init connection to mysql database %s, in %s", database, m.config.Host))
			return err
		}
		err = m.db.Ping()
		if err != nil {
			klog.Error(err, fmt.Sprintf("cannot access mysql databases %s in %s", database, m.config.Host))
			return err
		}
		m.db.Close()
	}
	return nil
}

func (m *MYSQL) Inject() error {
	var err error

	err = m.Connect()
	if err != nil {
		return err
	}

	klog.Info("mysql inject in progress...")
	dbs := m.config.Databases
	if len(dbs) == 0 {
		err = fmt.Errorf("no database specified in %s", m.config.Host)
		klog.Error(err, "")
		return err
	}

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", m.config.Username, m.config.Password, "tcp", m.config.Host, dbs[0])
	m.db, err = sql.Open("mysql", dsn)
	if err != nil {
		klog.Error(err, fmt.Sprintf("failed to init connection to mysql database %s, in %s", dbs[0], m.config.Host))
		return err
	}

	err = m.initTable()
	if err != nil {
		klog.Error(err, fmt.Sprintf("failed to init table in mysql database %s, in %s", dbs[0], m.config.Host))
		return err
	}

	var i int64
	totalLoop := m.config.NumLoops
	if totalLoop == 0 {
		totalLoop = int64(^uint64(0) >> 1)
	}
	klog.Info(fmt.Sprintf("total cycle is: %d", totalLoop))
	for i = 0; i < totalLoop; i++ {
		for _, database := range dbs {
			_, err = m.db.Exec(fmt.Sprintf("use %s;", database))
			if err != nil {
				klog.Error(err)
			}
			_, err = m.db.Exec(fmt.Sprintf("INSERT INTO %s () VALUES ();", m.config.TableName))
			if err != nil {
				klog.Error(err)
			}
		}
		timeString := time.Now().Format("2006-01-02 15:04:05")
		klog.Info(fmt.Sprintf("loop %d: %s", i, timeString))
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (m *MYSQL) initTable() error {
	var err error
	for _, database := range m.config.Databases {
		_, err = m.db.Exec(fmt.Sprintf("use %s;", database))
		if err != nil {
			return err
		}
		_, table_check := m.db.Query(fmt.Sprintf("select * from %s;", m.config.TableName))
		if table_check == nil {
			if !m.config.OverWrite {
				klog.Info("continue using exist table")
				return nil
			}
			// delete the old one
			m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", m.config.TableName))
		}

		//create new table
		_, err := m.db.Exec(fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, insert_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP)", m.config.TableName))
		if err != nil {
			return err
		}
	}

	return nil
}
