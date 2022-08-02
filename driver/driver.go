package driver

import (
	"fmt"
	"strings"

	"github.com/big-appled/db-injector/pkg/dbconfig"
	"github.com/big-appled/db-injector/pkg/mongo"
	"github.com/big-appled/db-injector/pkg/mysql"
	"github.com/big-appled/db-injector/pkg/postgres"
	klog "k8s.io/klog/v2"
)

type SupportedDB string

const (
	MySQL    SupportedDB = "MySQL"
	Postgres SupportedDB = "Postgres"
	MongoDB  SupportedDB = "MongoDB"
)

type Database interface {
	Init(dbconfig.Config) error
	Connect() error
	Inject() error
}

type DriverManager struct {
	db       Database
	dbconfig dbconfig.Config
}

func NewManager(dbconfig *dbconfig.Config) (*DriverManager, error) {
	var CacheManager DriverManager
	var err error

	// init database
	if strings.EqualFold(dbconfig.Provider, string(Postgres)) { // postgres
		CacheManager.db = new(postgres.PG)
	} else if strings.EqualFold(dbconfig.Provider, string(MySQL)) { // mysql
		CacheManager.db = new(mysql.MYSQL)
	} else if strings.EqualFold(dbconfig.Provider, string(MongoDB)) { // mongo
		CacheManager.db = new(mongo.MG)
	} else {
		err = fmt.Errorf("provider type %s is not supported", dbconfig.Provider)
		klog.Error(err)
		return &CacheManager, err
	}

	CacheManager.dbconfig = *dbconfig
	err = CacheManager.db.Init(CacheManager.dbconfig)

	return &CacheManager, err
}

func (d *DriverManager) DBConnect() error {
	return d.db.Connect()
}

func (d *DriverManager) DBInject() error {
	return d.db.Inject()
}
