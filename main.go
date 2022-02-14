/*
Copyright 2021.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/big-appled/db-injector/driver"
	"github.com/big-appled/db-injector/pkg/dbconfig"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	klog "k8s.io/klog/v2"
)

const (
	DefaultInterval = 250 * time.Millisecond
)

type DemoOptions struct {
	Type      string
	EndPoint  string
	Database  string
	Username  string
	Password  string
	OverWrite bool
	NumLoops  int64
}

func NewCommand(baseName string) (*cobra.Command, error) {

	option := &DemoOptions{}

	c := &cobra.Command{
		Use:   "db-injector",
		Short: "Insert timestamp into database",
		Long:  "Insert timestamp into database",
		Run: func(c *cobra.Command, args []string) {
			option.Run()
		},
	}

	option.BindFlags(c.Flags(), c)

	return c, nil
}

func (d *DemoOptions) BindFlags(flags *pflag.FlagSet, c *cobra.Command) {
	flags.StringVarP(&d.Type, "type", "t", "", "mysql, postgres, mongodb")
	c.MarkFlagRequired("type")
	flags.StringVarP(&d.EndPoint, "endpoint", "e", "", "database endpoint")
	c.MarkFlagRequired("type")
	flags.StringVarP(&d.Database, "database", "d", "", "name of the database, it can be multiple, seperated by comma")
	c.MarkFlagRequired("database")
	flags.StringVarP(&d.Username, "username", "u", "", "username")
	c.MarkFlagRequired("database")
	flags.StringVarP(&d.Password, "password", "p", "", "password")
	c.MarkFlagRequired("database")
	flags.Int64VarP(&d.NumLoops, "count", "c", 0, "number of loops to execute")
	flags.BoolVarP(&d.OverWrite, "overwrite", "o", true, "overwrite old table data")
}

func (d *DemoOptions) newDBConfig() *dbconfig.Config {

	c := &dbconfig.Config{
		Provider:  d.Type,
		Host:      d.EndPoint,
		OverWrite: d.OverWrite,
		NumLoops:  d.NumLoops,
		Username:  d.Username,
		Password:  d.Password,
		Databases: strings.Split(d.Database, ","),
		TableName: "injection_table",
	}

	return c
}

func (d *DemoOptions) Run() error {
	var err error

	dbdriver, err := driver.NewManager(d.newDBConfig())
	if err != nil {
		return err
	}

	err = dbdriver.DBInject()
	if err != nil {
		return err
	}
	klog.Info("database stress done")

	return err
}

func main() {
	defer klog.Flush()

	baseName := filepath.Base(os.Args[0])

	c, _ := NewCommand(baseName)
	c.Execute()
}
