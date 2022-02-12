package dbconfig

type Config struct {
	NumLoops  int64
	Host      string
	Databases []string
	Username  string
	Password  string
	Provider  string
	OverWrite bool
	TableName string
}
