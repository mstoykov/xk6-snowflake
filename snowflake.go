package snowflake

import (
	"database/sql"
	_ "github.com/snowflakedb/gosnowflake"
	"go.k6.io/k6/js/modules"
)

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &SQL{}
)

type RootModule struct{}
type SQL struct {
	vu modules.VU
}

type keyValue map[string]interface{}

func init() {
	modules.Register("k6/x/snowflake", new(RootModule))
}

func (*SQL) Open(connectionString string) (*sql.DB, error) {
	db, err := sql.Open("snowflake", connectionString)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (*SQL) Query(db *sql.DB, query string, args ...interface{}) ([]keyValue, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	result := make([]keyValue, 0)

	for rows.Next() {
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		data := make(keyValue, len(cols))
		for i, colName := range cols {
			data[colName] = *valuePtrs[i].(*interface{})
		}

		result = append(result, data)
	}

	_err := rows.Close()
	if _err != nil {
		return nil, _err
	}

	return result, nil
}

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &SQL{vu: vu}
}

func (sql *SQL) Exports() modules.Exports {
	return modules.Exports{Default: sql}
}
