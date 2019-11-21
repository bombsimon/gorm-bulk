package gormbulk

import (
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
)

type ExecFunc func(scope *gorm.Scope, columnNames, groups []string)

// InsertFunc is the default insert func. It will pass a gorm.Scope pointer
// which holds all the vars in scope.SQLVars. The value set to scope.SQL
// will be used as SQL and the variables in scope.SQLVars will be used as
// values.
//
//  INSERT INTO `tbl`
//    (col1, col2)
//  VALUES
//    (?, ?), (?, ?)
func InsertFunc(scope *gorm.Scope, columnNames, groups []string) {
	defaultWithFormat(scope, columnNames, groups, "INSERT INTO %s (%s) VALUES %s")
}

// InsertIgnoreFunc will run INSERT IGNORE with all the records and values set
// on the passed scope pointer.
//
//  INSERT IGNORE INTO `tbl`
//    (col1, col2)
//  VALUES
//    (?, ?), (?, ?)
func InsertIgnoreFunc(scope *gorm.Scope, columnNames, groups []string) {
	defaultWithFormat(scope, columnNames, groups, "INSERT IGNORE INTO %s (%s) VALUES %s")
}

// InsertOnDuplicateKeyUpdateFunc will perform a bulk insert but on duplicate key
// perform an update.
//
//  INSERT INTO `tbl`
//    (col1, col2)
//  VALUES
//    (?, ?), (?, ?)
//  ON DUPLICATE KEY UPDATE
//    col1 = VALUES(col1),
//    col2 = VALUES(col2)
func InsertOnDuplicateKeyUpdateFunc(scope *gorm.Scope, columnNames, groups []string) {
	var duplicateUpdates []string

	for i := range columnNames {
		duplicateUpdates = append(
			duplicateUpdates,
			fmt.Sprintf("%s = VALUES(%s)", columnNames[i], columnNames[i]),
		)
	}

	// This is not SQL string formatting, prepare statements is in use.
	// nolint: gosec
	scope.Raw(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		scope.QuotedTableName(),
		strings.Join(columnNames, ", "),
		strings.Join(groups, ", "),
		strings.Join(duplicateUpdates, ", "),
	))
}

func defaultWithFormat(scope *gorm.Scope, columnNames, groups []string, format string) {
	var (
		extraOptions string
		sqlFormat    = fmt.Sprintf("%s%%s", format)
	)

	if insertOption, ok := scope.Get("gorm:insert_option"); ok {
		// Add the extra insert option
		extraOptions = fmt.Sprintf(" %s", insertOption)
	}

	scope.Raw(fmt.Sprintf(
		sqlFormat,
		scope.QuotedTableName(),
		strings.Join(columnNames, ", "),
		strings.Join(groups, ", "),
		extraOptions,
	))
}
