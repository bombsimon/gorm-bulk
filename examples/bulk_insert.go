package examples

//go:generate slice-converter --keyword TYPE-TO-INTERFACE

import (
	"fmt"
	"strings"

	gormbulk "github.com/bombsimon/gorm-bulk"
	"github.com/jinzhu/gorm"
)

// MyType represents some kind of database model. This struct has a tag set to
// support automatic code generation to convert []MyType to []interface{}.
// TYPE-TO-INTERFACE
type MyType struct {
	Field1 string `gorm:"type:varchar(10); unique"`
	Field2 int    `gorm:"type:int(3)"`
}

// UpdateMany takes a slice of multiple MyType and will perform one single
// insert with all the elements as values.
func UpdateMany(db *gorm.DB, myTypes []MyType) error {
	// Call the auto generated function to convert []MyType to []interface{}
	// with the same data. This is required to allow the bulk functions to
	// iterate over the objects.
	myTypesAsInterface := MyTypeSliceToInterfaceSlice(myTypes)

	// Set gorm:insert option to update field1 only. To update all the fields on
	// a collision use `BulkInsertOnDuplicateKeyUpdate` which will construct a
	// complete update set for all fields.
	db = db.Model(&MyType{}).
		Set(
			"gorm:insert_option",
			"ON DUPLICATE KEY UPDATE field1 = VALUES(field1)",
		)

	if err := gormbulk.BulkInsert(db, myTypesAsInterface); err != nil {
		return err
	}

	return nil
}

// UpdateCustomFunc runs a bulk update on all passed MyType and will format the
// SQL according to the myExecFunc changes.
func UpdateCustomFunc(db *gorm.DB, myTypes []MyType) error {
	myTypesAsInterface := MyTypeSliceToInterfaceSlice(myTypes)
	myExecFunc := func(scope *gorm.Scope, columnNames, _ []string) {
		var (
			newVars         []interface{}
			newPlaceholders []string
		)

		for _, mt := range myTypes {
			newVars = append(newVars, mt.Field1)
			newPlaceholders = append(newPlaceholders, fmt.Sprintf("(?)"))
		}

		// This is not SQL string formatting
		// nolint: gosec
		scope.Raw(fmt.Sprintf(
			"INSERT INTO `mytype` (field1) VALUES %s",
			strings.Join(newPlaceholders, ", "),
		))

		scope.SQLVars = newVars
	}

	if err := gormbulk.BulkExec(db, myTypesAsInterface, myExecFunc); err != nil {
		return err
	}

	return nil
}
