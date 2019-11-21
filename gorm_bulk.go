package gormbulk

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jinzhu/gorm"
)

// BulkInsert will call BulkExec with the default InsertFunc.
func BulkInsert(db *gorm.DB, objects []interface{}) error {
	return BulkExec(db, objects, InsertFunc)
}

// BulkInsertIgnore will call BulkExec with the default InsertFunc.
func BulkInsertIgnore(db *gorm.DB, objects []interface{}) error {
	return BulkExec(db, objects, InsertIgnoreFunc)
}

// BulkInsertOnDuplicateKeyUpdate will call BulkExec with the default InsertFunc.
func BulkInsertOnDuplicateKeyUpdate(db *gorm.DB, objects []interface{}) error {
	return BulkExec(db, objects, InsertOnDuplicateKeyUpdateFunc)
}

// BulkExec will convert a slice of interface to bulk SQL statement. The final
// SQL will be determined by the ExecFunc passed.
func BulkExec(db *gorm.DB, objects []interface{}, execFunc ExecFunc) error {
	scope, err := scopeFromObjects(db, objects, execFunc)
	if err != nil {
		return err
	}

	return db.Exec(scope.SQL, scope.SQLVars...).Error
}

func scopeFromObjects(db *gorm.DB, objects []interface{}, execFunc ExecFunc) (*gorm.Scope, error) {
	// No objects passed, nothing to do.
	if len(objects) < 1 {
		return nil, nil
	}

	var (
		columnNames       []string
		quotedColumnNames []string
		placeholders      []string
		groups            []string
		scope             = db.NewScope(objects[0])
	)

	// Get a map of the first element to calculate field names and number of
	// placeholders.
	firstObjectFields, err := ObjectToMap(objects[0])
	if err != nil {
		return nil, err
	}

	for k := range firstObjectFields {
		// Add raw column names to use for iteration over each row later to get
		// the correct order of columns.
		columnNames = append(columnNames, k)

		// Add to the quoted slice to use for the final SQL to avoid errors and
		// injections.
		quotedColumnNames = append(quotedColumnNames, scope.Quote(gorm.ToColumnName(k)))

		// Add as many placeholders (question marks) as there are columns.
		placeholders = append(placeholders, "?")

		// Sort the column names to ensure the right order.
		sort.Strings(columnNames)
		sort.Strings(quotedColumnNames)
	}

	for _, r := range objects {
		objectScope := db.NewScope(r)

		row, err := ObjectToMap(r)
		if err != nil {
			return nil, err
		}

		for _, key := range columnNames {
			objectScope.AddToVars(row[key])
		}

		groups = append(
			groups,
			fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")),
		)

		// Add object vars to the outer scope vars
		scope.SQLVars = append(scope.SQLVars, objectScope.SQLVars...)
	}

	execFunc(scope, quotedColumnNames, groups)

	return scope, nil
}

// ObjectToMap takes any object of type <T> and returns a map with the gorm
// field DB name as key and the value as value. Special fields and actions
//  * Foreign keys - Will be left out
//  * Relationship fields - Will be left out
//  * Fields marked to be ignored - Will be left out
//  * Fields named ID with auto increment - Will be left out
//  * Fields named ID set as primary key with blank value - Will be left out
//  * Fields named CreatedAt or UpdatedAt - Will be set to gorm.NowFunc() value
//  * Blank fields with default value - Will be set to the default value
func ObjectToMap(object interface{}) (map[string]interface{}, error) {
	var attributes = map[string]interface{}{}

	// De-reference pointers (and it's values)
	rv := reflect.ValueOf(object)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
		object = rv.Interface()
	}

	if rv.Kind() != reflect.Struct {
		return nil, errors.New("value must be kind of Struct")
	}

	for _, field := range (&gorm.Scope{Value: object}).Fields() {
		// Exclude relational record because it's not directly contained in database columns
		_, hasForeignKey := field.TagSettingsGet("FOREIGNKEY")
		if hasForeignKey {
			continue
		}

		if field.StructField.Relationship != nil {
			continue
		}

		// Skip ignored fields.
		if field.IsIgnored {
			continue
		}

		// Skip ID fields which is primary key/auto increment.
		if field.DBName == "id" {
			// Check if auto increment is set (but not set to false)
			if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
				if !strings.EqualFold(value, "false") {
					continue
				}
			}

			// Primary keys will be auto incremented and populated automatically
			// by the DBM so if they're blank (have their default value), skip
			// them.
			if field.IsPrimaryKey && field.IsBlank {
				continue
			}
		}

		if field.Struct.Name == "CreatedAt" || field.Struct.Name == "UpdatedAt" {
			attributes[field.DBName] = gorm.NowFunc()
			continue
		}

		// Set the default value for blank fields.
		if field.StructField.HasDefaultValue && field.IsBlank {
			if val, ok := field.TagSettingsGet("DEFAULT"); ok {
				attributes[field.DBName] = strings.Trim(val, "'")
				continue
			}
		}

		attributes[field.DBName] = field.Field.Interface()
	}

	return attributes, nil
}
