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

// BulkExecChunk will split the objects passed into the passed chunk size. A
// slice of errors will be returned (if any).
func BulkExecChunk(db *gorm.DB, objects []interface{}, execFunc ExecFunc, chunkSize int) []error {
	var allErrors []error

	for {
		var chunkObjects []interface{}

		if len(objects) <= chunkSize {
			chunkObjects = objects
			objects = []interface{}{}
		} else {
			chunkObjects = objects[:chunkSize]
			objects = objects[chunkSize:]
		}

		if err := BulkExec(db, chunkObjects, execFunc); err != nil {
			allErrors = append(allErrors, err)
		}

		// Nothing more to do
		if len(objects) < 1 {
			break
		}
	}

	if len(allErrors) > 0 {
		return allErrors
	}

	return nil
}

// BulkExec will convert a slice of interface to bulk SQL statement. The final
// SQL will be determined by the ExecFunc passed.
func BulkExec(db *gorm.DB, objects []interface{}, execFunc ExecFunc) error {
	scope, err := scopeFromObjects(db, objects, execFunc)
	if err != nil {
		return err
	}

	// No scope and no error means nothing to do
	if scope == nil {
		return nil
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
		bulkNow           = gorm.NowFunc()
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

		// Add as many placeholders (question marks) as there are columns.
		placeholders = append(placeholders, "?")

		// Sort the column names to ensure the right order.
		sort.Strings(columnNames)
	}

	// We must setup quotedColumnNames after sorting columnNames since sorting
	// of quoted fields might differ from sorting without. This way we know that
	// columnNames is the master of the order and will be used both when setting
	// field and values order.
	for i := range columnNames {
		quotedColumnNames = append(quotedColumnNames, scope.Quote(columnNames[i]))
	}

	for _, r := range objects {
		objectScope := db.NewScope(r)

		row, err := ObjectToMap(r)
		if err != nil {
			return nil, err
		}

		for _, key := range columnNames {
			field := row[key]
			value := field.Field.Interface()

			switch field.Struct.Name {
			// Column CreatedAt and UpdatedAt with zero value will be set to same time
			case "CreatedAt", "UpdatedAt":
				if field.IsBlank {
					value = bulkNow
				}
			}

			objectScope.AddToVars(value)
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
//  * Blank fields with default value - Will be set to the default value
func ObjectToMap(object interface{}) (map[string]*gorm.Field, error) {
	var (
		attributes = map[string]*gorm.Field{}
	)

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

		if field.IsIgnored {
			continue
		}

		// Let the DBM set the default values since these might be meta values
		// such as 'CURRENT_TIMESTAMP'. Has default will be set to true also for
		// 'AUTO_INCREMENT' fields which is not primary keys so we must check
		// that we've ACTUALLY configured a default value and uses the tag
		// before we skip it.
		if field.StructField.HasDefaultValue && field.IsBlank {
			if _, ok := field.TagSettingsGet("DEFAULT"); ok {
				continue
			}
		}

		// Skip blank primary key fields named ID. They're probably coming from
		// `gorm.Model` which doesn't have the AUTO_INCREMENT tag.
		if field.DBName == "id" && field.IsPrimaryKey && field.IsBlank {
			continue
		}

		// Check if auto increment is set (but not set to false). If so skip the
		// field and let the DBM auto increment the value.
		if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
			if !strings.EqualFold(value, "false") {
				continue
			}
		}

		attributes[field.DBName] = field
	}

	return attributes, nil
}
