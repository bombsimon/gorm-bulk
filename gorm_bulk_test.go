package gormbulk

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_scopeFromObjects(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)

	gdb, err := gorm.Open("mysql", db)
	require.NoError(t, err)

	type test struct {
		Foo string
		Bar string
	}

	cases := []struct {
		description     string
		slice           []interface{}
		execFunc        ExecFunc
		scopes          map[string]string
		expectedSQL     string
		expectedSQLVars []interface{}
		errContains     string
	}{
		{
			description: "scope returned ok",
			slice: []interface{}{
				test{"one", "two"},
			},
			execFunc: func(scope *gorm.Scope, _, _ []string) {
				scope.Raw("INSERT INTO")
				scope.SQLVars = []interface{}{}
			},
			expectedSQL: "INSERT INTO",
		},
		{
			description: "scope returned ok with existing execFunc",
			slice: []interface{}{
				test{"one", "two"},
			},
			execFunc:    InsertFunc,
			scopes:      map[string]string{"gorm:insert_option": "ON DUPLICATE KEY UPDATE foo = VALUES(foo)"},
			expectedSQL: "INSERT INTO `tests` (`bar`, `foo`) VALUES (?, ?) ON DUPLICATE KEY UPDATE foo = VALUES(foo)",
		},
		{
			description: "test primary keys",
			slice: []interface{}{
				struct {
					ID  int `gorm:"primary_key"` // Should be skipped
					Foo string
				}{
					ID:  0,
					Foo: "foo",
				},
			},
			execFunc:    InsertFunc,
			expectedSQL: "INSERT INTO `` (`foo`) VALUES (?)",
		},
		{
			description: "test auto increment",
			slice: []interface{}{
				struct {
					ID  int `gorm:"auto_increment"` // Should be skipped
					Foo string
				}{
					ID:  0,
					Foo: "foo",
				},
			},
			execFunc:    InsertFunc,
			expectedSQL: "INSERT INTO `` (`foo`) VALUES (?)",
		},
		{
			description: "test setting default value",
			slice: []interface{}{
				struct {
					ID  int    `gorm:"auto_increment"` // Should be skipped
					Foo string `gorm:"default:'foobar'"`
					Bar string
				}{
					ID:  0,
					Foo: "",
					Bar: "barbar",
				},
			},
			execFunc:        InsertFunc,
			expectedSQL:     "INSERT INTO `` (`bar`, `foo`) VALUES (?, ?)",
			expectedSQLVars: []interface{}{"barbar", "foobar"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			db := gdb
			for k, v := range tc.scopes {
				db = gdb.Set(k, v)
			}

			scope, err := scopeFromObjects(db, tc.slice, tc.execFunc)

			if tc.errContains != "" {
				require.Nil(t, scope)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)

				return
			}

			require.NotNil(t, scope)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedSQL, scope.SQL)

			if tc.expectedSQLVars != nil {
				assert.Equal(t, tc.expectedSQLVars, scope.SQLVars)
			}
		})
	}
}

func TestBulkExecChunk(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	gdb, err := gorm.Open("mysql", db)
	require.NoError(t, err)

	type test struct {
		ID  int `gorm:"primary_key"`
		Foo string
		Bar string
	}

	cases := []struct {
		description      string
		execFunc         ExecFunc
		slices           []interface{}
		chunkSize        int
		expectedMockFunc func(mock sqlmock.Sqlmock)
	}{
		{
			description: "six rows in chunks of 3 - will be two calls with 6 args",
			execFunc:    InsertFunc,
			slices: []interface{}{
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
			},
			chunkSize: 3,
			expectedMockFunc: func(mock sqlmock.Sqlmock) {
				// We expect two insert statements
				mock.ExpectExec("INSERT INTO `tests`").
					WithArgs("two", "one", "two", "one", "two", "one").
					WillReturnResult(sqlmock.NewResult(0, 0))

				mock.ExpectExec("INSERT INTO `tests`").
					WithArgs("two", "one", "two", "one", "two", "one").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			description: "uneven row count, chunk size of 3, calls with different arg count",
			execFunc:    InsertFunc,
			slices: []interface{}{
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
				test{Foo: "one", Bar: "two"},
			},
			chunkSize: 3,
			expectedMockFunc: func(mock sqlmock.Sqlmock) {
				// We expect two insert statements
				mock.ExpectExec("INSERT INTO `tests`").
					WithArgs("two", "one", "two", "one", "two", "one").
					WillReturnResult(sqlmock.NewResult(0, 0))

				mock.ExpectExec("INSERT INTO `tests`").
					WithArgs("two", "one", "two", "one", "two", "one").
					WillReturnResult(sqlmock.NewResult(0, 0))

				mock.ExpectExec("INSERT INTO `tests`").
					WithArgs("two", "one").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			tc.expectedMockFunc(mock)

			err := BulkExecChunk(gdb, tc.slices, tc.execFunc, tc.chunkSize)

			require.Nil(t, err)
		})
	}
}
