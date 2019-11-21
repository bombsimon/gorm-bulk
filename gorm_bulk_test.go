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
		description string
		slice       []interface{}
		execFunc    ExecFunc
		scopes      map[string]string
		expectedSQL string
		errContains string
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
		})
	}
}
