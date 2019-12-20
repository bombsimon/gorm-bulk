package gormbulk

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Exec(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)

	gdb, err := gorm.Open("mysql", db)
	require.NoError(t, err)

	type test struct {
		Foo string
		Bar string
	}

	cases := []struct {
		description  string
		execFunc     ExecFunc
		columns      []string
		placeholders []string
		scopes       map[string]string
		expectedSQL  string
	}{
		{
			description:  "correct insert without scopes",
			execFunc:     InsertFunc,
			columns:      []string{"foo", "bar"},
			placeholders: []string{"(?, ?)", "(?, ?)"},
			expectedSQL:  "INSERT INTO `tests` (foo, bar) VALUES (?, ?), (?, ?)",
		},
		{
			description:  "adds correct update",
			execFunc:     InsertFunc,
			columns:      []string{"foo", "bar"},
			placeholders: []string{"(?, ?)", "(?, ?)"},
			scopes:       map[string]string{"gorm:insert_option": "ON DUPLICATE KEY UPDATE foo = VALUES(foo)"},
			expectedSQL:  "INSERT INTO `tests` (foo, bar) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE foo = VALUES(foo)",
		},
		{
			description:  "correct insert adds all updates",
			execFunc:     InsertOnDuplicateKeyUpdateFunc,
			columns:      []string{"foo", "bar"},
			placeholders: []string{"(?, ?)", "(?, ?)"},
			expectedSQL:  "INSERT INTO `tests` (foo, bar) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE foo = VALUES(foo), bar = VALUES(bar)",
		},
		{
			description:  "on duplicate key does not update created_at",
			execFunc:     InsertOnDuplicateKeyUpdateFunc,
			columns:      []string{"`created_at`", "`foo`"},
			placeholders: []string{"(?, ?)", "(?, ?)"},
			expectedSQL:  "INSERT INTO `tests` (`created_at`, `foo`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `foo` = VALUES(`foo`)",
		},
		{
			description:  "correct insert ignore",
			execFunc:     InsertIgnoreFunc,
			columns:      []string{"foo", "bar"},
			placeholders: []string{"(?, ?)", "(?, ?)"},
			expectedSQL:  "INSERT IGNORE INTO `tests` (foo, bar) VALUES (?, ?), (?, ?)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			scope := gdb.NewScope(test{})

			for k, v := range tc.scopes {
				scope.Set(k, v)
			}

			tc.execFunc(scope, tc.columns, tc.placeholders)

			assert.Equal(t, tc.expectedSQL, scope.SQL)
		})
	}
}
