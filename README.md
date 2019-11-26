# gorm-bulk

[![Build Status](https://travis-ci.com/bombsimon/gorm-bulk.svg?branch=master)](https://travis-ci.com/bombsimon/gorm-bulk)
[![Coverage Status](https://coveralls.io/repos/github/bombsimon/gorm-bulk/badge.svg?branch=master)](https://coveralls.io/github/bombsimon/gorm-bulk?branch=master)
[![Documentation](https://godoc.org/github.com/bombsimon/gorm-bulk?status.svg)](http://godoc.org/github.com/bombsimon/gorm-bulk)

Perform regular Gorm tasks - in bulk!

This project aims to support the missing feature from the famous ORM
[Gorm](https://gorm.io/). The feature I'm talking about is bulk support which
has been on the wish list since 2014; [see
here](https://github.com/jinzhu/gorm/issues/255).

This is inspired by
[t-tiger/gorm-bulk-insert](https://github.com/t-tiger/gorm-bulk-insert) which in
turn is inspired by [this
comment](https://github.com/jinzhu/gorm/issues/255#issuecomment-481159929) but
with the focus on flexibility and letting the end user handle final SQL, final
values, how many to bulk at once etcetera.

## Project status

This project is in it's early phase and since I want to ensure that the end user
interface ends up as smoot, simple and flexible as possible I won't create a
v1.0 release tag until I feel the most important things are in to place.

This doesn't mean that things aren't workign as indented, just that the API
might change without notice.

## Installation

```sh
go get -u github.com/bombsimon/gorm-bulk/...
```

## Usage

### Generate slice conversion

To be able to iterate over any type you have to pass an interface slice
(`[]interface{}`). To make this easier this package is bundled with a code
generator that will generate functions to convert `[]*<T>` and `[]<T>` to
`[]interface{}`.

See [exmaples](examples) for details about how to use `go generate` and what the
[result](examples/types_to_if.gen.go) will look like.

### Bulk actions

This package ships with a few standard bulk action methods. A bulk action uses
an `ExecFunc` which takes a `*gorm.Scope` (holding the table name, all the
values and where you may set the SQL), a slice of all column names and a slice
of all placeholder groups (a set of prepared statements for each slice element).

* `InsertFunc` - Regular `INSERT INTO` with all passed values.
* `InsertIgnoreFunc` - Run `INSERT IGNORE INTO` with all passed values which
   will just discard duplicates (and any other error).
* `InsertOnDuplicateKeyUpdateFunc` - Run `INSERT INTO ... VALUES(...) ON
   DUPLICATE KEY UPDATE x = VALUES(x)`.

Notice that `InsertFunc` and `InsertIgnoreFunc` will look at
`gorm:insert_option` to fetch any user defined additions.

These three `ExecFunc`s are wrapped in `BulkInsert`, `BulkInsertIgnore` and
`BulkInsertOnDuplicateKeyUpdate` so you only have to pass your `*gorm.DB` and
interface slice.

```go
func Example(db *gorm.DB, myTypes []MyType) error {
    myTypesAsInterface := MyTypeSliceToInterfaceSlice(myTypes)

    if err := gormbulk.BulkInsert(db, myTypesAsInterface); err != nil {
        return err
    }

    return nil
}
```

#### Creating your own action

To create your own action where you may return whatever SQL and values you want just implement an `ExecFunc`. This is how a simple `INSERT INTO` would be defined.

```go
func MyCustomBulkFunc(scope *gorm.Scope, columnNames, placeholders []string) {
    scope.Raw(fmt.Sprintf(
        "INSERT INTO %s (%s) VALUES %s",
        scope.QuotedTableName(),
        strings.Join(columnNames, ", "), 
        strings.Join(placeholders, ", "), 
    ))
}
```

### Using the bulk

If you just want to perform a simple bulk insert, use one of the pre implemented
wrapper bulk functions and pass your `*gorm.DB` and data set, [see the
example](examples/bulk_insert.go).
