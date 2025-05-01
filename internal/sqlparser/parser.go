package sqlparser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rafaelmgr12/litegodb/pkg/litegodb"
	"github.com/xwb1989/sqlparser"
)

func ParseAndExecute(query string, db litegodb.DB) (interface{}, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return handleInsert(stmt, db)
	case *sqlparser.Select:
		return handleSelect(stmt, db)
	case *sqlparser.Delete:
		return handleDelete(stmt, db)
	default:
		return nil, fmt.Errorf("unsupported SQL statement")
	}
}

func handleInsert(stmt *sqlparser.Insert, db litegodb.DB) (interface{}, error) {
	table := stmt.Table.Name.String()
	rows := stmt.Rows.(sqlparser.Values)

	if len(rows) != 1 {
		return nil, fmt.Errorf("only single row insert is supported")
	}

	vals := rows[0]

	var key int
	var value string

	if len(stmt.Columns) == 0 {
		if len(vals) != 2 {
			return nil, fmt.Errorf("expected 2 values (key, value)")
		}
		keyExpr, ok1 := vals[0].(*sqlparser.SQLVal)
		valExpr, ok2 := vals[1].(*sqlparser.SQLVal)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("invalid values")
		}

		keyParsed, err := strconv.Atoi(string(keyExpr.Val))
		if err != nil {
			return nil, fmt.Errorf("invalid key value")
		}
		key = keyParsed
		value = string(valExpr.Val)
	} else {
		for i, col := range stmt.Columns {
			switch col.String() {
			case "key":
				keyExpr, ok := vals[i].(*sqlparser.SQLVal)
				if !ok {
					return nil, fmt.Errorf("invalid key expression")
				}
				k, err := strconv.Atoi(string(keyExpr.Val))
				if err != nil {
					return nil, fmt.Errorf("invalid key value")
				}
				key = k
			case "value":
				valExpr, ok := vals[i].(*sqlparser.SQLVal)
				if !ok {
					return nil, fmt.Errorf("invalid value expression")
				}
				value = string(valExpr.Val)
			default:
				return nil, fmt.Errorf("unsupported column: %s", col.String())
			}
		}
	}

	if err := db.Put(table, key, value); err != nil {
		return nil, fmt.Errorf("failed to put value: %w", err)
	}

	return "inserted", nil
}

func handleSelect(stmt *sqlparser.Select, db litegodb.DB) (interface{}, error) {
	table := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	var key int
	foundKey := false

	// Parse WHERE key = X
	if stmt.Where != nil {
		compExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported where clause")
		}

		leftCol := compExpr.Left.(*sqlparser.ColName).Name.String()
		rightVal := compExpr.Right.(*sqlparser.SQLVal)

		if strings.ToLower(leftCol) != "key" {
			return nil, fmt.Errorf("only WHERE key = ... supported")
		}

		k, err := strconv.Atoi(string(rightVal.Val))
		if err != nil {
			return nil, fmt.Errorf("invalid key value")
		}

		key = k
		foundKey = true
	}

	if !foundKey {
		return nil, fmt.Errorf("WHERE clause with key is required")
	}

	value, found, err := db.Get(table, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("key not found")
	}

	return map[string]interface{}{
		"key":   key,
		"value": value,
	}, nil
}

func handleDelete(stmt *sqlparser.Delete, db litegodb.DB) (interface{}, error) {
	table := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	var key int
	foundKey := false

	if stmt.Where != nil {
		compExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported where clause")
		}

		leftCol := compExpr.Left.(*sqlparser.ColName).Name.String()
		rightVal := compExpr.Right.(*sqlparser.SQLVal)

		if strings.ToLower(leftCol) != "key" {
			return nil, fmt.Errorf("only WHERE key = ... supported")
		}

		k, err := strconv.Atoi(string(rightVal.Val))
		if err != nil {
			return nil, fmt.Errorf("invalid key value")
		}

		key = k
		foundKey = true
	}

	if !foundKey {
		return nil, fmt.Errorf("WHERE clause with key is required")
	}

	err := db.Delete(table, key)
	if err != nil {
		return nil, fmt.Errorf("failed to delete key: %w", err)
	}

	return "deleted", nil
}
