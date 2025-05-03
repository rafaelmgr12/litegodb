package sqlparser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rafaelmgr12/litegodb/internal/interfaces"
	"github.com/rafaelmgr12/litegodb/internal/session"
	"github.com/xwb1989/sqlparser"
)

func ParseAndExecute(query string, db interfaces.DB, session *session.Session) (interface{}, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") {
		return handleTransaction(query, db, session)
	}

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return handleInsert(stmt, db, session)
	case *sqlparser.Select:
		return handleSelect(stmt, db)
	case *sqlparser.Delete:
		return handleDelete(stmt, db, session)
	case *sqlparser.Update:
		return handleUpdate(stmt, db, session)
	default:
		return nil, fmt.Errorf("unsupported SQL statement")
	}
}

func handleInsert(stmt *sqlparser.Insert, db interfaces.DB, session *session.Session) (interface{}, error) {
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

		k, err := strconv.Atoi(string(keyExpr.Val))
		if err != nil {
			return nil, fmt.Errorf("invalid key value")
		}
		key = k
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

	if session != nil && session.Transaction != nil {
		session.Transaction.PutBatch(table, key, value)
	} else {
		if err := db.Put(table, key, value); err != nil {
			return nil, fmt.Errorf("failed to put value: %w", err)
		}
	}

	return "inserted", nil
}

func handleSelect(stmt *sqlparser.Select, db interfaces.DB) (interface{}, error) {
	table := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
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

func handleDelete(stmt *sqlparser.Delete, db interfaces.DB, session *session.Session) (interface{}, error) {
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

	if session != nil && session.Transaction != nil {
		session.Transaction.DeleteBatch(table, key)
	} else {
		if err := db.Delete(table, key); err != nil {
			return nil, fmt.Errorf("failed to delete key: %w", err)
		}
	}

	return "deleted", nil
}

func handleTransaction(query string, db interfaces.DB, session *session.Session) (any, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))

	switch {
	case strings.HasPrefix(upper, "BEGIN"):
		if session.Transaction != nil {
			return nil, fmt.Errorf("a transaction is already active")
		}
		session.Transaction = db.BeginTransaction()
		return "transaction started", nil

	case strings.HasPrefix(upper, "COMMIT"):
		if session.Transaction == nil {
			return nil, fmt.Errorf("no active transaction to commit")
		}
		err := session.Transaction.Commit()
		session.Transaction = nil
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return "transaction committed", nil

	case strings.HasPrefix(upper, "ROLLBACK"):
		if session.Transaction == nil {
			return nil, fmt.Errorf("no active transaction to rollback")
		}
		session.Transaction.Rollback()
		session.Transaction = nil
		return "transaction rolled back", nil

	default:
		return nil, fmt.Errorf("unsupported transaction statement: %s", query)
	}
}

func handleUpdate(stmt *sqlparser.Update, db interfaces.DB, session *session.Session) (interface{}, error) {
	table := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	var key int
	foundKey := false

	if stmt.Where != nil {
		compExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported WHERE clause")
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

	var newValue string
	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()
		if strings.ToLower(colName) != "value" {
			return nil, fmt.Errorf("only updating 'value' column is supported")
		}

		valExpr, ok := expr.Expr.(*sqlparser.SQLVal)
		if !ok {
			return nil, fmt.Errorf("invalid value expression")
		}

		newValue = string(valExpr.Val)
	}

	if session != nil && session.Transaction != nil {
		session.Transaction.PutBatch(table, key, newValue)
	} else {
		if err := db.Put(table, key, newValue); err != nil {
			return nil, fmt.Errorf("failed to update value: %w", err)
		}
	}

	return "updated", nil
}
