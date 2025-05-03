package interfaces

import "github.com/rafaelmgr12/litegodb/pkg/litegodb"

type AdapterDB struct {
	Impl litegodb.DB
}

func (a *AdapterDB) Put(table string, key int, value string) error {
	return a.Impl.Put(table, key, value)
}

func (a *AdapterDB) Get(table string, key int) (string, bool, error) {
	return a.Impl.Get(table, key)
}

func (a *AdapterDB) Delete(table string, key int) error {
	return a.Impl.Delete(table, key)
}

func (a *AdapterDB) Flush(table string) error {
	return a.Impl.Flush(table)
}

func (a *AdapterDB) CreateTable(table string, degree int) error {
	return a.Impl.CreateTable(table, degree)
}

func (a *AdapterDB) DropTable(table string) error {
	return a.Impl.DropTable(table)
}

func (a *AdapterDB) Load() error {
	return a.Impl.Load()
}

func (a *AdapterDB) Close() error {
	return a.Impl.Close()
}

func (a *AdapterDB) BeginTransaction() Transaction {
	return &AdapterTx{Impl: a.Impl.BeginTransaction()}
}

type AdapterTx struct {
	Impl litegodb.Transaction
}

func (t *AdapterTx) PutBatch(table string, key int, value string) {
	t.Impl.PutBatch(table, key, value)
}

func (t *AdapterTx) DeleteBatch(table string, key int) {
	t.Impl.DeleteBatch(table, key)
}

func (t *AdapterTx) Commit() error {
	return t.Impl.Commit()
}

func (t *AdapterTx) Rollback() {
	t.Impl.Rollback()
}
