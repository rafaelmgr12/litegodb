package server

import "github.com/rafaelmgr12/litegodb/internal/interfaces"

type fakeDB struct {
	putFn    func(string, int, string) error
	getFn    func(string, int) (string, bool, error)
	deleteFn func(string, int) error
}

func (f *fakeDB) Put(table string, key int, value string) error {
	if f.putFn != nil {
		return f.putFn(table, key, value)
	}
	return nil
}

func (f *fakeDB) Get(table string, key int) (string, bool, error) {
	if f.getFn != nil {
		return f.getFn(table, key)
	}
	return "", false, nil
}

func (f *fakeDB) Delete(table string, key int) error {
	if f.deleteFn != nil {
		return f.deleteFn(table, key)
	}
	return nil
}

func (f *fakeDB) Flush(table string) error                 { return nil }
func (f *fakeDB) CreateTable(table string, d int) error    { return nil }
func (f *fakeDB) DropTable(table string) error             { return nil }
func (f *fakeDB) Load() error                              { return nil }
func (f *fakeDB) Close() error                             { return nil }
func (f *fakeDB) BeginTransaction() interfaces.Transaction { return nil }
