package integrations

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/rafaelmgr12/litegodb/internal/storage/disk"
	"github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
	"github.com/stretchr/testify/require"
)

const (
	stressDbFile  = "test_stress.db"
	stressLogFile = "test_stress.log"
)

func setupStressKVStore(t *testing.T) (*kvstore.BTreeKVStore, func()) {
	_ = os.Remove(stressDbFile)
	_ = os.Remove(stressLogFile)

	diskManager, err := disk.NewFileDiskManager(stressDbFile)
	require.NoError(t, err)

	kvStore, err := kvstore.NewBTreeKVStore(3, diskManager, stressLogFile)
	require.NoError(t, err)

	cleanup := func() {
		kvStore.Close()
		diskManager.Close()
		os.Remove(stressDbFile)
		os.Remove(stressLogFile)
	}

	return kvStore, cleanup
}

func TestStressKVStore(t *testing.T) {
	kvStore, cleanup := setupStressKVStore(t)
	defer cleanup()

	table := "stress_table"
	err := kvStore.CreateTableName(table, 3)
	require.NoError(t, err)

	const numberOfWorkers = 20
	const numberOfRecords = 10000

	var wg sync.WaitGroup
	wg.Add(numberOfWorkers)

	for i := 0; i < numberOfWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numberOfRecords; j++ {
				key := workerID*numberOfRecords + j
				value := fmt.Sprintf("value%d", key)
				err := kvStore.Put(table, key, value)
				require.NoError(t, err)

				gotValue, found, err := kvStore.Get(table, key)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, value, gotValue)
			}
		}(i)
	}

	wg.Wait()

	// Perform read verification after all writes are done
	for i := 0; i < numberOfWorkers; i++ {
		for j := 0; j < numberOfRecords; j++ {
			key := i*numberOfRecords + j
			expectedValue := fmt.Sprintf("value%d", key)
			gotValue, found, err := kvStore.Get(table, key)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, gotValue)
		}
	}
}

func TestStressKVStoreWithTransactions(t *testing.T) {
	kvStore, cleanup := setupStressKVStore(t)
	defer cleanup()

	table := "stress_tx_table"
	err := kvStore.CreateTableName(table, 3)
	require.NoError(t, err)

	const numberOfWorkers = 10
	const numberOfRecords = 5000

	var wg sync.WaitGroup
	wg.Add(numberOfWorkers)

	for i := 0; i < numberOfWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			tx := kvStore.BeginTransaction()
			for j := 0; j < numberOfRecords; j++ {
				key := workerID*numberOfRecords + j
				value := fmt.Sprintf("value%d", key)
				tx.PutBatch(table, key, value)
			}
			err := tx.Commit()
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify all records
	for i := 0; i < numberOfWorkers; i++ {
		for j := 0; j < numberOfRecords; j++ {
			key := i*numberOfRecords + j
			expectedValue := fmt.Sprintf("value%d", key)
			gotValue, found, err := kvStore.Get(table, key)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, gotValue)
		}
	}
}
