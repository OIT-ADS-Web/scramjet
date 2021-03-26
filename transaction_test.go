package scramjet_test

/*
import (
	"fmt"
	"testing"

	sj "github.com/OIT-ADS-Web/scramjet"
)

func TestCreateTransaction(t *testing.T) {
	// NOTE: setup() calls sj.Configure(--)
	err := sj.ClearAllTransactions()
	if err != nil {
		t.Error("error clearing out transactions table")
	}

	id := sj.NewTransaction()
	fmt.Printf("transaction=%d\n", id)

	count := sj.TransactionCount()
	if count != 1 {
		t.Error("did not count 1 transaction after create")
	}
	fmt.Printf("count=%d\n", count)

	found, err := sj.RetrieveSingleTransaction(id)

	if err != nil {
		t.Errorf("could not find transaction id=%d", id)
	}

	if found.TransactionId != id {
		t.Errorf("id of transaction (%d) does not match id=%d", found.TransactionId, id)
	}

	err = sj.ClearAllTransactions()
	if err != nil {
		t.Error("error clearing out transactions table")
	}
	count = sj.TransactionCount()
	if count != 0 {
		t.Error("did not count 0 transaction after clearing")
	}
}

func TestCompleteTransaction(t *testing.T) {
	// 1. clear out
	err := sj.ClearAllTransactions()
	if err != nil {
		t.Error("error clearing out transactions table")
	}
	id := sj.NewTransaction()
	found, _ := sj.RetrieveSingleTransaction(id)
	if found.CompletedAt.Valid {
		t.Error("transaction should not be complete yet")
	}

	err = sj.FinishTransaction(id)
	if err != nil {
		t.Error("error finishing transaction")
	}
	found, _ = sj.RetrieveSingleTransaction(id)
	if !found.CompletedAt.Valid {
		t.Error("transaction *should* be complete yet")
	}

	fmt.Printf("transaction=#%v", found)
}
*/
