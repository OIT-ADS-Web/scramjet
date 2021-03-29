package scramjet_test

import (
	"testing"

	sj "github.com/OIT-ADS-Web/scramjet"
)

func TestCreateTables(t *testing.T) {
	// NOTE: setup() calls sj.Configure(--)
	if !sj.StagingTableExists() {
		t.Error("did not create staging table")
	}
	if !sj.ResourceTableExists() {
		t.Error("did not create resource table")
	}
}
