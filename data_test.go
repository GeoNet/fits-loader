package main

import (
	"database/sql"
	"testing"
	"time"
)

func TestUpdateOrAdd(t *testing.T) {
	setup()
	defer teardown()

	cleanDB(t)

	d := data{
		sourceFile:      "etc/VGT2_e.json",
		observationFile: "etc/VGT2_e.csv",
	}

	if err := d.parseAndValidate(); err != nil {
		t.Fatal(err)
	}

	if err := d.saveSite(); err != nil {
		t.Fatal(err)
	}

	if countSites(t) != 1 {
		t.Error("didn't find site in the DB.")
	}

	if err := d.updateOrAdd(); err != nil {
		t.Fatal(err)
	}

	if countObs(t) != 7 {
		t.Error("didn't find 7 observations in the DB.")
	}
}

func TestDeleteThenSave(t *testing.T) {
	setup()
	defer teardown()

	cleanDB(t)

	d := data{
		sourceFile:      "etc/VGT2_e.json",
		observationFile: "etc/VGT2_e.csv",
	}

	if err := d.parseAndValidate(); err != nil {
		t.Fatal(err)
	}

	if err := d.saveSite(); err != nil {
		t.Fatal(err)
	}

	if countSites(t) != 1 {
		t.Error("didn't find site in the DB.")
	}

	// Save  observations with one additional one that is not in the file.
	o := obs{
		t: time.Now().UTC(),
		v: 12.2,
		e: 6.6,
	}

	d.obs = append(d.obs, o)

	if err := d.updateOrAdd(); err != nil {
		t.Fatal(err)
	}

	if countObs(t) != 8 {
		t.Error("didn't find 8 observations in the DB.")
	}

	// Add a duplicate obs and try to delete then save.  Transaction should roll back leaving
	// 8 obs in the DB.
	d.obs = append(d.obs, o)

	if err := d.deleteThenSave(); err == nil {
		t.Fatal("should get transaction error for observation primary key violation.")
	}

	if countObs(t) != 8 {
		t.Error("didn't find 8 observations in the DB.")
	}

	// reload the data then delete and save.  There should be exactly 7 rows in the DB -
	// the extra observation we added earlier is not in the observation file and should be gone.
	if err := d.parseAndValidate(); err != nil {
		t.Fatal(err)
	}

	if err := d.deleteThenSave(); err != nil {
		t.Fatal(err)
	}

	if countObs(t) != 7 {
		t.Error("didn't find 7 observations in the DB.")
	}
}

// clean out all sites and observations from the DB.
func cleanDB(t *testing.T) {
	if err := db.QueryRow("truncate fits.site cascade").Scan(); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
}

// tables have been truncated so we can be lazy with the accuracy of the select

func countObs(t *testing.T) (c int) {
	if err := db.QueryRow(`	select count(*) from fits.observation`).Scan(&c); err != nil {
		t.Fatal(err)
	}

	return
}

func countSites(t *testing.T) (c int) {
	if err := db.QueryRow(`	select count(*) from fits.site`).Scan(&c); err != nil {
		t.Fatal(err)
	}

	return
}
