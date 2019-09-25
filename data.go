package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var (
	addObservation *sql.Stmt
	addSite        *sql.Stmt
)

// initData should be called after the db is available.
func initData() (err error) {
	// siteID, typeID, methodID, sampleID, systemID, time, value, error
	addObservation, err = db.Prepare("SELECT fits.add_observation($1, $2, $3, $4, $5, $6, $7, $8)")

	// siteID, name, longitude, latitude, height, ground_relationship
	addSite, err = db.Prepare("SELECT fits.add_site($1, $2, $3, $4, $5, $6)")
	if err != nil {
		return err
	}

	return
}

type data struct {
	sourceFile, observationFile string
	source
	observation
}

func (d *data) parseAndValidate() (err error) {

	b, err := ioutil.ReadFile(d.sourceFile)
	if err != nil {
		return err
	}

	if err = d.unmarshall(b); err != nil {
		return err
	}

	f, err := os.Open(d.observationFile)
	if err != nil {
		return err
	}
	defer f.Close()

	if err = d.read(f); err != nil {
		return err
	}
	f.Close()

	if !locValid {
		if err = d.valid(); err != nil {
			return err
		}
	}

	return err
}

// updateOrAdd saves data to by d to the FITS DB.  If
// an observation already exists for the source timestamp then the value and error are updated
// otherwise the data is inserted.
func (d *data) updateOrAdd() (err error) {
	for _, o := range d.obs {
		_, err = addObservation.Exec(
			d.Properties.SiteID,
			d.Properties.TypeID,
			d.Properties.MethodID,
			d.Properties.SampleID,
			d.Properties.SystemID,
			o.t,
			o.v,
			o.e)
		if err != nil {
			return err
		}
	}

	return err
}

// deleteThenSave saves data to the FITS db.  Observations for the source are first deleted and then
// values in *obs added.  This is done in a transaction.
func (d *data) deleteThenSave() (err error) {

	tx, err := db.Begin()

	var sitePK int
	err = tx.QueryRow(`SELECT DISTINCT ON (sitepk) sitepk 
				FROM fits.site WHERE siteid = $1`, d.Properties.SiteID).Scan(&sitePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get sitePK for %s", d.Properties.SiteID)
	}
	if err != nil {
		return err
	}

	var samplePK int
	err = tx.QueryRow(`SELECT DISTINCT ON (samplePK) samplePK
				FROM fits.sample join fits.system using (systempk)
				WHERE sampleID = $1
				AND
				systemID = $2`, d.Properties.SampleID, d.Properties.SystemID).Scan(&samplePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get samplePK for %s.%s", d.Properties.SampleID, d.Properties.SystemID)
	}
	if err != nil {
		return err
	}

	var methodPK int
	err = tx.QueryRow(`SELECT methodPK FROM fits.method WHERE methodID = $1`, d.Properties.MethodID).Scan(&methodPK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get methodPK for %s", d.Properties.MethodID)
	}
	if err != nil {
		return err
	}

	// also checks that the type is valid for this method.
	var typePK int
	err = tx.QueryRow(`SELECT DISTINCT ON (typePK) typePK
				FROM fits.type 
				JOIN fits.type_method USING (typepk) 
				JOIN fits.method USING (methodpk) 
				WHERE 
				typeid = $1 
				AND 
				 methodid = $2`, d.Properties.TypeID, d.Properties.MethodID).Scan(&typePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get typePK for %s.%s", d.Properties.TypeID, d.Properties.MethodID)
	}
	if err != nil {
		return err
	}

	obsDelete, err := tx.Prepare(`DELETE FROM fits.observation
					WHERE
					sitepk = (SELECT DISTINCT ON (sitepk) sitepk FROM fits.site WHERE siteid = $1)
					AND
					typePK = (SELECT DISTINCT ON (typepk)  typepk FROM fits.type WHERE typeid = $2)
					`)
	if err != nil {
		return err
	}
	defer obsDelete.Close()

	insert := `INSERT INTO fits.observation(sitePK, typePK, methodPK, samplePK, time, value, error) VALUES `

	var row string
	for _, v := range d.obs {
		row = fmt.Sprintf("(%d, %d, %d, %d, '%s'::timestamptz, %f, %f),", sitePK, typePK, methodPK, samplePK, v.t.Format(time.RFC3339), v.v, v.e)
		insert += row
	}

	insert = strings.TrimSuffix(insert, ",")

	obsInsert, err := tx.Prepare(insert)
	if err != nil {
		return err
	}
	defer obsInsert.Close()

	_, err = obsDelete.Exec(d.Properties.SiteID, d.Properties.TypeID)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = obsInsert.Exec()
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
