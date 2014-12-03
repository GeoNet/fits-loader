package main

// TODO version flag.
// TODO help?

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"log/syslog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DataBase DataBase
}

type DataBase struct {
	Host, User, Password       string
	MaxOpenConns, MaxIdleConns int
}

type site struct {
	networkID, siteID, name                          string
	longitude, latitude, height, ground_relationship float64
}

type source struct {
	networkID, siteID, typeID, methodID, sampleID, systemID string
}

type observation struct {
	obsTime         time.Time
	value, obsError float64
}

type data struct {
	source source
	obs    []observation
}

var (
	config         Config
	db             *sql.DB
	addSite        *sql.Stmt
	checkNetwork   *sql.Stmt
	checkSite      *sql.Stmt
	checkType      *sql.Stmt
	checkSample    *sql.Stmt
	checkSystem    *sql.Stmt
	addObservation *sql.Stmt
	siteFile       string
	dataDir        string
	slog           bool
	configFile     string
	dryRun         bool
	deleteFirst    bool
)

func init() {
	flag.StringVar(&siteFile, "site-file", "", "CSV file of site information to load into the FITS database.")
	flag.StringVar(&dataDir, "data-dir", "", "path to directory of observation CSV files.")
	flag.StringVar(&configFile, "config-file", "/etc/sysconfig/fits-loader.json", "optional file to load the config from.")
	flag.BoolVar(&slog, "syslog", false, "output log messages to syslog instead of stdout.")
	flag.BoolVar(&deleteFirst, "delete-first", false, "sync the FITS DB data with the information in each CSV file.  Duplicate time stamps are a validation error.")
	flag.BoolVar(&dryRun, "dry-run", false, "data is parsed and validated but not loaded to the DB.  A DB connection is needed for validation.")
	flag.Parse()

	if slog {
		logwriter, err := syslog.New(syslog.LOG_NOTICE, "fits-loader")
		if err == nil {
			log.Println("** logging to syslog **")
			log.SetOutput(logwriter)
		} else {
			log.Println("problem switching to syslog.  Contiuning.")
			log.Println(err)
		}
	}

	f, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Printf("ERROR - problem loading %s - can't find any config.", configFile)
		log.Fatal(err)
	}

	err = json.Unmarshal(f, &config)
	if err != nil {
		log.Println("Problem parsing config file.")
		log.Fatal(err)
	}
}

func main() {
	if err := initDB(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := prepareStatements(); err != nil {
		log.Fatal(err)
	}

	if siteFile != "" {
		if err := saveSItes(siteFile); err != nil {
			log.Fatal(err)
		}
	}

	if dataDir != "" {
		log.Printf("searching for CSV data in %s", dataDir)
		if err := loadData(dataDir); err != nil {
			log.Fatal(err)
		}
	}
}

// loadData lists files ending in '.csv' in the directory d.  Calls saveData on each file.
func loadData(d string) (err error) {
	files, err := ioutil.ReadDir(d)
	if err != nil {
		return err
	}

	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), `.csv`) && f.Size() > 0 {
			if err := saveData(d + "/" + f.Name()); err != nil {
				return err
			}
		}
	}

	return err
}

// saveData reads, parses, and validates the CSV file pointed to by f.
// if dryRun is false it save the contents of the file to the FITS DB.
func saveData(f string) (err error) {

	log.Printf("parsing and validating %s", f)

	of, err := os.Open(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(of)

	// header line - 4 fields
	// networkID, siteID, typeID, methodID
	// e.g.,
	// LI,TAUP,e,bernese5
	r.FieldsPerRecord = 4
	r.TrimLeadingSpace = true

	head, err := r.Read()
	if err != nil {
		return fmt.Errorf("error reading %s: %s", of.Name(), err)
	}

	// Observations for the rest of the file.  This has 3 fields per record
	// date-time, value, error
	// e.g.,
	// 2002-03-22T12:00:00.000000Z,-18.86,3.07
	r.FieldsPerRecord = 3
	rawObs, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading %s: %s", of.Name(), err)
	}

	source := source{networkID: head[0],
		siteID:   head[1],
		typeID:   head[2],
		methodID: head[3],
		sampleID: "none",
		systemID: "none"}

	obs := make([]observation, len(rawObs))

	for i, r := range rawObs {
		o := observation{}

		o.obsTime, err = time.Parse(time.RFC3339Nano, r[0])
		if err != nil {
			return fmt.Errorf("error parsing date time for file %s: %s", f, r[0])
		}

		o.value, err = strconv.ParseFloat(r[1], 64)
		if err != nil {
			return fmt.Errorf("error parsing value for file %s: %s", f, r[1])
		}

		o.obsError, err = strconv.ParseFloat(r[2], 64)
		if err != nil {
			return fmt.Errorf("error parsing error for file %s: %s", f, r[2])
		}

		obs[i] = o
	}

	data := data{
		source: source,
		obs:    obs,
	}

	if err := source.valid(); err != nil {
		return fmt.Errorf("invalid header for %s: %s", of.Name(), err)
	}

	if deleteFirst {
		if err := data.checkDuplicateObs(); err != nil {
			return fmt.Errorf("duplicate observations for %s: %s", of.Name(), err)
		}
	}

	if !dryRun {
		if !deleteFirst {
			if err := data.updateOrAdd(); err != nil {
				return err
			}
		} else {
			if err := data.deleteThenSave(); err != nil {
				return err
			}
		}
	}

	return err
}

// updateOrAdd saves data in the data struct pointed to by d to the FITS DB.  If
// an observation already exists for the source timestamp then the value and error are updated
// otherwise the data is inserted.
func (d *data) updateOrAdd() (err error) {
	log.Printf("adding or updating FITS db with %s observations for %s.%s", d.source.typeID, d.source.networkID, d.source.siteID)
	for _, o := range d.obs {
		_, err = addObservation.Exec(
			d.source.networkID,
			d.source.siteID,
			d.source.typeID,
			d.source.methodID,
			d.source.sampleID,
			d.source.systemID,
			o.obsTime,
			o.value,
			o.obsError)
		if err != nil {
			return err
		}
	}

	return err
}

// deleteThenSave saves data to the FITS db.  Observations for the source are first deleted and then
// values in *obs added.  This is done in a transaction.
func (d *data) deleteThenSave() (err error) {

	// you can test the transaction by adding a duplicate observation.  Either here by adding to the insert string
	// or in the source data (if you don't use data.checkDuplicateObs() before calling this function).

	log.Printf("syncing FITS db with %s observations for %s.%s", d.source.typeID, d.source.networkID, d.source.siteID)

	tx, err := db.Begin()

	var sitePK int
	err = tx.QueryRow(`SELECT DISTINCT ON (sitepk) sitepk 
				FROM fits.site JOIN fits.network USING (networkpk) 
				WHERE siteid = $2 
				AND 
				networkid = $1`, d.source.networkID, d.source.siteID).Scan(&sitePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get sitePK for %s.%s", d.source.networkID, d.source.siteID)
	}
	if err != nil {
		return err
	}

	var samplePK int
	err = tx.QueryRow(`SELECT DISTINCT ON (samplePK) samplePK
				FROM fits.sample join fits.system using (systempk)
				WHERE sampleID = $1
				AND
				systemID = $2`, d.source.sampleID, d.source.systemID).Scan(&samplePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get samplePK for %s.%s", d.source.sampleID, d.source.systemID)
	}
	if err != nil {
		return err
	}

	var methodPK int
	err = tx.QueryRow(`SELECT methodPK FROM fits.method WHERE methodID = $1`, d.source.methodID).Scan(&methodPK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get methodPK for %s", d.source.methodID)
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
				 methodid = $2`, d.source.typeID, d.source.methodID).Scan(&typePK)
	if err == sql.ErrNoRows {
		return fmt.Errorf("couldn't get typePK for %s.%s", d.source.typeID, d.source.methodID)
	}
	if err != nil {
		return err
	}

	obsDelete, err := tx.Prepare(`DELETE FROM fits.observation
					WHERE
					sitepk = (SELECT DISTINCT ON (sitepk) sitepk FROM fits.site JOIN fits.network USING (networkpk) WHERE siteid = $2 AND networkid = $1)
					AND
					typePK = (SELECT DISTINCT ON (typepk)  typepk FROM fits.type WHERE typeid = $3)
					`)
	if err != nil {
		return err
	}
	defer obsDelete.Close()

	insert := `INSERT INTO fits.observation(sitePK, typePK, methodPK, samplePK, time, value, error) VALUES `

	var row string
	for _, v := range d.obs {
		row = fmt.Sprintf("(%d, %d, %d, %d, '%s'::timestamptz, %f, %f),", sitePK, typePK, methodPK, samplePK, v.obsTime.Format(time.RFC3339), v.value, v.obsError)
		insert += row
	}

	insert = strings.TrimSuffix(insert, ",")

	obsInsert, err := tx.Prepare(insert)
	if err != nil {
		return err
	}
	defer obsInsert.Close()

	_, err = obsDelete.Exec(d.source.networkID, d.source.siteID, d.source.typeID)
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

// saveSites reads CSV site data from a file, validates the site information, then saves it to the FITS database.
//
// The CSV file format must be one line per site with 7 columns:
//    networkID, siteID, name, longitude, latitude, height, ground_relationship
// e.g.,
//    XX,NLS1,"Nelson Landslide 1",173.255763078,-41.27828177106451,-999.9,-999.9
//
// Fields can be surrounded with double quotes.  This allows names to have apostrophe etc.
// Leading white space is trimmed from a field.
func saveSItes(f string) (err error) {
	log.Printf("parsing and validating site data from %f", f)

	file, err := os.Open(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(file)

	r.FieldsPerRecord = 7
	r.TrimLeadingSpace = true

	rawSites, err := r.ReadAll()
	if err != nil {
		return err
	}

	for _, e := range rawSites {
		s := site{
			networkID: e[0],
			siteID:    e[1],
			name:      e[2],
		}

		s.longitude, err = strconv.ParseFloat(e[3], 64)
		if err != nil {
			return fmt.Errorf("error parsing longitude for site %s.%s: %s", s.networkID, s.siteID, e[3])
		}

		s.latitude, err = strconv.ParseFloat(e[4], 64)
		if err != nil {
			return fmt.Errorf("error parsing latitude for site %s.%s: %s", s.networkID, s.siteID, e[4])
		}

		s.height, err = strconv.ParseFloat(e[5], 64)
		if err != nil {
			return fmt.Errorf("error parsing height for site %s.%s: %s", s.networkID, s.siteID, e[5])
		}

		s.ground_relationship, err = strconv.ParseFloat(e[6], 64)
		if err != nil {
			return fmt.Errorf("error parsing ground_relationship for site %s.%s: %s", s.networkID, s.siteID, e[6])
		}

		if err = s.valid(); err != nil {
			return fmt.Errorf("invalid site information for %s.%s: %s", s.siteID, s.networkID, err)
		}

		if !dryRun {
			log.Printf("adding or updating FITS DB site information for %s.%s", s.networkID, s.siteID)
			if err = s.save(); err != nil {
				return err
			}
		}
	}
	return err
}

// valid checks that the site  referred to by the pointer s is valid.
// * Checks that s.networkID is in the DB.
// Keep in mind the DB could change between validation and save.
func (s *site) valid() (err error) {
	var d string

	err = checkNetwork.QueryRow(s.networkID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("networkID %s not found in the DB", s.networkID)
	}
	if err != nil {
		return err
	}

	return err
}

// save saves the site referred to by the pointer s to the FITS DB.
func (s *site) save() (err error) {
	_, err = addSite.Exec(s.networkID, s.siteID, s.name, s.longitude, s.latitude, s.height, s.ground_relationship)
	return err
}

// valid makes sure the source information is valid (exists in the FITS DB).
func (s *source) valid() (err error) {
	var d string

	err = checkSite.QueryRow(s.networkID, s.siteID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("siteID, networkID not found in the DB for %s.%s", s.networkID, s.siteID)
	}
	if err != nil {
		return err
	}

	err = checkType.QueryRow(s.typeID, s.methodID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("typeID.methodID not found in the DB for %s.%s", s.typeID, s.methodID)
	}
	if err != nil {
		return err
	}

	err = checkSample.QueryRow(s.sampleID, s.systemID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("sampleID not found in the DB: %s %s", s.sampleID, s.systemID)
	}
	if err != nil {
		return err
	}

	return err
}

// checkDuplicateObs makes sure there are no duplicate timestamps in d.obs
func (d *data) checkDuplicateObs() (err error) {
	o := make(map[string]int, len(d.obs))

	for _, v := range d.obs {
		o[v.obsTime.Format(time.RFC3339)] = 1
	}

	dups := len(d.obs) - len(o)

	if dups == 0 {
		return err
	}

	return fmt.Errorf("found %d duplicate timestamp(s)", dups)
}

// initDB starts the DB connection pool.  Defer a db.Close() after calling this.
func initDB() (err error) {
	db, err = sql.Open("postgres", "connect_timeout=1 user="+config.DataBase.User+
		" password="+config.DataBase.Password+
		" host="+config.DataBase.Host+
		" connect_timeout=30"+
		" dbname=fits sslmode=disable")
	if err != nil {
		return err
	}

	db.SetMaxIdleConns(config.DataBase.MaxIdleConns)
	db.SetMaxOpenConns(config.DataBase.MaxOpenConns)

	if err := db.Ping(); err != nil {
		return err
	}

	return err
}

// prepared statements.  Call once the DB is available.
func prepareStatements() (err error) {
	// networkID, siteID, name, longitude, latitude, height, ground_relationship
	addSite, err = db.Prepare("SELECT fits.add_site($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		return fmt.Errorf("add site statement: %s", err)
	}

	checkNetwork, err = db.Prepare("SELECT networkID from fits.network where networkID = $1")
	if err != nil {
		return fmt.Errorf("check network statement: %s", err)
	}

	checkSite, err = db.Prepare("SELECT siteID FROM fits.site JOIN fits.network USING (networkpk) WHERE networkID = $1 AND siteID = $2")
	if err != nil {
		return fmt.Errorf("check site statement: %s", err)
	}

	checkType, err = db.Prepare(`SELECT typeID
					 FROM fits.type 
					 JOIN fits.type_method USING (typepk) 
					 JOIN fits.method USING (methodpk) 
					 WHERE 
					 typeid = $1 
					 AND 
					 methodid = $2`)
	if err != nil {
		return fmt.Errorf("check type statement: %s", err)
	}

	checkSample, err = db.Prepare(`SELECT DISTINCT (sampleID) sampleID 
						FROM fits.sample join fits.system using (systempk) 
						WHERE sampleID = $1
						AND 
						systemID = $2`)
	if err != nil {
		return fmt.Errorf("check sample statement: %s", err)
	}

	// networkID, siteID, typeID, methodID, sampleID, systemID, time, value, error
	addObservation, err = db.Prepare("SELECT fits.add_observation($1, $2, $3, $4, $5, $6, $7, $8 ,$9)")
	if err != nil {
		return fmt.Errorf("add observation statement: %s", err)
	}

	return err
}
