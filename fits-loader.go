package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

type Config struct {
	DataBase DataBase
}

type DataBase struct {
	Host, User, Password, SSLMode string
	MaxOpenConns, MaxIdleConns    int
}

// version 1.x no longer uses the network code in the DB.
const vers = "1.0"

var (
	config                                       = initConfig()
	db                                           *sql.DB
	dataDir                                      string
	configFile                                   string
	dryRun, deleteFirst, slog, version, locValid bool
)

func initConfig() Config {
	flag.StringVar(&dataDir, "data-dir", "", "path to directory of observation and source files.")
	flag.StringVar(&configFile, "config-file", "fits-loader.json", "optional file to load the config from.")
	flag.BoolVar(&slog, "syslog", false, "output log messages to syslog instead of stdout.")
	flag.BoolVar(&deleteFirst, "delete-first", false, "sync the FITS DB data with the information in each observation file.")
	flag.BoolVar(&dryRun, "dry-run", false, "data is parsed and validated but not loaded to the DB.  A DB connection is needed for validation.")
	flag.BoolVar(&locValid, "local-validate", false, "data is parsed and validated without a connection to the DB.")
	flag.BoolVar(&version, "version", false, "prints the version and exits.")
	flag.Parse()

	if version {
		fmt.Printf("fits-loader version %s\n", vers)
		os.Exit(1)
	}

	if locValid {
		fmt.Println("Validating without DB connection")
	}

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

	var c Config
	if !locValid {
		f, err := os.ReadFile(configFile)
		if err != nil {
			log.Printf("ERROR - problem loading %s - can't find any config.", configFile)
			log.Fatal(err)
		}

		err = json.Unmarshal(f, &c)
		if err != nil {
			log.Println("Problem parsing config file.")
			log.Fatal(err)
		}
	}

	return c
}

func main() {
	if dataDir == "" {
		log.Fatal("please specify the data directory")
	}

	if !locValid {
		if err := config.initDB(); err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	}

	log.Printf("searching for observation and source data in %s", dataDir)
	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var proc []data

	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			log.Fatalf("error getting file info for %s: %s", f.Name(), err.Error())
		}
		if !f.IsDir() && strings.HasSuffix(f.Name(), `.csv`) && info.Size() > 0 {
			meta := f.Name()
			meta = strings.TrimSuffix(meta, `.csv`) + `.json`

			if _, err := os.Stat(dataDir + "/" + meta); os.IsNotExist(err) {
				log.Fatalf("found no json source file for %s", f.Name())
			}
			proc = append(proc, data{
				sourceFile:      dataDir + "/" + meta,
				observationFile: dataDir + "/" + f.Name(),
			})
		}
	}

	log.Printf("found %d observation files to process", len(proc))

	for _, d := range proc {
		log.Printf("reading and validating %s", d.observationFile)
		if err := d.parseAndValidate(); err != nil {
			log.Fatal(err)
		}

		if !dryRun && !locValid {
			log.Printf("saving site information from %s", d.sourceFile)
			if err := d.saveSite(); err != nil {
				log.Fatal(err)
			}

			log.Printf("saving observations from %s", d.observationFile)

			if !deleteFirst {
				if err := d.updateOrAdd(); err != nil {
					log.Fatal(err)
				}
			} else {
				if err := d.deleteThenSave(); err != nil {
					log.Fatal(err)
				}
			}
		}

	}
}

// initDB starts the DB connection pool.  Defer a db.Close() after calling this.
func (c *Config) initDB() (err error) {
	db, err = sql.Open("postgres", "connect_timeout=1 user="+c.DataBase.User+
		" password="+c.DataBase.Password+
		" host="+c.DataBase.Host+
		" connect_timeout=30"+
		" dbname=fits"+
		" sslmode="+c.DataBase.SSLMode)
	if err != nil {
		return err
	}

	db.SetMaxIdleConns(c.DataBase.MaxIdleConns)
	db.SetMaxOpenConns(c.DataBase.MaxOpenConns)

	if err := db.Ping(); err != nil {
		return err
	}

	if err := initData(); err != nil {
		return err
	}

	if err := initSource(); err != nil {
		return err
	}

	return err
}
