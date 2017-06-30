# FITS Loader

Loads source and observation data into the Field Information Time Series data base.

[![Build Status](https://snap-ci.com/GeoNet/fits-loader/branch/master/build_image)](https://snap-ci.com/GeoNet/fits-loader/branch/master)

### Usage

`fits-loader` reads a data directory looking for observation CSV files.  Each observation file must have a corresponding source JSON file that 
contains the addtional meta data required for processing the observation file.  Each observation and source file should have the same name with appropriate extensions e.g., observations: `VGT2_e.csv` and source: `VGT2_e.json`

#### File Formats

##### Observation File

CSV with 3 columns and one header line.  Duplicate date time stamps in the file are a validation error.  

```
date time, e (mm), error (mm)
2012-07-31T12:01:04.000000Z,-0.00,4.26
2012-08-01T11:58:56.000000Z,1.07,4.48
2012-08-02T12:01:04.000000Z,-1.03,3.95
2012-08-03T11:58:56.000000Z,-1.95,3.91
2012-08-04T12:01:04.000000Z,4.33,3.39
2012-08-05T11:58:56.000000Z,0.18,3.75
2012-08-06T12:01:04.000000Z,4.61,4.64

```

##### Source File

```
{
 	"type": "Point",
 	"coordinates": [
 	175.673170826,
 	-39.108617051
 	],
 	"properties": {
 		"siteID": "VGT2",
 		"height": -999.9,
 		"groundRelationship": -999.9,
 		"name": "Te Maari 2",
 		"typeID": "e",
 		"methodID": "bernese5"
 	}
 }
```

#### Command Line

###### Configuration

The file `fits-loader.json` holds the database configuration e.g.,

```
{
	"DataBase": {
		"Host": "localhost",
		"User": "fits_w",
		"Password": "test",
		"MaxOpenConns": 2, 
		"MaxIdleConns": 1,
		"SSLMode": "require"
	}
}

```

  Save an appropriately edited version of this somewhere on your filesystem and specify the path to it when running `fits-loader` e.g.,

 ```
fits-loader --config-file /etc/sysconfig/fits-loader.json ...
```

`SSLMode` should be set to `require` for running with databases that support it (e.g., AWS RDS).  This setting is automatically changed to `disable` when running the tests.  See also Connection String Parameters here http://godoc.org/github.com/lib/pq  

###### Update or Add Data

Specify a path to a directory containing observation CSV and source JSON files.  There must be one source JSON file per CSV file.  

```
fits-loader --config-file /etc/sysconfig/fits-loader.json --data-dir /work/gnss
```

* Observation and source data are loaded and validated.
* Site information is added to the DB or updated where the siteID already exists.
* Observations for the source are added to the DB or where there are already observations for the source at the date times in the observation
file the value and error are updated.


###### Sync Data

The observations in the DB  for the source are synchronised exactly with those in the observation file.

```
fits-loader --config-file /etc/sysconfig/fits-loader.json --data-dir /work/gnss --delete-first
```

 Observation and source data are loaded and validated.
* Site information is added to the DB or updated where the siteID already exists.
* Observations in the DB for the source are exactly synchronised with the observations in the file.

###### Validation

Use any of the above commands to parse validate data without attempting saving to the DB by adding:

```
--dry-run
```

###### Syslogging

switch to syslogging by adding

```
--syslog
```

### Building

```
godep go build
```

