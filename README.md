# FITS Loader

Loads CSV data into the Field Information Time Series data base.

### Building

```
godep go build
```

### Usage

###### Sync Data from CSV Files

```
fits-loader --config-file fits-loader.json --data-dir /work/gnss/CSV --delete-first
```

###### Update or Add Data from CSV Files

```
fits-loader --config-file fits-loader.json --data-dir /work/gnss/CSV 
```

###### Update or Add Site Information

```
fits-loader --config-file fits-loader.json  --site-file /work/gnss-sites.csv
```

###### Validation

Use any of the above commands to validate data without loading to the DB by adding:

```
--dry-run
```

###### Syslogging

switch to syslogging by adding

```
--syslog
```

### File Formats

###### Sites

```
CG,VGWH,"Whangaehu Hut",175.588983537,-39.282405455075185,-999.9,-999.9
CG,VGWN,"Wahianoa",175.597857026,-39.3269275090749,-999.9,-999.9
CG,VGWT,"West Tongariro",175.589695416,-39.115141444076286,-999.9,-999.9
CG,WAHU,"Waihua",177.234413566,-39.07721064207654,-999.9,-999.9
```

###### Observations

```
LI,AUCK,e,bernese5
2000-01-02T12:00:00.000000Z,-4.12,4.32
2000-01-03T12:00:00.000000Z,-9.07,3.26
2000-01-04T12:00:00.000000Z,-12.46,5.23
2000-01-05T12:00:00.000000Z,-3.75,3.10
2000-01-06T12:00:00.000000Z,-7.72,3.45
```