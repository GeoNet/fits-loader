package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

var (
	checkType    *sql.Stmt
	checkSample  *sql.Stmt
)

// initSource should be called after the db is available.
func initSource() (err error) {
	checkType, err = db.Prepare(`SELECT typeID
					 FROM fits.type 
					 JOIN fits.type_method USING (typepk) 
					 JOIN fits.method USING (methodpk) 
					 WHERE 
					 typeid = $1 
					 AND 
					 methodid = $2`)
	if err != nil {
		return err
	}

	checkSample, err = db.Prepare(`SELECT DISTINCT (sampleID) sampleID 
						FROM fits.sample join fits.system using (systempk) 
						WHERE sampleID = $1
						AND 
						systemID = $2`)
	if err != nil {
		return err
	}

	return
}

type source struct {
	Properties  sourceProperties
	Type        string
	Coordinates []float64
}

type sourceProperties struct {
	SiteID, Name, TypeID, MethodID, SampleID, SystemID string
	Height, GroundRelationship                                    float64
}

func (s *source) longitude() float64 {
	return s.Coordinates[0]
}

func (s *source) latitude() float64 {
	return s.Coordinates[1]
}

func (s *source) unmarshall(b []byte) (err error) {
	err = json.Unmarshal(b, s)
	if err != nil {
		return err
	}

	if s.Type != "Point" {
		return fmt.Errorf("found non Point type: %s", s.Type)
	}

	if s.Coordinates == nil || len(s.Coordinates) != 2 {
		return fmt.Errorf("didn't find correct coordinates for point")
	}

	if s.Properties.SampleID == "" {
		s.Properties.SampleID = "none"
	}

	if s.Properties.SystemID == "" {
		s.Properties.SystemID = "none"
	}

	return err
}

func (s *source) valid() (err error) {
	var d string

	err = checkType.QueryRow(s.Properties.TypeID, s.Properties.MethodID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("typeID.methodID not found in the DB for %s.%s", s.Properties.TypeID, s.Properties.MethodID)
	}
	if err != nil {
		return err
	}

	err = checkSample.QueryRow(s.Properties.SampleID, s.Properties.SystemID).Scan(&d)
	if err == sql.ErrNoRows {
		return fmt.Errorf("sampleID not found in the DB: %s %s", s.Properties.SampleID, s.Properties.SystemID)
	}
	if err != nil {
		return err
	}

	return err
}

func (s *source) saveSite() (err error) {
	_, err = addSite.Exec(
		s.Properties.SiteID,
		s.Properties.Name,
		s.longitude(),
		s.latitude(),
		s.Properties.Height,
		s.Properties.GroundRelationship)

	return err
}
