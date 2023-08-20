package ods_scan_data

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)

func ReadDataFromSourceDatabase(config DBConfig, destinationDB *sql.DB, wg *sync.WaitGroup, resultCh chan<- []scan_data) {
	defer wg.Done()

	var rowCnt_write int

	// Connect to the source database (SQL Server)
	sourceDB, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s&encrypt=disable&connection+timeout=30", config.User, config.Password, config.Server, config.Port, config.Database))
	if err != nil {
		log.Printf("Failed to connect to the source database %s on server %s: %v", config.Database, config.Server, err)
		return
	}
	defer sourceDB.Close()

	fmt.Printf("Reading data from source database %s on server %s...\n", config.Database, config.Server)

	// Read data from the 'employees' table
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT top 1000 id, %s as kiosk_id, program_code as program_scan_code, scan_code, scan_time, encrypted_scan_code, placement_code, choice_selected, sample_dispensed, scan_type, location_id, position_id, scan_mode_id, optin_selected FROM freeosk.dbo.scan_data where id > 1 order by id desc", config.kiosk_id))
	if err != nil {
		log.Printf("Error executing query for source database %s on server %s: %v", config.Database, config.Server, err)
		return
	}
	defer rows.Close()

	// Create a slice to hold the Employee structs
	var scandata []scan_data

	// Loop through the rows and store data into the struct
	for rows.Next() {
		var data scan_data
		if err := rows.Scan(&data.id, &data.kiosk_id, &data.program_scan_code, &data.scan_code, &data.scan_time, &data.encrypted_scan_code, &data.placement_code, &data.choice_selected, &data.sample_dispensed, &data.scan_type, &data.location_id, &data.position_id, &data.scan_mode_id, &data.optin_selected); err != nil {
			log.Printf("Error scanning row for source database %s on server %s: %v", config.Database, config.Server, err)
			return
		}
		scandata = append(scandata, data)
	}

	fmt.Printf("Number of scan data retrieved from source database %s on server %s: %d\n", config.Database, config.Server, len(scandata))

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows for source database %s on server %s: %v", config.Database, config.Server, err)
		return
	}

	// Send the slice of employees to the result channel

	fmt.Printf("Writing data to destination database %s on server %s...\n", config.Database, config.Server)

	// Prepare the insert statement
	stmt, err := destinationDB.Prepare("INSERT ignore INTO freeosk.ods_scan_data(id, kiosk_id,  program_scan_code, scan_code, scan_time, encrypted_scan_code, placement_code, choice_selected, sample_dispensed, scan_type, location_id, position_id, scan_mode_id, optin_selected) VALUES(?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?)")
	if err != nil {
		log.Printf("failed to prepare insert statement: %s", err)
	}
	defer stmt.Close()

	// Write data to the 'scan_data' table
	for _, data := range scandata {
		res, err := stmt.Exec(data.id, data.kiosk_id, data.program_scan_code, data.scan_code, data.scan_time, data.encrypted_scan_code, data.placement_code, data.choice_selected, data.sample_dispensed, data.scan_type, data.location_id, data.position_id, data.scan_mode_id, data.optin_selected)
		if err != nil {
			log.Printf("failed to insert data into the destination database: %s", err)
		}

		// number of inserted rows
		affected, err := res.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}

		// count the number of rows inserted
		rowCnt_write = rowCnt_write + int(affected)

		// fmt.Println(fmt.Sprintf("data inserted for id:%s and kiosk_id:%s ", data.id, data.kiosk_id))
	}
	fmt.Println("Total rows inserted:", rowCnt_write)
	fmt.Println("Number of rows processed:", len(scandata))

	resultCh <- scandata

}
