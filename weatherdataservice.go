// Package weatherdata provides the ability to restrieve weather data from an
// SQL database.
//
// At the moment its functionality is quite rudimentary, but still effective
// for what's required.
package weatherdata

import (
	"database/sql"
	"fmt"
	"log"
)

func convert(params []string) []any {
	list := make([]any, len(params))
	for i, p := range params {
		list[i] = p
	}

	return list
}

const WeatherTable = "weather_data"

// WeatherDataSearchParams is a small struct that can contain an optional start
// and end date for filtering weather data searches, whether when searching a
// database or in other contexts. At the moment, the only expected context is a
// database.
type WeatherDataSearchParams struct {
	StartDate string
	EndDate   string
}

// WeatherDataService allows for searching a SQL database for weather data and
// returning an array of WeatherData items containing the returned results if
// any were available.
type WeatherDataService struct {
	db *sql.DB
}

// BuildSearchQuery returns a parameterised SQL query string to run against the
// database along with the parameters to replace the placeholders with, based
// on the search parameters provided.
func (wds *WeatherDataService) BuildSearchQuery(searchParams WeatherDataSearchParams) (string, []string) {
	var queryParameters []string
	searchQuery := `SELECT humidity, temperature, timestamp FROM %s`

	if searchParams.StartDate != "" && searchParams.EndDate != "" {
		searchQuery = searchQuery + " WHERE timestamp BETWEEN ? AND ? "
		queryParameters = append(queryParameters, searchParams.StartDate, searchParams.EndDate)
		return searchQuery, queryParameters
	}

	if searchParams.StartDate != "" {
		searchQuery = searchQuery + " WHERE timestamp >= ? "
		queryParameters = append(queryParameters, searchParams.StartDate)
	}

	if searchParams.EndDate != "" {
		searchQuery = searchQuery + " WHERE timestamp <= ? "
		queryParameters = append(queryParameters, searchParams.EndDate)
	}

	return searchQuery, queryParameters
}

// NewWeatherDataService returns a new WeatherDataService with an open database
// handle.
func NewWeatherDataService(db *sql.DB) *WeatherDataService {
	if db == nil {
		panic("nil database connection")
	}
	return &WeatherDataService{db}
}

// GetWeatherData returns an array of WeatherData items containing any and all
// entries from the underlying table in the database. The returned records can
// be filtered by providing an optional start and end date in a
// WeatherDataSearchParams struct. By default, the struct is assumbed to be
// empty.
func (wds *WeatherDataService) GetWeatherData(searchParams WeatherDataSearchParams) []WeatherData {

	searchQuery, queryParameters := wds.BuildSearchQuery(searchParams)

	rows, err := wds.db.Query(
		fmt.Sprintf(searchQuery, WeatherTable),
		convert(queryParameters)...,
	)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer rows.Close()

	var weatherData []WeatherData
	for rows.Next() {
		var wd WeatherData
		err := rows.Scan(&wd.Humidity, &wd.Temperature, &wd.Timestamp)
		if err != nil {
			log.Printf("Unable to add record: %s\n", err.Error())
		}
		weatherData = append(weatherData, wd)
	}

	return weatherData
}
