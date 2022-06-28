package weatherdata

import (
	"database/sql"
	"log"
	"os"
	"testing"

	"github.com/go-testfixtures/testfixtures/v3"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

const DatabaseFile = "./testdata/database/weather_station_test.sqlite"

var (
	db       *sql.DB
	fixtures *testfixtures.Loader
)

func TestMain(m *testing.M) {
	var err error

	db, err := sql.Open("sqlite3", DatabaseFile)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	fixtures, err = testfixtures.New(
		testfixtures.Database(db),
		testfixtures.Dialect("sqlite"),
		testfixtures.Directory("testdata/fixtures"),
	)

	if err != nil {
		log.Fatalln(err.Error())
	}

	os.Exit(m.Run())
}

func prepareTestDatabase() {
	if err := fixtures.Load(); err != nil {
		log.Fatal(err.Error())
	}
}

func TestCanSelectAllRecordsCorrectly(t *testing.T) {
	prepareTestDatabase()

	db, err := sql.Open("sqlite3", DatabaseFile)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	wds := NewWeatherDataService(db)
	weatherData := wds.GetWeatherData(WeatherDataSearchParams{})
	if len(weatherData) != 11 {
		t.Errorf("got %d, expected %d", len(weatherData), 11)
	}
}

func TestCanSelectFilteredRecordsCorrectly(t *testing.T) {
	prepareTestDatabase()

	db, err := sql.Open("sqlite3", DatabaseFile)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	wds := NewWeatherDataService(db)

	testData := map[WeatherDataSearchParams]int{
		WeatherDataSearchParams{StartDate: "2022-04-01"}:             5,
		WeatherDataSearchParams{"2022-04-01", "2022-04-02 23:59:59"}: 5,
		WeatherDataSearchParams{EndDate: "2022-04-02 23:59:59"}:      11,
		WeatherDataSearchParams{"2020-01-01", "2020-01-03 23:59:59"}: 3,
	}

	for searchData, expectedResultCount := range testData {
		weatherData := wds.GetWeatherData(searchData)
		if len(weatherData) != expectedResultCount {
			t.Errorf("got %d, expected %d", len(weatherData), expectedResultCount)
		}
	}
}
