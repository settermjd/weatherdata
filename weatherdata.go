package weatherdata

import "time"

type WeatherData struct {
	Timestamp   time.Time
	Humidity    float32
	Temperature float32
}
