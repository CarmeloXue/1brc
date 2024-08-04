package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {

	file, err := os.Open("../../../../data/weather_stations.csv")
	defer file.Close()
	if err != nil {
		fmt.Println("get errors: %v", err)
	}

	simpleProcess(file)

}

type WeatherStations struct {
	Min   float64
	Max   float64
	Count int
	Total float64
	Avg   float64
}

func simpleProcess(file *os.File) {
	var (
		err             error
		str             []byte
		weatherStations = make(map[string]WeatherStations)
	)
	start := time.Now()

	reader := bufio.NewReader(file)

	for err == nil {
		str, _, err = reader.ReadLine()
		// fmt.Println(string(str))
		if strings.Contains(string(str), ";") {
			v := strings.Split(string(str), ";")
			city, temp := v[0], v[1]
			tempFloat, err := strconv.ParseFloat(temp, 64)
			if err != nil {
				panic(err)
			}
			ws, ok := weatherStations[city]
			if ok {
				if tempFloat > ws.Max {
					ws.Max = tempFloat
				} else if tempFloat < ws.Min {
					ws.Min = tempFloat
				}

				ws.Count++
				ws.Total += tempFloat
				ws.Avg = ws.Total / float64(ws.Count)
			} else {
				weatherStations[city] = WeatherStations{
					Min:   tempFloat,
					Max:   tempFloat,
					Count: 1,
					Total: tempFloat,
					Avg:   tempFloat,
				}
			}
		}
	}

	keySlice := make([]string, 0, len(weatherStations))

	for city, _ := range weatherStations {
		keySlice = append(keySlice, city)
	}

	sort.Strings(keySlice)

	var sb strings.Builder

	sb.WriteString("{")

	for idx, city := range keySlice {
		sb.WriteString(city)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprint(weatherStations[city].Min))
		sb.WriteString("/")
		sb.WriteString(fmt.Sprint(weatherStations[city].Avg))
		sb.WriteString("/")
		sb.WriteString(fmt.Sprint(weatherStations[city].Max))

		if idx != len(weatherStations)-1 {
			sb.WriteString(", ")
		} else {
			sb.WriteString("}")
		}
	}

	fmt.Println(fmt.Sprintf("content %v, used: %vs", sb.String(), time.Since(start).Seconds()))
}
