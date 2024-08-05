package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WeatherStations struct {
	Min   float64
	Max   float64
	Count int
	Total float64
	Avg   float64
}

var defaultOffset int64 = 100 // read some more bytes for next line

func main() {

	// read file
	file, err := os.Open("../../../../data/weather_stations.csv")
	defer file.Close()
	if err != nil {
		fmt.Println("get errors: %v", err)
	}

	fileState, err := file.Stat()
	if err != nil {
		fmt.Println("get errors: %v", err)
	}
	var (
		processerCount = 100
		chunkSizeMB    = 1024 * 128
		offsetCh       = make(chan int, processerCount)                        // put start point here
		parsedWS       = make(chan map[string]WeatherStations, processerCount) // put parsed data here
	)

	// send start points to offset channel
	go func() {
		i := 0
		fmt.Println("file Size: ", fileState.Size(), "chunk size: ", chunkSizeMB)
		for i < int(fileState.Size()) {
			offsetCh <- i
			i += chunkSizeMB
		}
		close(offsetCh)
	}()

	var wg sync.WaitGroup
	wg.Add(processerCount)

	// create goroutine to parse files
	for i := 0; i < processerCount; i++ {
		go func() {
			for offset := range offsetCh {
				parsedWS <- parseFileAtOffset(file, int64(offset))
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(parsedWS)
	}()

	// merge to a map
	for parsed := range parsedWS {
		fmt.Println(parsed)
	}
	// simpleProcess(file)

}

func parseFileAtOffset(file *os.File, offset int64) map[string]WeatherStations {
	fmt.Println("start at offset: ", offset)
	buf := make([]byte, offset+defaultOffset)

	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("read from offset: %v has error: %v", offset, err))
	}
	fmt.Println("read file length: ", n)
	if len(buf) == 0 {
		return map[string]WeatherStations{}
	}
	var start int64
	// skip first line for none first chunk
	if offset != 0 {
		var ch string
		for ch != "\n" {
			start++
			ch = string(buf[start])
		}
		start++
	}

	var (
		isCity     = true
		wsMap      = make(map[string]WeatherStations)
		sb         strings.Builder
		cityName   string
		tempString string
	)

	// start to process
	for start < int64(n) {
		if isCity {
			// for loop to read chars to get a city name, stop when meet ';' and set value flag
			ch := string(buf[start])
			for ch != ";" {
				sb.WriteString(ch)
				start++
				ch = string(buf[start])
			}

			isCity = false
			cityName = sb.String()
			sb.Reset()
			start++
		} else {
			// for loop to read chars to get a temperature, stop when meet '\n'
			ch := string(buf[start])
			for ch != "\n" {
				sb.WriteString(ch)
				start++
				ch = string(buf[start])
			}
			isCity = true
			tempString = sb.String()
			sb.Reset()
			tempFloat, err := strconv.ParseFloat(tempString, 64)
			if err != nil {
				panic(err)
			}
			// set city to map
			city, ok := wsMap[cityName]
			if !ok {
				wsMap[cityName] = WeatherStations{
					Min:   tempFloat,
					Max:   tempFloat,
					Total: tempFloat,
					Count: 1,
				}
			} else {
				if tempFloat > city.Max {
					city.Max = tempFloat
				} else if tempFloat < city.Min {
					city.Min = tempFloat
				}
				city.Total += tempFloat
				city.Count++
				city.Avg = city.Total / float64(city.Count)
			}

			cityName = ""
			tempString = ""
			start++
		}

	}

	return wsMap
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
