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

var (
	smallFile = "../../data/weather_stations.csv"
	largeFile = "../../data/measurements.txt"
)

var defaultOffset int64 = 128 // read some more bytes for next line

func main() {
	start := time.Now()
	defer func() {
		fmt.Println("execution time: ", time.Since(start))
	}()

	// read file
	file, err := os.Open(smallFile)
	defer file.Close()
	if err != nil {
		fmt.Printf("get errors: %v\n", err)
	}

	fileState, err := file.Stat()
	if err != nil {
		fmt.Printf("get errors: %v\n", err)
	}
	var (
		mb             = 1024 * 1024
		processerCount = 100
		chunkSizeMB    = 1 * mb / 128
		offsetCh       = make(chan int, processerCount)                        // put start point here
		parsedWS       = make(chan map[string]WeatherStations, processerCount) // put parsed data here
	)

	// send start points to offset channel
	go func() {
		i := 0
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
				parsedWS <- parseFileAtOffset(file, int64(offset), int64(chunkSizeMB))
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(parsedWS)
	}()
	wholeCounts := 0
	// merge to a map
	var wholeMap = map[string]WeatherStations{}
	for parsed := range parsedWS {
		for k, v := range parsed {
			wholeValue, exist := wholeMap[k]
			if !exist {
				wholeMap[k] = v
			} else {
				if v.Min < wholeValue.Min {
					wholeValue.Min = v.Min
				}

				if v.Max > wholeValue.Max {
					wholeValue.Max = v.Max
				}

				wholeValue.Total += v.Total
				wholeValue.Count += v.Count
				// wholeValue.Avg = wholeValue.Total / float64(wholeValue.Count)
			}
		}
	}

	counts := 0

	for _ = range wholeMap {
		counts++
	}

	fmt.Println("total:", counts, "whole counts: ", wholeCounts)

	// simpleProcess(file)

}

func parseFileAtOffset(file *os.File, offset int64, chunkSize int64) map[string]WeatherStations {
	buf := make([]byte, chunkSize+defaultOffset)

	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("read from offset: %v has error: %v", offset, err))
	}

	if n == 100 {
		fmt.Println(string(buf))
	}

	if len(buf) == 0 {
		return map[string]WeatherStations{}
	}
	var start int64
	// skip first line for none first chunk
	if offset != 0 {
		var ch string
		for start < int64(n) {
			ch = string(buf[start])
			if ch == "\n" {
				start++
				break
			}
			start++
		}
	}

	var (
		isCity     = true
		wsMap      = make(map[string]WeatherStations)
		sb         strings.Builder
		cityName   string
		tempString string
	)

	for {
		if isCity {
			// for loop to read chars to get a city name, stop when meet ';' and set value flag
			for start < int64(n) {
				if buf[start] == ';' {
					cityName = sb.String()
					sb.Reset()
					isCity = false
					start++
					break
				} else {
					sb.WriteByte(buf[start])
				}
				start++
			}
		} else {
			// for loop to read chars to get a temperature, stop when meet '\n'
			for start < int64(n) {
				ch := buf[start]

				if ch == '\n' {
					// eol, need to calculate
					tempString = sb.String()
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
						// city.Avg = city.Total / float64(city.Count)
					}
					cityName = ""
					tempString = ""
					isCity = true
					sb.Reset()
					start++
					break
				} else {
					sb.WriteByte(ch)
				}
				start++
			}
		}

		if (isCity && start >= chunkSize) || start >= int64(n) {
			break
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

	counts := 0

	for err == nil {
		str, _, err = reader.ReadLine()
		// fmt.Println(string(str))
		if strings.Contains(string(str), ";") {
			counts++
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

	fmt.Println(fmt.Sprintf("used: %v, counts: %v\n", time.Since(start).Seconds(), counts))
}
