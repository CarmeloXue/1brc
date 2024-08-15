package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
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
	line_1    = "../../data/1line.csv"
)

var defaultOffset int64 = 128 // read some more bytes for next line

func main() {
	start := time.Now()
	defer func() {
		fmt.Println("execution time: ", time.Since(start))
	}()

	{
		f, err := os.Create("./profile/cpu.profile")
		if err != nil {
			fmt.Println("cannot create cpu profile: ", err)
			return
		}
		defer f.Close()
		err = pprof.StartCPUProfile(f)
		if err != nil {
			fmt.Println("cannot profile cpu: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// read file
	file, err := os.Open(largeFile)
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
		chunkSizeMB    = 16 * mb
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
				buf := make([]byte, int64(chunkSizeMB)+defaultOffset)
				parsedWS <- parseFileAtOffset(file, buf, int64(offset), int64(chunkSizeMB))
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(parsedWS)
	}()
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
				wholeValue.Avg = wholeValue.Total / float64(wholeValue.Count)
			}
		}
	}

	keySlice := make([]string, 0, len(wholeMap))
	for city, _ := range wholeMap {
		keySlice = append(keySlice, city)
	}
	var sb strings.Builder

	sort.Strings(keySlice)
	for idx, city := range keySlice {
		sb.WriteString(city)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprint(wholeMap[city].Min))
		sb.WriteString("/")
		sb.WriteString(fmt.Sprint(wholeMap[city].Avg))
		sb.WriteString("/")
		sb.WriteString(fmt.Sprint(wholeMap[city].Max))

		if idx != len(wholeMap)-1 {
			sb.WriteString(", ")
		} else {
			sb.WriteString("}")
		}
	}
}

func parseFileAtOffset(file *os.File, buf []byte, offset int64, chunkSize int64) map[string]WeatherStations {
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("read from offset: %v has error: %v", offset, err))
	}

	if len(buf) == 0 {
		return map[string]WeatherStations{}
	}
	var start, end int64 // start and end determins a part of content to be parsed
	// skip first line for none first chunk
	if offset != 0 {
		for start < int64(n) {
			ch := buf[start]
			if ch == '\n' {
				start++
				break
			}
			start++
		}
	}

	end = start

	var (
		isCity     = true
		wsMap      = make(map[string]WeatherStations, 10000)
		cityName   string
		tempString string
	)

	for {
		if isCity {
			// for loop to read chars to get a city name, stop when meet ';' and set value flag
			for end < int64(n) {
				if buf[end] == ';' {
					cityName = fastBytesToString(buf[start : end+1])
					isCity = false
					end++
					start = end
					break
				}
				end++
			}
		} else {
			// for loop to read chars to get a temperature, stop when meet '\n'
			for end < int64(n) {
				ch := buf[end]
				if ch == '\n' {
					// eol, need to calculate
					tempString = fastBytesToString(buf[start:end])
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
					end++
					start = end
					break
				}
				end++
			}
		}

		if (isCity && start >= chunkSize) || end >= int64(n) {
			break
		}
	}

	return wsMap
}

func fastBytesToString(b []byte) string {
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	stringHeader := reflect.StringHeader{
		Data: sliceHeader.Data,
		Len:  sliceHeader.Len,
	}

	return *(*string)(unsafe.Pointer(&stringHeader))
}

func simpleProcess(file *os.File) []string {
	var (
		err             error
		str             []byte
		weatherStations = make(map[string]WeatherStations)
	)

	reader := bufio.NewReader(file)

	for err == nil {
		str, _, err = reader.ReadLine()
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

	fmt.Println(fmt.Sprintf("counts: %v\n", len(keySlice)))
	return keySlice
}
