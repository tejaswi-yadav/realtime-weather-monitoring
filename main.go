package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type ForecastResponse struct {
	City struct {
		Name    string `json:"name"`
		Country string `json:"country"`
	} `json:"city"`
	List []struct {
		Dt   int64 `json:"dt"`
		Main struct {
			Temp      float64 `json:"temp"`
			Humidity  float64 `json:"humidity"`
			Pressure  float64 `json:"pressure"`
		} `json:"main"`
		Wind struct {
			Speed float64 `json:"speed"`
		} `json:"wind"`
		Weather []struct {
			Description string `json:"description"`
		} `json:"weather"`
	} `json:"list"`
}

type ForecastResult struct {
	ZIP    string
	Output string
	Err    error
}

type KafkaMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	Forecast    []DayForecast `json:"forecast"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

type DayForecast struct {
	Date        string  `json:"date"`
	AvgTemp     float64 `json:"avg_temp"`
	Description string  `json:"description"`
}

// New message types for different topics
type TemperatureMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	Temperature float64   `json:"temperature"`
	Date        string    `json:"date"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

type HumidityMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	Humidity    float64   `json:"humidity"`
	Date        string    `json:"date"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

type WeatherConditionMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	Condition   string    `json:"condition"`
	Date        string    `json:"date"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

type WindSpeedMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	WindSpeed   float64   `json:"wind_speed"`
	Date        string    `json:"date"`
	Time        string    `json:"time"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

type PressureMessage struct {
	ZIPCode     string    `json:"zip_code"`
	City        string    `json:"city"`
	Country     string    `json:"country"`
	Pressure    float64   `json:"pressure"`
	Date        string    `json:"date"`
	Time        string    `json:"time"`
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
}

func FetchForecast(zip string, days int, apiKey string, ch chan<- ForecastResult, wg *sync.WaitGroup, kafkaWriters map[string]*kafka.Writer, requestID string, alertConfig map[string]float64) {
	defer wg.Done()

	url := fmt.Sprintf("http://api.openweathermap.org/data/2.5/forecast?zip=%s,us&units=metric&appid=%s", zip, apiKey)
	resp, err := http.Get(url)
	if err != nil {
		ch <- ForecastResult{ZIP: zip, Err: err}
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var data ForecastResponse
	if err := json.Unmarshal(body, &data); err != nil {
		ch <- ForecastResult{ZIP: zip, Err: err}
		return
	}

	// Send to multiple Kafka topics
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Process ALL 3-hour forecast intervals for proper time-series graphs
	// Also aggregate daily averages for summary output
	dailyMap := make(map[string][]float64)
	output := fmt.Sprintf("Forecast for %s, %s:\n", data.City.Name, data.City.Country)
	var forecastData []DayForecast
	
	// Calculate cutoff time (days * 24 hours from now)
	cutoffTime := time.Now().Add(time.Duration(days) * 24 * time.Hour)
	
	// Process each 3-hour forecast interval
	for _, entry := range data.List {
		entryTime := time.Unix(entry.Dt, 0)
		
		// Skip if beyond requested days
		if entryTime.After(cutoffTime) {
			continue
		}
		
		date := entryTime.Format("2006-01-02")
		timeStr := entryTime.Format("15:04")
		
		// Aggregate for daily averages (for summary output)
		dailyMap[date] = append(dailyMap[date], entry.Main.Temp)
		
		// Extract weather description
		description := "clear sky"
		if len(entry.Weather) > 0 {
			description = entry.Weather[0].Description
		}
		
		// Send temperature data (each 3-hour interval)
		if writer, exists := kafkaWriters["temperature-data"]; exists {
			tempMsg := TemperatureMessage{
				ZIPCode:     zip,
				City:        data.City.Name,
				Country:     data.City.Country,
				Temperature: entry.Main.Temp,
				Date:        date,
				Timestamp:   entryTime,
				RequestID:   requestID,
			}
			
			msgBytes, _ := json.Marshal(tempMsg)
			writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(zip),
				Value: msgBytes,
			})
		}
		
		// Send humidity data (each 3-hour interval)
		if writer, exists := kafkaWriters["humidity-data"]; exists {
			humidityMsg := HumidityMessage{
				ZIPCode:     zip,
				City:        data.City.Name,
				Country:     data.City.Country,
				Humidity:    entry.Main.Humidity,
				Date:        date,
				Timestamp:   entryTime,
				RequestID:   requestID,
			}
			
			msgBytes, _ := json.Marshal(humidityMsg)
			writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(zip),
				Value: msgBytes,
			})
		}
		
		// Send wind speed data (each 3-hour interval)
		if writer, exists := kafkaWriters["wind-data"]; exists {
			windMsg := WindSpeedMessage{
				ZIPCode:     zip,
				City:        data.City.Name,
				Country:     data.City.Country,
				WindSpeed:   entry.Wind.Speed,
				Date:        date,
				Time:        timeStr,
				Timestamp:   entryTime,
				RequestID:   requestID,
			}
			
			msgBytes, _ := json.Marshal(windMsg)
			writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(zip),
				Value: msgBytes,
			})
		}
		
		// Send pressure data (each 3-hour interval)
		if writer, exists := kafkaWriters["pressure-data"]; exists {
			pressureMsg := PressureMessage{
				ZIPCode:     zip,
				City:        data.City.Name,
				Country:     data.City.Country,
				Pressure:    entry.Main.Pressure,
				Date:        date,
				Time:        timeStr,
				Timestamp:   entryTime,
				RequestID:   requestID,
			}
			
			msgBytes, _ := json.Marshal(pressureMsg)
			writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(zip),
				Value: msgBytes,
			})
		}
		
		// Send weather conditions
		if writer, exists := kafkaWriters["weather-conditions"]; exists {
			conditionMsg := WeatherConditionMessage{
				ZIPCode:     zip,
				City:        data.City.Name,
				Country:     data.City.Country,
				Condition:   description,
				Date:        date,
				Timestamp:   entryTime,
				RequestID:   requestID,
			}
			
			msgBytes, _ := json.Marshal(conditionMsg)
			writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(zip),
				Value: msgBytes,
			})
		}
	}
	
	// Build summary output with daily averages
	dates := make([]string, 0, len(dailyMap))
	for date := range dailyMap {
		dates = append(dates, date)
	}
	// Sort dates chronologically
	timeLayout := "2006-01-02"
	for i := 0; i < len(dates)-1; i++ {
		for j := i + 1; j < len(dates); j++ {
			ti, _ := time.Parse(timeLayout, dates[i])
			tj, _ := time.Parse(timeLayout, dates[j])
			if ti.After(tj) {
				dates[i], dates[j] = dates[j], dates[i]
			}
		}
	}
	
	count := 0
	for _, date := range dates {
		if count >= days {
			break
		}
		temps := dailyMap[date]
		sum := 0.0
		for _, t := range temps {
			sum += t
		}
		avg := sum / float64(len(temps))
		output += fmt.Sprintf("%s: avg %.1f°C\n", date, avg)
		
		forecastData = append(forecastData, DayForecast{
			Date:        date,
			AvgTemp:     avg,
			Description: "forecast",
		})
		count++
	}

	// Send complete forecast to weather-forecasts topic asynchronously
	// This prevents blocking and timeout errors since it's not critical for graphs
	kafkaMsg := KafkaMessage{
		ZIPCode:   zip,
		City:      data.City.Name,
		Country:   data.City.Country,
		Forecast:  forecastData,
		Timestamp: time.Now(),
		RequestID: requestID,
	}

	if writer, exists := kafkaWriters["weather-forecasts"]; exists {
		msgBytes, err := json.Marshal(kafkaMsg)
		if err != nil {
			log.Printf("Error marshaling weather forecast message for %s: %v", zip, err)
		} else {
			// Send asynchronously in a goroutine to avoid blocking
			go func(w *kafka.Writer, key string, value []byte, zipCode string) {
				summaryCtx, summaryCancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer summaryCancel()
				
				err := w.WriteMessages(summaryCtx, kafka.Message{
					Key:   []byte(key),
					Value: value,
				})
				if err != nil {
					log.Printf("Error sending weather forecast to Kafka for %s: %v", zipCode, err)
				} else {
					log.Printf("Successfully sent weather forecast for %s to Kafka", zipCode)
				}
			}(writer, zip, msgBytes, zip)
		}
	}


	ch <- ForecastResult{ZIP: zip, Output: output}
}

func main() {
	apiKey := os.Getenv("OPENWEATHER_API_KEY")
	if apiKey == "" {
		fmt.Println("Error: OPENWEATHER_API_KEY not set")
		return
	}

	// Initialize multiple Kafka writers
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092" // Default Kafka broker
	}

	// Define topics
	topics := []string{
		"weather-forecasts",
		"temperature-data", 
		"humidity-data",
		"wind-data",
		"pressure-data",
		"weather-conditions",
	}

	// Create Kafka writers for each topic
	kafkaWriters := make(map[string]*kafka.Writer)
	
	// Test Kafka connection first
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	conn, err := kafka.DialLeader(ctx, "tcp", kafkaBroker, "weather-forecasts", 0)
	if err != nil {
		log.Printf("Warning: Could not connect to Kafka broker at %s: %v", kafkaBroker, err)
		log.Println("Continuing without Kafka...")
	} else {
		conn.Close()
		log.Printf("Connected to Kafka broker at %s", kafkaBroker)
		
		// Create writers for each topic
		for _, topic := range topics {
			writer := &kafka.Writer{
				Addr:     kafka.TCP(kafkaBroker),
				Topic:    topic,
				Balancer: &kafka.LeastBytes{},
			}
			kafkaWriters[topic] = writer
			log.Printf("Created Kafka writer for topic: %s", topic)
		}
	}

	// Close all writers when done
	defer func() {
		for _, writer := range kafkaWriters {
			writer.Close()
		}
	}()

	var input string
	fmt.Print("Enter ZIP codes (comma separated): ")
	fmt.Scanln(&input)
	
	// Clean input: remove control characters and trim
	cleaned := strings.Map(func(r rune) rune {
		if r >= 32 && r != 127 { // Keep printable ASCII except DEL
			return r
		}
		return -1 // Remove control characters
	}, input)
	
	zips := strings.Split(cleaned, ",")
	validZips := make([]string, 0)
	for i := range zips {
		zip := strings.TrimSpace(zips[i])
		// Validate ZIP code (US ZIP codes are 5 digits)
		if len(zip) == 5 {
			valid := true
			for _, c := range zip {
				if c < '0' || c > '9' {
					valid = false
					break
				}
			}
			if valid {
				validZips = append(validZips, zip)
			} else {
				fmt.Printf("Warning: Invalid ZIP code '%s' (must be 5 digits), skipping...\n", zip)
			}
		} else if zip != "" {
			fmt.Printf("Warning: Invalid ZIP code '%s' (must be 5 digits), skipping...\n", zip)
		}
	}
	
	if len(validZips) == 0 {
		fmt.Println("Error: No valid ZIP codes entered. Please enter at least one 5-digit ZIP code.")
		return
	}
	
	zips = validZips

	var days int
	fmt.Print("Enter number of forecast days: ")
	fmt.Scanln(&days)
		if days <= 0 {
		fmt.Println("Error: Number of days must be atleast 1")
		return
	}
	//if days > 5 {
	//	days = 5
	//}

	// Get alert thresholds from user
	fmt.Println("\n=== Alert Configuration ===")
	var highTempThreshold float64
	fmt.Print("Enter high temperature alert threshold (e.g., 30): ")
	fmt.Scanln(&highTempThreshold)
	
	var lowTempThreshold float64
	fmt.Print("Enter low temperature alert threshold (e.g., 0): ")
	fmt.Scanln(&lowTempThreshold)
	
	var highHumidityThreshold float64
	fmt.Print("Enter high humidity alert threshold (e.g., 80): ")
	fmt.Scanln(&highHumidityThreshold)
	
	var lowHumidityThreshold float64
	fmt.Print("Enter low humidity alert threshold (e.g., 20): ")
	fmt.Scanln(&lowHumidityThreshold)
	
	var highWindThreshold float64
	fmt.Print("Enter high wind speed alert threshold in m/s (e.g., 15): ")
	fmt.Scanln(&highWindThreshold)
	
	var highPressureThreshold float64
	fmt.Print("Enter high pressure alert threshold in hPa (e.g., 1050): ")
	fmt.Scanln(&highPressureThreshold)
	
	var lowPressureThreshold float64
	fmt.Print("Enter low pressure alert threshold in hPa (e.g., 980): ")
	fmt.Scanln(&lowPressureThreshold)
	
	// Validate pressure thresholds (low should be less than high)
	if lowPressureThreshold >= highPressureThreshold {
		fmt.Printf("Warning: Low pressure threshold (%.2f) should be less than high pressure threshold (%.2f). Swapping values.\n", 
			lowPressureThreshold, highPressureThreshold)
		lowPressureThreshold, highPressureThreshold = highPressureThreshold, lowPressureThreshold
	}

	ch := make(chan ForecastResult)
	var wg sync.WaitGroup
	requestID := fmt.Sprintf("req_%d", time.Now().Unix())

	// Send alert configuration to Kafka for evaluator
	alertConfigMsg := map[string]interface{}{
		"request_id":              requestID,
		"high_temp_threshold":     highTempThreshold,
		"low_temp_threshold":      lowTempThreshold,
		"high_humidity_threshold": highHumidityThreshold,
		"low_humidity_threshold":  lowHumidityThreshold,
		"high_wind_threshold":     highWindThreshold,
		"high_pressure_threshold": highPressureThreshold,
		"low_pressure_threshold":  lowPressureThreshold,
		"timestamp":               time.Now(),
	}
	
	alertConfigBytes, _ := json.Marshal(alertConfigMsg)
	if len(kafkaWriters) > 0 {
		// Create alert-config writer
		alertConfigWriter := &kafka.Writer{
			Addr:     kafka.TCP(kafkaBroker),
			Topic:    "alert-config",
			Balancer: &kafka.LeastBytes{},
		}
		defer alertConfigWriter.Close()
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		err := alertConfigWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(requestID),
			Value: alertConfigBytes,
		})
		if err != nil {
			log.Printf("Error sending alert config: %v", err)
		} else {
			log.Printf("Alert config sent: High Temp=%.2f, Low Temp=%.2f, High Hum=%.2f, Low Hum=%.2f, High Wind=%.2f, High Pressure=%.2f, Low Pressure=%.2f",
				highTempThreshold, lowTempThreshold, highHumidityThreshold, lowHumidityThreshold, highWindThreshold, highPressureThreshold, lowPressureThreshold)
		}
	}

	alertConfig := map[string]float64{
		"high_temp":      highTempThreshold,
		"low_temp":       lowTempThreshold,
		"high_humidity":  highHumidityThreshold,
		"low_humidity":   lowHumidityThreshold,
		"high_wind":      highWindThreshold,
		"high_pressure": highPressureThreshold,
		"low_pressure":   lowPressureThreshold,
	}

	for _, zip := range zips {
		wg.Add(1)
		go FetchForecast(zip, days, apiKey, ch, &wg, kafkaWriters, requestID, alertConfig)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for res := range ch {
		if res.Err != nil {
			fmt.Printf("Error fetching %s: %v\n", res.ZIP, res.Err)
		} else {
			fmt.Println(res.Output)
		}
	}
}
