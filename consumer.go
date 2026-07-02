package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// Message structures for different topics
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

// Prometheus metrics
var (
	// Temperature metrics
	temperatureGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "weather_temperature_celsius",
			Help: "Current temperature in Celsius",
		},
		[]string{"zip_code", "city", "country", "datetime"},
	)

	// Humidity metrics
	humidityGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "weather_humidity_percent",
			Help: "Current humidity percentage",
		},
		[]string{"zip_code", "city", "country", "datetime"},
	)

	// Weather condition metrics
	weatherConditionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "weather_condition_code",
			Help: "Weather condition code (0=clear, 1=cloudy, 2=rainy, etc.)",
		},
		[]string{"zip_code", "city", "country", "datetime", "condition"},
	)

	// Wind speed metrics
	windSpeedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "weather_wind_speed_ms",
			Help: "Wind speed in meters per second",
		},
		[]string{"zip_code", "city", "country", "datetime"},
	)

	// Pressure metrics
	pressureGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "weather_pressure_hpa",
			Help: "Atmospheric pressure in hectopascals",
		},
		[]string{"zip_code", "city", "country", "datetime"},
	)

	// Message counters
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "weather_messages_processed_total",
			Help: "Total number of weather messages processed",
		},
		[]string{"topic", "zip_code"},
	)

	// Error counter
	messageErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "weather_message_errors_total",
			Help: "Total number of message processing errors",
		},
		[]string{"topic", "error_type"},
	)
)

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(temperatureGauge)
	prometheus.MustRegister(humidityGauge)
	prometheus.MustRegister(weatherConditionGauge)
	prometheus.MustRegister(windSpeedGauge)
	prometheus.MustRegister(pressureGauge)
	prometheus.MustRegister(messagesProcessed)
	prometheus.MustRegister(messageErrors)
}

// Convert weather condition to numeric code
func conditionToCode(condition string) float64 {
	switch condition {
	case "clear sky":
		return 0
	case "few clouds", "scattered clouds":
		return 1
	case "broken clouds", "overcast clouds":
		return 2
	case "light rain", "moderate rain", "heavy rain":
		return 3
	case "snow":
		return 4
	default:
		return 5 // other
	}
}

// Process temperature message
func processTemperatureMessage(msg TemperatureMessage) {
	// Use timestamp to create unique label per 3-hour interval
	datetime := msg.Timestamp.Format("2006-01-02 15:04")
	
	temperatureGauge.WithLabelValues(
		msg.ZIPCode,
		msg.City,
		msg.Country,
		datetime,
	).Set(msg.Temperature)

	messagesProcessed.WithLabelValues("temperature-data", msg.ZIPCode).Inc()
	
	log.Printf("Processed temperature: %s (%s) - %.2f°C on %s", 
		msg.City, msg.ZIPCode, msg.Temperature, datetime)
}

// Process humidity message
func processHumidityMessage(msg HumidityMessage) {
	// Use timestamp to create unique label per 3-hour interval
	datetime := msg.Timestamp.Format("2006-01-02 15:04")
	
	humidityGauge.WithLabelValues(
		msg.ZIPCode,
		msg.City,
		msg.Country,
		datetime,
	).Set(msg.Humidity)

	messagesProcessed.WithLabelValues("humidity-data", msg.ZIPCode).Inc()
	
	log.Printf("Processed humidity: %s (%s) - %.2f%% on %s", 
		msg.City, msg.ZIPCode, msg.Humidity, datetime)
}

// Process weather condition message
func processWeatherConditionMessage(msg WeatherConditionMessage) {
	conditionCode := conditionToCode(msg.Condition)
	// Use timestamp to create unique label per 3-hour interval
	datetime := msg.Timestamp.Format("2006-01-02 15:04")
	
	weatherConditionGauge.WithLabelValues(
		msg.ZIPCode,
		msg.City,
		msg.Country,
		datetime,
		msg.Condition,
	).Set(conditionCode)

	messagesProcessed.WithLabelValues("weather-conditions", msg.ZIPCode).Inc()
	
	log.Printf("Processed condition: %s (%s) - %s on %s", 
		msg.City, msg.ZIPCode, msg.Condition, datetime)
}

// Process wind speed message
func processWindSpeedMessage(msg WindSpeedMessage) {
	// Use timestamp to create unique label per 3-hour interval
	datetime := msg.Timestamp.Format("2006-01-02 15:04")
	
	windSpeedGauge.WithLabelValues(
		msg.ZIPCode,
		msg.City,
		msg.Country,
		datetime,
	).Set(msg.WindSpeed)

	messagesProcessed.WithLabelValues("wind-data", msg.ZIPCode).Inc()
	
	log.Printf("Processed wind speed: %s (%s) - %.2f m/s on %s", 
		msg.City, msg.ZIPCode, msg.WindSpeed, datetime)
}

// Process pressure message
func processPressureMessage(msg PressureMessage) {
	// Use timestamp to create unique label per 3-hour interval
	datetime := msg.Timestamp.Format("2006-01-02 15:04")
	
	pressureGauge.WithLabelValues(
		msg.ZIPCode,
		msg.City,
		msg.Country,
		datetime,
	).Set(msg.Pressure)

	messagesProcessed.WithLabelValues("pressure-data", msg.ZIPCode).Inc()
	
	log.Printf("Processed pressure: %s (%s) - %.2f hPa on %s", 
		msg.City, msg.ZIPCode, msg.Pressure, datetime)
}

// Kafka consumer for a specific topic
func consumeTopic(topic string, reader *kafka.Reader) {
	log.Printf("Starting consumer for topic: %s", topic)
	
	// Track startup phase to suppress transient errors
	startupPhase := true
	startupDeadline := time.Now().Add(60 * time.Second)
	retryCount := 0
	
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			// Check if it's a transient startup error
			errStr := err.Error()
			isTransientError := strings.Contains(errStr, "connection refused") ||
				strings.Contains(errStr, "Rebalance In Progress") ||
				strings.Contains(errStr, "Leader Not Available") ||
				strings.Contains(errStr, "Group Coordinator Not Available")
			
			// Suppress transient errors during startup phase (first 60 seconds)
			if isTransientError && startupPhase && time.Now().Before(startupDeadline) {
				retryCount++
				if retryCount == 1 {
					log.Printf("Waiting for Kafka to be ready for topic %s...", topic)
				}
				time.Sleep(2 * time.Second)
				continue
			}
			
			// Exit startup phase after deadline
			if startupPhase && time.Now().After(startupDeadline) {
				startupPhase = false
				if retryCount > 0 {
					log.Printf("Kafka connection established for topic %s", topic)
				}
			}
			
			// Log non-transient errors or persistent errors after startup
			if !isTransientError {
				log.Printf("Error reading message from %s: %v", topic, err)
			} else if !startupPhase {
				// Transient error after startup - log but don't spam
				log.Printf("Transient error reading from %s (will retry): %v", topic, err)
			}
			
			messageErrors.WithLabelValues(topic, "read_error").Inc()
			time.Sleep(1 * time.Second)
			continue
		}

		// First successful message - exit startup phase
		if startupPhase {
			startupPhase = false
			if retryCount > 0 {
				log.Printf("Kafka ready for topic %s, processing messages", topic)
			}
		}

		// Process message based on topic
		switch topic {
		case "temperature-data":
			var tempMsg TemperatureMessage
			if err := json.Unmarshal(msg.Value, &tempMsg); err != nil {
				log.Printf("Error unmarshaling temperature message: %v", err)
				messageErrors.WithLabelValues(topic, "unmarshal_error").Inc()
				continue
			}
			processTemperatureMessage(tempMsg)

		case "humidity-data":
			var humidityMsg HumidityMessage
			if err := json.Unmarshal(msg.Value, &humidityMsg); err != nil {
				log.Printf("Error unmarshaling humidity message: %v", err)
				messageErrors.WithLabelValues(topic, "unmarshal_error").Inc()
				continue
			}
			processHumidityMessage(humidityMsg)

		case "weather-conditions":
			var conditionMsg WeatherConditionMessage
			if err := json.Unmarshal(msg.Value, &conditionMsg); err != nil {
				log.Printf("Error unmarshaling weather condition message: %v", err)
				messageErrors.WithLabelValues(topic, "unmarshal_error").Inc()
				continue
			}
			processWeatherConditionMessage(conditionMsg)

		case "wind-data":
			var windMsg WindSpeedMessage
			if err := json.Unmarshal(msg.Value, &windMsg); err != nil {
				log.Printf("Error unmarshaling wind speed message: %v", err)
				messageErrors.WithLabelValues(topic, "unmarshal_error").Inc()
				continue
			}
			processWindSpeedMessage(windMsg)

		case "pressure-data":
			var pressureMsg PressureMessage
			if err := json.Unmarshal(msg.Value, &pressureMsg); err != nil {
				log.Printf("Error unmarshaling pressure message: %v", err)
				messageErrors.WithLabelValues(topic, "unmarshal_error").Inc()
				continue
			}
			processPressureMessage(pressureMsg)
		}
		
		// Add a small delay after processing to allow Prometheus to scrape metrics at different times
		// This helps create time-series data points spread over time instead of all at once
		time.Sleep(2 * time.Second)
	}
}

func main() {
	// Get configuration from environment
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "8080"
	}

	log.Printf("Starting weather metrics consumer...")
	log.Printf("Kafka broker: %s", kafkaBroker)
	log.Printf("Metrics port: %s", metricsPort)

	// Create Kafka readers for each topic
	topics := []string{"temperature-data", "humidity-data", "wind-data", "pressure-data", "weather-conditions"}
	readers := make([]*kafka.Reader, len(topics))

	for i, topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{kafkaBroker},
			Topic:   topic,
			GroupID: "weather-metrics-consumer",
		})
		readers[i] = reader
	}

	// Start HTTP server for metrics
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		log.Printf("Starting metrics server on port %s", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Fatalf("Error starting metrics server: %v", err)
		}
	}()

	// Start consumers for each topic
	for i, topic := range topics {
		go consumeTopic(topic, readers[i])
	}

	// Keep the main goroutine alive
	select {}
}
