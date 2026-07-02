package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type AlertConfig struct {
	RequestID              string    `json:"request_id"`
	HighTempThreshold      float64   `json:"high_temp_threshold"`
	LowTempThreshold       float64   `json:"low_temp_threshold"`
	HighHumidityThreshold  float64   `json:"high_humidity_threshold"`
	LowHumidityThreshold   float64   `json:"low_humidity_threshold"`
	HighWindThreshold      float64   `json:"high_wind_threshold"`
	HighPressureThreshold  float64   `json:"high_pressure_threshold"`
	LowPressureThreshold   float64   `json:"low_pressure_threshold"`
	Timestamp              time.Time `json:"timestamp"`
}

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

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	log.Printf("Starting alert evaluator...")
	log.Printf("Kafka broker: %s", kafkaBroker)

	// Read from alert-config topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "alert-config",
		GroupID: "alert-evaluator",
	})

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading alert config: %v", err)
			continue
		}

		var alertConfig AlertConfig
		if err := json.Unmarshal(msg.Value, &alertConfig); err != nil {
			log.Printf("Error unmarshaling alert config: %v", err)
			continue
		}

		log.Printf("Received alert configuration for request %s", alertConfig.RequestID)
		log.Printf("High Temp: %.2f, Low Temp: %.2f", alertConfig.HighTempThreshold, alertConfig.LowTempThreshold)
		log.Printf("High Humidity: %.2f, Low Humidity: %.2f", alertConfig.HighHumidityThreshold, alertConfig.LowHumidityThreshold)
		log.Printf("High Wind: %.2f, High Pressure: %.2f, Low Pressure: %.2f", alertConfig.HighWindThreshold, alertConfig.HighPressureThreshold, alertConfig.LowPressureThreshold)

		// Start evaluating temperature alerts
		go evaluateTemperatureAlerts(kafkaBroker, alertConfig)
		
		// Start evaluating humidity alerts
		go evaluateHumidityAlerts(kafkaBroker, alertConfig)
		
		// Start evaluating wind alerts
		go evaluateWindAlerts(kafkaBroker, alertConfig)
		
		// Start evaluating pressure alerts
		go evaluatePressureAlerts(kafkaBroker, alertConfig)
	}
}

func evaluateTemperatureAlerts(broker string, config AlertConfig) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "temperature-data",
		GroupID: "temperature-evaluator-" + config.RequestID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading temperature message: %v", err)
			continue
		}

		var tempMsg TemperatureMessage
		if err := json.Unmarshal(msg.Value, &tempMsg); err != nil {
			continue
		}

		// Check if this message is for our request
		if tempMsg.RequestID != config.RequestID {
			continue
		}

		// Evaluate high temperature alert
		if tempMsg.Temperature > config.HighTempThreshold {
			log.Printf("HIGH TEMP ALERT: %.2f°C in %s (threshold: %.2f°C)", 
				tempMsg.Temperature, tempMsg.City, config.HighTempThreshold)
		}

		// Evaluate low temperature alert
		if tempMsg.Temperature < config.LowTempThreshold {
			log.Printf("LOW TEMP ALERT: %.2f°C in %s (threshold: %.2f°C)", 
				tempMsg.Temperature, tempMsg.City, config.LowTempThreshold)
		}
	}
}

func evaluateHumidityAlerts(broker string, config AlertConfig) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "humidity-data",
		GroupID: "humidity-evaluator-" + config.RequestID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading humidity message: %v", err)
			continue
		}

		var humidityMsg HumidityMessage
		if err := json.Unmarshal(msg.Value, &humidityMsg); err != nil {
			continue
		}

		// Check if this message is for our request
		if humidityMsg.RequestID != config.RequestID {
			continue
		}

		// Evaluate high humidity alert
		if humidityMsg.Humidity > config.HighHumidityThreshold {
			log.Printf("HIGH HUMIDITY ALERT: %.2f%% in %s (threshold: %.2f%%)", 
				humidityMsg.Humidity, humidityMsg.City, config.HighHumidityThreshold)
		}

		// Evaluate low humidity alert
		if humidityMsg.Humidity < config.LowHumidityThreshold {
			log.Printf("LOW HUMIDITY ALERT: %.2f%% in %s (threshold: %.2f%%)", 
				humidityMsg.Humidity, humidityMsg.City, config.LowHumidityThreshold)
		}
	}
}

func evaluateWindAlerts(broker string, config AlertConfig) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "wind-data",
		GroupID: "wind-evaluator-" + config.RequestID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading wind message: %v", err)
			continue
		}

		var windMsg WindSpeedMessage
		if err := json.Unmarshal(msg.Value, &windMsg); err != nil {
			continue
		}

		// Check if this message is for our request
		if windMsg.RequestID != config.RequestID {
			continue
		}

		// Evaluate high wind alert
		if windMsg.WindSpeed > config.HighWindThreshold {
			log.Printf("HIGH WIND ALERT: %.2f m/s in %s (threshold: %.2f m/s)", 
				windMsg.WindSpeed, windMsg.City, config.HighWindThreshold)
		}
	}
}

func evaluatePressureAlerts(broker string, config AlertConfig) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "pressure-data",
		GroupID: "pressure-evaluator-" + config.RequestID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading pressure message: %v", err)
			continue
		}

		var pressureMsg PressureMessage
		if err := json.Unmarshal(msg.Value, &pressureMsg); err != nil {
			continue
		}

		// Check if this message is for our request
		if pressureMsg.RequestID != config.RequestID {
			continue
		}

		// Evaluate high pressure alert
		if pressureMsg.Pressure > config.HighPressureThreshold {
			log.Printf("HIGH PRESSURE ALERT: %.2f hPa in %s (threshold: %.2f hPa)", 
				pressureMsg.Pressure, pressureMsg.City, config.HighPressureThreshold)
		}

		// Evaluate low pressure alert
		if pressureMsg.Pressure < config.LowPressureThreshold {
			log.Printf("LOW PRESSURE ALERT: %.2f hPa in %s (threshold: %.2f hPa)", 
				pressureMsg.Pressure, pressureMsg.City, config.LowPressureThreshold)
		}
	}
}

