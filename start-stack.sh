#!/bin/bash

echo "🚀 Starting Weather Monitoring Stack with Prometheus & Alert Manager"
echo "================================================================"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ .env file not found. Please create one with your OPENWEATHER_API_KEY"
    echo "Example:"
    echo "OPENWEATHER_API_KEY=your_api_key_here"
    exit 1
fi

echo "✅ Environment file found"

# Build and start all services
echo "🔨 Building and starting services..."
docker-compose up --build -d

echo ""
echo "⏳ Waiting for services to start..."
sleep 30

echo ""
echo "📊 Service Status:"
echo "=================="
docker-compose ps

echo ""
echo "🌐 Access URLs:"
echo "=============="
echo "• Prometheus UI:     http://localhost:9090"
echo "• Alert Manager:     http://localhost:9093"
echo "• Kafka UI:          http://localhost:8080"
echo "• Weather Consumer:  http://localhost:8080/metrics"
echo "• Weather Consumer:  http://localhost:8080/health"

echo ""
echo "🧪 Testing Prometheus Scraping:"
echo "==============================="
echo "Checking if Prometheus can scrape weather-consumer metrics..."

# Wait a bit more for services to be ready
sleep 10

# Test Prometheus scraping
curl -s http://localhost:9090/api/v1/targets | grep -q "weather-consumer" && echo "✅ Prometheus can reach weather-consumer" || echo "❌ Prometheus cannot reach weather-consumer"

echo ""
echo "📈 To test the complete pipeline:"
echo "================================="
echo "1. Run weather requests: docker exec -it weather-app ./weather-app"
echo "2. Check metrics: curl http://localhost:8080/metrics"
echo "3. View in Prometheus: http://localhost:9090"
echo "4. Check alerts: http://localhost:9093"

echo ""
echo "🎯 Next Steps:"
echo "============="
echo "1. Add Grafana service for visualization"
echo "2. Create weather dashboards"
echo "3. Test alerting with extreme weather data"
