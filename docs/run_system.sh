#!/bin/bash
# IoT Database System Runner Script
# Quick start script for CLI and web interfaces

echo "🏭 IoT Database Query Interface"
echo "================================"

# Check if database exists
if [ ! -f "iot_production.db" ]; then
    echo "📊 Database not found. Creating database with sample data..."
    python3 iot_database_setup.py
    if [ $? -eq 0 ]; then
        echo "✅ Database created successfully!"
    else
        echo "❌ Failed to create database"
        exit 1
    fi
else
    echo "✅ Database found"
fi

echo ""
echo "Choose an interface:"
echo "1. CLI Interface (Interactive terminal)"
echo "2. Web Interface (Streamlit)"  
echo "3. Test System"
echo "4. Exit"
echo ""

read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        echo "🖥️  Starting CLI Interface..."
        python3 iot_cli.py
        ;;
    2)
        echo "🌐 Starting Web Interface..."
        echo "Web interface will be available at: http://localhost:8501"
        streamlit run iot_streamlit_app.py
        ;;
    3)
        echo "🧪 Running System Tests..."
        python3 test_frontends.py
        ;;
    4)
        echo "👋 Goodbye!"
        exit 0
        ;;
    *)
        echo "❌ Invalid choice. Please run the script again."
        exit 1
        ;;
esac