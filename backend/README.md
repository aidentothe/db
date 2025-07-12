# Apache Spark + Next.js Proof of Concept

This project demonstrates Apache Spark capabilities through a modern web interface built with Next.js and React.

## Features

1. **Word Count** - Demonstrates Spark RDD operations for text processing
2. **Data Analysis** - Shows DataFrame operations and aggregations
3. **SQL Queries** - Executes Spark SQL queries on structured data
4. **Machine Learning** - K-Means clustering using MLlib

## Architecture

- **Frontend**: Next.js 14 with TypeScript and Tailwind CSS
- **Backend**: Flask API with PySpark
- **Processing**: Apache Spark for distributed data processing

## Setup Instructions

### Backend Setup

1. Navigate to the backend directory:
```bash
cd backend
```

2. Activate the Python virtual environment:
```bash
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Start the Flask server:
```bash
python app.py
```

The backend will run on http://localhost:5000

### Frontend Setup

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Start the development server:
```bash
npm run dev
```

The frontend will run on http://localhost:3000

## Usage

1. Open http://localhost:3000 in your browser
2. Navigate through the different tabs to explore Spark features:
   - **Word Count**: Enter text to count word frequencies
   - **Data Analysis**: View statistics on sample sales data
   - **SQL Query**: Execute SQL queries on the data
   - **Machine Learning**: Run K-Means clustering on customer data

## API Endpoints

- `POST /api/word-count` - Performs word count on text
- `POST /api/analyze-csv` - Analyzes structured data
- `POST /api/sql-query` - Executes SQL queries
- `POST /api/machine-learning/kmeans` - Runs K-Means clustering

## Requirements

- Python 3.8+
- Node.js 18+
- Java 8+ (for Spark)

## Technologies Used

- Apache Spark 4.0.0
- Next.js 14
- React 18
- TypeScript
- Tailwind CSS
- Flask
- PySpark