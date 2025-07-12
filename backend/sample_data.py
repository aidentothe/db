# Sample datasets for demonstration

SALES_DATA = [
    {"product": "Laptop", "category": "Electronics", "price": 999.99, "quantity": 5, "date": "2024-01-15"},
    {"product": "Mouse", "category": "Electronics", "price": 29.99, "quantity": 20, "date": "2024-01-15"},
    {"product": "Keyboard", "category": "Electronics", "price": 79.99, "quantity": 15, "date": "2024-01-16"},
    {"product": "Monitor", "category": "Electronics", "price": 299.99, "quantity": 8, "date": "2024-01-16"},
    {"product": "Desk", "category": "Furniture", "price": 199.99, "quantity": 3, "date": "2024-01-17"},
    {"product": "Chair", "category": "Furniture", "price": 149.99, "quantity": 10, "date": "2024-01-17"},
    {"product": "Lamp", "category": "Furniture", "price": 49.99, "quantity": 12, "date": "2024-01-18"},
    {"product": "Notebook", "category": "Stationery", "price": 4.99, "quantity": 100, "date": "2024-01-18"},
    {"product": "Pen", "category": "Stationery", "price": 1.99, "quantity": 200, "date": "2024-01-19"},
    {"product": "Tablet", "category": "Electronics", "price": 599.99, "quantity": 7, "date": "2024-01-19"},
]

CUSTOMER_DATA = [
    {"customer_id": 1, "age": 25, "spending": 1500.00, "visits": 12},
    {"customer_id": 2, "age": 35, "spending": 2500.00, "visits": 20},
    {"customer_id": 3, "age": 28, "spending": 800.00, "visits": 8},
    {"customer_id": 4, "age": 42, "spending": 3200.00, "visits": 25},
    {"customer_id": 5, "age": 31, "spending": 1200.00, "visits": 15},
    {"customer_id": 6, "age": 27, "spending": 900.00, "visits": 10},
    {"customer_id": 7, "age": 45, "spending": 4000.00, "visits": 30},
    {"customer_id": 8, "age": 22, "spending": 600.00, "visits": 5},
    {"customer_id": 9, "age": 38, "spending": 2800.00, "visits": 22},
    {"customer_id": 10, "age": 29, "spending": 1100.00, "visits": 14},
]

SAMPLE_TEXT = """
Apache Spark is a unified analytics engine for large-scale data processing.
It provides high-level APIs in Java, Scala, Python and R, and an optimized engine 
that supports general execution graphs. It also supports a rich set of higher-level 
tools including Spark SQL for SQL and structured data processing, MLlib for machine 
learning, GraphX for graph processing, and Structured Streaming for incremental 
computation and stream processing. Spark runs on Hadoop, Apache Mesos, Kubernetes, 
standalone, or in the cloud. It can access diverse data sources including HDFS, 
Cassandra, HBase, and S3.
"""