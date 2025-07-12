import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string

def generate_complex_sales_data():
    """Generate complex sales dataset with 50K rows and nested relationships"""
    np.random.seed(42)
    random.seed(42)
    
    n_rows = 50000
    
    # Generate base data
    transaction_ids = range(1, n_rows + 1)
    customer_ids = np.random.randint(1000, 9999, n_rows)
    product_ids = np.random.randint(100, 999, n_rows)
    
    # Product categories with weights
    categories = ['Electronics', 'Furniture', 'Clothing', 'Books', 'Sports', 'Home', 'Beauty', 'Toys']
    subcategories = {
        'Electronics': ['Phones', 'Laptops', 'Audio', 'Gaming', 'Accessories'],
        'Furniture': ['Living Room', 'Bedroom', 'Office', 'Outdoor', 'Storage'],
        'Clothing': ['Men', 'Women', 'Kids', 'Shoes', 'Accessories'],
        'Books': ['Fiction', 'Non-Fiction', 'Academic', 'Children', 'Comics'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter'],
        'Home': ['Kitchen', 'Bathroom', 'Garden', 'Cleaning', 'Decor'],
        'Beauty': ['Skincare', 'Makeup', 'Hair', 'Fragrance', 'Tools'],
        'Toys': ['Educational', 'Action', 'Dolls', 'Games', 'Electronic']
    }
    
    # Generate categorical data
    selected_categories = np.random.choice(categories, n_rows, p=[0.25, 0.15, 0.15, 0.1, 0.1, 0.1, 0.08, 0.07])
    selected_subcategories = [np.random.choice(subcategories[cat]) for cat in selected_categories]
    
    # Generate prices with category-based distribution
    price_ranges = {
        'Electronics': (50, 2000),
        'Furniture': (100, 1500),
        'Clothing': (20, 300),
        'Books': (10, 100),
        'Sports': (25, 800),
        'Home': (15, 500),
        'Beauty': (10, 200),
        'Toys': (10, 150)
    }
    
    prices = []
    for cat in selected_categories:
        min_price, max_price = price_ranges[cat]
        price = np.random.exponential(scale=(max_price - min_price) / 3) + min_price
        prices.append(min(price, max_price))
    
    # Generate other fields
    quantities = np.random.poisson(2, n_rows) + 1
    discounts = np.random.choice([0, 0.05, 0.1, 0.15, 0.2, 0.25], n_rows, p=[0.6, 0.15, 0.1, 0.08, 0.05, 0.02])
    
    # Calculate totals
    totals = np.array(prices) * quantities * (1 - discounts)
    
    # Generate dates
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=np.random.randint(0, 365)) for _ in range(n_rows)]
    
    # Generate locations
    locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
                'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville']
    selected_locations = np.random.choice(locations, n_rows)
    
    # Generate customer demographics
    customer_ages = np.random.normal(35, 12, n_rows).astype(int)
    customer_ages = np.clip(customer_ages, 18, 80)
    
    customer_incomes = np.random.lognormal(10.5, 0.5, n_rows).astype(int)
    customer_segments = np.random.choice(['Premium', 'Standard', 'Budget'], n_rows, p=[0.2, 0.5, 0.3])
    
    # Payment methods
    payment_methods = np.random.choice(['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet'], 
                                     n_rows, p=[0.5, 0.3, 0.1, 0.1])
    
    # Generate product ratings and reviews
    ratings = np.random.choice([1, 2, 3, 4, 5], n_rows, p=[0.05, 0.1, 0.15, 0.35, 0.35])
    review_counts = np.random.poisson(10, n_rows)
    
    # Create DataFrame
    df = pd.DataFrame({
        'transaction_id': transaction_ids,
        'customer_id': customer_ids,
        'product_id': product_ids,
        'category': selected_categories,
        'subcategory': selected_subcategories,
        'price': np.round(prices, 2),
        'quantity': quantities,
        'discount': discounts,
        'total': np.round(totals, 2),
        'payment_method': payment_methods,
        'store_location': selected_locations,
        'transaction_date': dates,
        'customer_age': customer_ages,
        'customer_income': customer_incomes,
        'customer_segment': customer_segments,
        'product_rating': ratings,
        'review_count': review_counts
    })
    
    return df

def generate_wide_dataset():
    """Generate wide dataset with 5K rows and 50+ columns"""
    np.random.seed(42)
    
    n_rows = 5000
    n_metrics = 30
    
    # Base customer data
    df = pd.DataFrame({
        'customer_id': range(1, n_rows + 1),
        'age': np.random.randint(18, 80, n_rows),
        'gender': np.random.choice(['M', 'F', 'O'], n_rows),
        'city': np.random.choice(['NYC', 'LA', 'CHI', 'HOU', 'PHX'] * 20, n_rows),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'] * 20, n_rows),
        'income': np.random.normal(50000, 20000, n_rows).astype(int),
        'education': np.random.choice(['HS', 'College', 'Graduate'], n_rows),
        'marital_status': np.random.choice(['Single', 'Married', 'Divorced'], n_rows),
        'employment': np.random.choice(['FT', 'PT', 'Self', 'Unemployed'], n_rows)
    })
    
    # Add behavioral metrics (30 columns)
    for i in range(n_metrics):
        df[f'metric_{i+1}'] = np.random.exponential(scale=10, size=n_rows)
    
    # Add purchase history columns
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home', 'Beauty', 'Auto', 'Travel', 'Health']
    for cat in categories:
        df[f'{cat.lower()}_purchases'] = np.random.poisson(5, n_rows)
        df[f'{cat.lower()}_spending'] = np.random.exponential(scale=100, size=n_rows)
        df[f'{cat.lower()}_avg_rating'] = np.random.uniform(1, 5, n_rows)
    
    # Add time-based columns
    for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']:
        df[f'purchases_{month.lower()}'] = np.random.poisson(2, n_rows)
    
    return df

def generate_timeseries_dataset():
    """Generate time-series dataset with 25K rows and temporal patterns"""
    np.random.seed(42)
    
    # Generate date range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = pd.date_range(start_date, end_date, freq='H')
    
    # Sample 25K random timestamps
    selected_dates = np.random.choice(date_range, 25000, replace=True)
    selected_dates = sorted(selected_dates)
    
    df = pd.DataFrame({
        'timestamp': selected_dates,
        'sensor_id': np.random.randint(1, 100, 25000),
        'location_id': np.random.randint(1, 20, 25000),
        'temperature': np.random.normal(22, 5, 25000),
        'humidity': np.random.normal(45, 15, 25000),
        'pressure': np.random.normal(1013, 10, 25000),
        'wind_speed': np.random.exponential(scale=10, size=25000),
        'wind_direction': np.random.uniform(0, 360, 25000),
        'precipitation': np.random.exponential(scale=2, size=25000),
        'visibility': np.random.normal(10, 3, 25000),
        'uv_index': np.random.uniform(0, 11, 25000),
        'air_quality': np.random.uniform(0, 300, 25000)
    })
    
    # Add seasonal patterns
    df['day_of_year'] = df['timestamp'].dt.dayofyear
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    
    # Add derived metrics
    df['heat_index'] = df['temperature'] + 0.5 * df['humidity']
    df['comfort_index'] = 100 - abs(df['temperature'] - 22) - abs(df['humidity'] - 50)
    df['weather_score'] = (df['visibility'] * df['comfort_index']) / (df['wind_speed'] + 1)
    
    return df

def main():
    """Generate all sample datasets"""
    
    # Generate complex sales data
    print("Generating complex sales dataset...")
    complex_df = generate_complex_sales_data()
    complex_df.to_csv('../../sample_datasets/complex_sales.csv', index=False)
    print(f"Complex sales dataset: {len(complex_df)} rows, {len(complex_df.columns)} columns")
    
    # Generate wide dataset
    print("Generating wide dataset...")
    wide_df = generate_wide_dataset()
    wide_df.to_csv('../../sample_datasets/wide_customer_data.csv', index=False)
    print(f"Wide dataset: {len(wide_df)} rows, {len(wide_df.columns)} columns")
    
    # Generate time-series dataset
    print("Generating time-series dataset...")
    ts_df = generate_timeseries_dataset()
    ts_df.to_csv('../../sample_datasets/timeseries_sensors.csv', index=False)
    print(f"Time-series dataset: {len(ts_df)} rows, {len(ts_df.columns)} columns")
    
    print("All datasets generated successfully!")

if __name__ == "__main__":
    main()