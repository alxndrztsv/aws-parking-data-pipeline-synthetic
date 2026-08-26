import hashlib
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

fake = Faker()


def get_persistent_park_code(city: str) -> int:
    """
    Uses a deterministic hash for cities.
    """
    hash_digest = hashlib.md5(city.encode('utf-8')).digest()
    return (int.from_bytes(hash_digest[:2], 'big') % 9000) + 1000


def format_duration_h_m(total_seconds: int) -> str:
    """Formats seconds into 'X h Y m' format (e.g., '2 h 15 m')."""
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours} h {minutes} m"


def generate_relevant_raw_data(city: str, start_date: datetime, end_date: datetime, rows: int = 500) -> pd.DataFrame:
    """Generates synthetic raw data with ONLY the relevant columns."""
    data = []
    park_code = get_persistent_park_code(city)

    # Pre generate a pool of terminals for the park
    num_terminals = 50
    terminal_pool = {}
    for i in range(1, num_terminals + 1):
        meter_code = str(i)
        terminal_pool[meter_code] = {
            "METER_DESC": f"{i}-{city}",
            "ADDRESS": fake.street_address(),
            "ZONE_DESC": random.choice(["Yellow", "Red", "Green", "Orange", "Blue"]),
            "CIRCUIT_DESC": random.choice(["On-street", "Off-street"])
        }

    # Generate transactions    
    for _ in range(rows):
        meter_date = fake.date_time_between(start_date=start_date, end_date=end_date)
        server_date = meter_date + timedelta(hours=random.randint(1, 5))
        end_date_txn = meter_date + timedelta(minutes=random.randint(20, 120))
        
        total_duration_sec = random.randint(1800, 7200)  # 30 mins to 2 hours
        paid_duration_sec = total_duration_sec - random.randint(0, 300)

        # Pick a random terminal from the pre-generated pool
        meter_code = str(random.randint(1, num_terminals))
        terminal_info = terminal_pool[meter_code]

        data.append({
            "PARK_CODE": park_code,
            "PARK_NAME": f"IRL_{city}",
            "METER_DESC": terminal_info["METER_DESC"],
            "ADDRESS": terminal_info["ADDRESS"],
            "USER_NUMBER": str(random.choice([1, 2, 3])),
            "END_DATE": end_date_txn.strftime("%d/%m/%Y %H:%M"),
            "FREE_DURATION": random.choice([0, 15, 30]),
            "CURRENCY": "EUR",  # single currency
            "SERVER_DATE": server_date.strftime("%d/%m/%Y %H:%M"),
            "METER_DATE": meter_date.strftime("%d/%m/%Y %H:%M"),
            "METER_CODE": meter_code,
            "AMOUNT": round(random.uniform(1.0, 10.0), 1),
            "PAYMENT_MEAN": random.choice(["COINS", "CARD"]),
            "TOTAL_DURATION": total_duration_sec,
            "PAID_DURATION": paid_duration_sec,
            "SYSTEM_ID": str(random.randint(100000000, 999999999)),
            "PRINTED_ID": str(random.randint(1000, 9999)),
            "ZONE_DESC": terminal_info["ZONE_DESC"],
            "CIRCUIT_DESC": terminal_info["CIRCUIT_DESC"],
            "ORIGIN": "PND",  # only pay and display
            "CARD_TRANS_ID": fake.uuid4()[:12] if random.random() > 0.5 else ""  # sometimes empty
        })
    return pd.DataFrame(data)


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Processes raw data, renames, formats durations, and drops irrelevant columns."""
    df = df.copy()
    
    # Rename columns to clean business names
    df.rename(columns={
        "PAYMENT_MEAN": "Payment Mean",
        "ORIGIN": "Origin",
        "SERVER_DATE": "Server Time",
        "METER_DATE": "Terminal Date",
        "METER_CODE": "Terminal Code",
        "AMOUNT": "Amount",
        "TOTAL_DURATION": "Total Duration",
        "PAID_DURATION": "Paid Duration",
        "SYSTEM_ID": "System ID",
        "PRINTED_ID": "Printed ID",
        "ZONE_DESC": "Zone Desc",
        "CIRCUIT_DESC": "Circuit Desc",
        "PARK_CODE": "Park Code",
        "PARK_NAME": "Park",
        "METER_DESC": "Terminal Description",
        "ADDRESS": "Address",
        "USER_NUMBER": "User Type:",
        "END_DATE": "End Date",
        "FREE_DURATION": "Free Duration:",
        "CURRENCY": "Currency",
        "CARD_TRANS_ID": "Banking id"
    }, inplace=True)

    # Calculate Duration in minutes
    df["Total Duration in mins"] = (df["Total Duration"] // 60).astype(int)
    df["Paid Duration in mins"] = (df["Paid Duration"] // 60).astype(int)

    # Format Duration to "X h Y m"
    df["Total Duration"] = df["Total Duration"].apply(lambda x: format_duration_h_m(x) if pd.notna(x) else None)
    df["Paid Duration"] = df["Paid Duration"].apply(lambda x: format_duration_h_m(x) if pd.notna(x) else None)

    # Clean up Payment Mean (COINS -> Coins)
    df["Payment Mean"] = df["Payment Mean"].replace("COINS", "Coins")

    # Add required empty columns
    df["Type"] = None
    df["Product Name"] = None
    df["User Name"] = None

    # Select and order final relevant columns
    final_columns = [
        "Payment Mean", "Origin", "Server Time", "Terminal Date", "Terminal Code", 
        "Amount", "Total Duration", "Paid Duration", "Total Duration in mins", 
        "Paid Duration in mins", "System ID", "Printed ID", "Zone Desc", "Circuit Desc", 
        "Park Code", "Park", "Terminal Description", "Address", "Type", "User Type:", 
        "End Date", "Free Duration:", "Currency", "Banking id", "Product Name", "User Name"
    ]
    
    # Ensure all columns exist (prevents errors if a column is missing in raw data)
    for col in final_columns:
        if col not in df.columns:
            df[col] = None
            
    return df[final_columns]


def main():
    cities_to_test = ["Dublin", "Cork", "Galway", "Wicklow", "Sligo"] 
    
    end_date = datetime.now(tz=timezone.utc).replace(microsecond=0)
    start_date = end_date - timedelta(days=30)

    os.makedirs("../data/bronze", exist_ok=True)
    os.makedirs("../data/silver", exist_ok=True)

    print(f"Starting streamlined data generation for {len(cities_to_test)} cities...")
    
    for city in cities_to_test:
        print(f"Processing {city}...")
        
        # 1. Generate RAW data (Bronze)
        raw_df = generate_relevant_raw_data(city, start_date, end_date, rows=1000)
        raw_df.to_csv(f"../data/bronze/{city}_raw.csv", index=False)
        
        # 2. Process data (Silver)
        processed_df = process_dataframe(raw_df)
        processed_df.to_csv(f"../data/silver/{city}_processed.csv", index=False)
        
    print("\nStreamlined generation complete! Check 'data/bronze' and 'data/silver'.")


if __name__ == "__main__":
    main()