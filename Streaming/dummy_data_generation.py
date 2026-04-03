# Databricks notebook source
import time
import csv
import os
import random
from datetime import datetime

# -------------------------------------
# CONFIGURATION
# -------------------------------------
REGION = ["Hyderabad","Bangalore","Chennai","Mumbai","Delhi","Lucknow","Gujarat","Rajasthan","Kolkata","Punjab"]                         # Region for file name
BATCH_SIZE = 10                              # Records per file
BASE_PATH = "/Volumes/streaming/realtimestreaming/sourcedata/data_sales/"
COUNTER_PATH = "/Volumes/streaming/realtimestreaming/sourcedata/data_sales/last_counter.txt"

# -------------------------------------
# LOAD PERSISTENT COUNTER (START FROM 1)
# -------------------------------------
if os.path.exists(COUNTER_PATH):
    with open(COUNTER_PATH, "r") as f:
        counter = int(f.read().strip())
else:
    counter = 1  # ✅ start from 1

# Create data directory if missing
os.makedirs(BASE_PATH, exist_ok=True)

# -------------------------------------
# FIXED LOOKUP LISTS (FROM YOUR Sales.csv)
# -------------------------------------
item_types = [
    "Dairy", "Soft Drinks", "Meat", "Fruits and Vegetables", "Household",
    "Snack Foods", "Baking Goods", "Frozen Foods", "Health and Hygiene",
    "Canned", "Breads", "Breakfast", "Starchy Foods", "Seafood", "Hard Drinks",
    "Others"
]

fat_types = ["Low Fat", "Regular", "LF", "reg"]

outlet_ids = ["OUT010", "OUT013", "OUT017", "OUT018", "OUT019",
              "OUT027", "OUT035", "OUT045", "OUT046", "OUT049"]

outlet_sizes = ["Small", "Medium", "High", ""]
outlet_location_types = ["Tier 1", "Tier 2", "Tier 3"]
outlet_types = ["Supermarket Type1", "Supermarket Type2", "Supermarket Type3", "Grocery Store"]

# -------------------------------------
# GENERATION LOOP
# -------------------------------------
while True:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = f"{random.choice(REGION)}_Sales_{today}_{counter}.csv"

    temp_path = f"{BASE_PATH}/tmp_{file_name}"
    final_path = f"{BASE_PATH}/{file_name}"

    with open(temp_path, "w", newline="") as f:
        writer = csv.writer(f)

        # ✅ SAME COLUMNS AS Sales.csv
        writer.writerow([
            "Item_Identifier", "Item_Weight", "Item_Fat_Content",
            "Item_Visibility", "Item_Type", "Item_MRP",
            "Outlet_Identifier", "Outlet_Establishment_Year",
            "Outlet_Size", "Outlet_Location_Type",
            "Outlet_Type", "Item_Outlet_Sales"
        ])

        for _ in range(BATCH_SIZE):

            # Generate clean values
            item_identifier = f"ID{counter:05d}"
            item_weight = round(random.uniform(5.0, 20.0), 2)
            item_fat = random.choice(fat_types)
            item_visibility = round(random.uniform(0, 0.32), 6)
            item_type = random.choice(item_types)
            item_mrp = round(random.uniform(30, 260), 4)
            outlet_id = random.choice(outlet_ids)
            outlet_year = random.choice(range(1985, 2010))
            outlet_size = random.choice(outlet_sizes)
            outlet_loc = random.choice(outlet_location_types)
            outlet_type = random.choice(outlet_types)
            sales = round(random.uniform(50, 12000), 4)

            row = [
                item_identifier, item_weight, item_fat, item_visibility,
                item_type, item_mrp, outlet_id, outlet_year,
                outlet_size, outlet_loc, outlet_type, sales
            ]

            # ✅ RANDOM NULL INJECTION (10% chance)
            if random.random() < 0.10:
                col_to_null = random.randint(0, len(row) - 1)
                row[col_to_null] = ""   # make column empty

            writer.writerow(row)
            counter += 1  # increment sequence across files

        # ✅ Add <eof> at the end of each CSV file
        writer.writerow(["<eof>"])

    # ✅ Atomic rename
    os.rename(temp_path, final_path)

    # ✅ Store updated counter
    with open(COUNTER_PATH, "w") as f:
        f.write(str(counter))

    print(f"Generated: {file_name} ({BATCH_SIZE} rows + <eof>)")

    time.sleep(2)  # new file every 2 seconds

# COMMAND ----------


