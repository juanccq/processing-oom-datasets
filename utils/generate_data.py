import random
import time

# NUM_USERS = 30_000_000
# NUM_TX = 60_000_000
NUM_USERS = 5_000_000
NUM_TX = 10_000_000

cities = ["El Alto", "La Paz", "Santa Cruz", "Cochabamba", "Sucre", "Oruro", "Tarija", "Potosi"]

print(f"1. Generating users.csv ({NUM_USERS} rows)...")
start = time.perf_counter()

with open("users_5.csv", "w") as f:
    f.write("user_id,name,city,signup_channel\n")

    for i in range(1, NUM_USERS + 1):
        city = random.choice(cities)

        f.write(f"{i},User_{i},{city},Organic\n")

print(f"users.csv generated in {time.perf_counter() - start:.2f} seconds.")

print(f"\n2. Generating transaction_logs.json ({NUM_TX} rows)...")
start = time.perf_counter()

with open("transaction_logs_10.json", "w") as f:
    for i in range(1, NUM_TX + 1):
        uid = random.randint(1, NUM_USERS)
        amount = round(random.uniform(10.0, 1500.0), 2)

        f.write(f'{{"tx_id": {i}, "user_id": {uid}, "amount": {amount}, "event": "purchase"}}\n')

print(f"transaction_logs.json generated in {time.perf_counter() - start:.2f} seconds.")

print("\nData generation complete! Run ='ls -lh' to verify sizes.")