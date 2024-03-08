import time
from datetime import datetime
import random
from faker import Faker
import json

fake = Faker()
airline_names = ["Indigo", "AirIndia", "Quatar", "SingaporeAirlines"]
destination_city = ["India", "Amsterdam", "Norway", "Singapore"]
source_city = ["India", "Amsterdam", "Norway", "Singapore"]

def generate_fake_data():
    data = {
        "aircraft_id": fake.uuid4(),
        "airlines_name": random.choice(airline_names),
        "travel": {
            "destination_city": random.choice(destination_city),
            "source_city": random.choice(source_city)
        },
        "aircraftEmergency_phone": fake.phone_number(),
        "aircraft_rating": round(random.uniform(1, 10), 2),
        "price": {
            "INR": round(random.uniform(5000, 20000), 2),
            "USD": round(random.uniform(100, 9000), 2)
        },
        "time": {
            "departure_time": fake.time(pattern="%H:%M:%S"),
            "arrival_time": fake.time(pattern="%H:%M:%S")
        }
    }
    return data

if __name__ == "__main__":
    try:
        curr_time = datetime.now()

        # Number of entries to generate
        num_entries = 100

        with open("AeroplaneData.json", "a") as json_file:
            for _ in range(num_entries):
                generated_data = generate_fake_data()
                json.dump(generated_data, json_file)
                json_file.write('\n')

                # Sleep for 1 second between entries
                time.sleep(1)

        print(f"{num_entries} fake data entries have been generated and appended to AeroplaneData.json.")
    except Exception as e:
        print(f"An error occurred: {e}")
