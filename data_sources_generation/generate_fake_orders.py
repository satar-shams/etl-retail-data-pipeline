from faker import Faker
import pandas as pd
import random
from datetime import date
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
fake = Faker()

def generate_orders(num_rows=1000):
    data = []
    for _ in range(num_rows):
        data.append({
            "order_id": fake.uuid4(),
            "customer_name": fake.name(),
            "product": fake.word(),
            "amount": round(random.uniform(10, 1000), 2),
            "order_date": fake.date_this_year()
        })

    df = pd.DataFrame(data)
    today = date.today().strftime("%Y_%m_%d")
    file_name = f"orders_{today}.csv"
    data_sources_path = os.path.join(script_dir, '..', 'data_sources', file_name)
    #file_path = f"data_sources/{file_name}"

    df.to_csv(data_sources_path, index=False)
    print(f"Fake orders generated: {data_sources_path}")

if __name__ == "__main__":
    generate_orders()
