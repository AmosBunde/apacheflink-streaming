from faker import Faker
import random
from datetime import datetime,time
import json
from confluent_kafka import SerializingProducer


fake = Faker()


def generate_sales_transactions():
    user = fake.simple_profile()


    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['productOne','productTwo','productThree','productFour','productFive','productSix']),
        "productName": random.choice(['Phone','MacBook','Wireless Modem','Tablet','Bag Pack','Bose Headphones']),
        "productCategory": random.choice(['Electronic','Fashion','Grocery','Home Appliances','Beauty','Sports']),
        "productPrices": round(random.uniform(10, 100), 2),
        "productQuantity": random.randint(1,10),
        "productBrands": random.choice(['Apple','Samsumg','Nokia','LG','Bose','Sony']),
        "currency": random.choice(['USD','GBP']),
        "Userid": user['username'],
        "transactionDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'paypal', 'stripe', 'online_transfers'])
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition()}]')


def main():
    topic = 'financial_transaction'
    producer = SerializingProducer({
        "bootstrap.servers": 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 100:
        try:
            transaction = generate_sales_transactions()
            transaction['TotalAmount'] = transaction['ProductPrice'] * transaction['productQuantity']

            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value= json.dumps(transaction),
                             on_delivery= delivery_report)
            producer.poll(0)
            
            time.sleep(5)

        except Exception as e:
            print(e)



if __name__ == "__main__":
    main()
