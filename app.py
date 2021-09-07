from aiokafka import AIOKafkaConsumer
import asyncio, os, ast , sys
import nest_asyncio
nest_asyncio.apply()

## global variable :: setting this for kafka Consumer
KAFKA_ENDPOINT = os.getenv('KAFKA_ENDPOINT', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'lpr')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'event_consumer_group')
loop = asyncio.get_event_loop()

## Database details and connection
DB_USER = os.getenv('DB_USER', 'dbadmin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'HT@1202k')
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_NAME = os.getenv('DB_NAME','pgdb')
TABLE_NAME = os.getenv('TABLE_NAME','event')

async def consume():
    kafkaConsumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop, bootstrap_servers=KAFKA_ENDPOINT, group_id=KAFKA_CONSUMER_GROUP_ID)

    await kafkaConsumer.start()
    try:
        async for msg in kafkaConsumer:
            message = msg.value
            payload=ast.literal_eval(message.decode('utf-8'))
            plate = payload['event_vehicle_detected_plate_number']
            when = payload['event_timestamp']
            try:
                print("===============================================")
                print(payload)
                print("===============================================")
            except Exception as e:
                print(e)
                print("Exiting ....")
                sys.exit(1)
    except Exception as e:
        print(e.message)
        print("Exiting ....")
        sys.exit(1)
    finally:
        await kafkaConsumer.stop()
loop.run_until_complete(consume())
