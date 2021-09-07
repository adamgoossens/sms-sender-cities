from twilio.rest import Client
from aiokafka import AIOKafkaConsumer
import asyncio, os, ast , sys
import nest_asyncio
nest_asyncio.apply()

## global variable :: setting this for kafka Consumer
KAFKA_ENDPOINT = os.getenv('KAFKA_ENDPOINT', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'lpr')
KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'event_consumer_group')
loop = asyncio.get_event_loop()

TWILIO_CLIENT_ID = os.getenv('TWILIO_CLIENT_ID')
TWILIO_CLIENT_KEY = os.getenv('TWILIO_CLIENT_KEY')
TWILIO_DESTINATION_NUMBER = os.getenv('TWILIO_DESTINATION_NUMBER')
TWILIO_FROM_NUMBER = os.getenv('TWILIO_FROM_NUMBER')

twilio_is_on = os.getenv('TWILIO_ENABLED', 'false') != 'false'

watch_for_plates = ["2216E06", "S7JDV", "LCA2555"]

anon_source_number = TWILIO_FROM_NUMBER[0:3] + "..." + TWILIO_FROM_NUMBER[-3:]
anon_dest_number = TWILIO_DESTINATION_NUMBER[0:3] + "..." + TWILIO_DESTINATION_NUMBER[-3:]

twilio = Client(TWILIO_CLIENT_ID, TWILIO_CLIENT_KEY)

async def consume():
    kafkaConsumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop, bootstrap_servers=KAFKA_ENDPOINT, group_id=KAFKA_CONSUMER_GROUP_ID)

    await kafkaConsumer.start()
    try:
        async for msg in kafkaConsumer:
            message = msg.value
            payload=ast.literal_eval(message.decode('utf-8'))
            plate = payload['event_vehicle_detected_plate_number']
            when = payload['event_timestamp']
            station = 'a123'

            sms_body = f"ALERT: Plate of interest {plate} detected at station {station} at {when}"
            try:
                if plate in watch_for_plates:
                    if twilio_is_on:
                        twilio.messages.create(to=TWILIO_DESTINATION_NUMBER,
                                               from_=TWILIO_FROM_NUMBER,
                                               body=sms_body)
                    print("===============================================")
                    print(f"Detected plate of interest: {plate}")
                    print(f"Is Twilio on? f{twilio_is_on}")
                    print(f"Sent SMS to f{anon_dest_number} from f{anon_source_number}: 'f{sms_body}'")
                    print(payload)
                    print("===============================================")
                else:
                    print("Plate {} not of interest.".format(plate))

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
