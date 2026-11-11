import argparse
import random
import time

from kafka import KafkaProducer

from common import BOOTSTRAP, SASL, TOPIC, encode, generar_data, to_json_bytes

parser = argparse.ArgumentParser()
parser.add_argument("--bytes", action="store_true", help="enviar payload de 3 bytes en vez de JSON")
args = parser.parse_args()

serializar = encode if args.bytes else to_json_bytes

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, **SASL)

print(f"Enviando a '{TOPIC}' en modo {'3 bytes' if args.bytes else 'JSON'}. Ctrl+C para parar.")

try:
    while True:
        data = generar_data()
        payload = serializar(data)
        producer.send(TOPIC, key=b"sensor1", value=payload)
        producer.flush()
        print(f"{data} -> {payload!r} ({len(payload)} bytes)")
        time.sleep(random.uniform(15, 30))
except KeyboardInterrupt:
    print("Detenido.")
finally:
    producer.close()
