import json
import random

# ---- Config ----
BOOTSTRAP = "161.35.11.141:9092"
TOPIC = "23428"

SASL = dict(
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="estudiante",
    sasl_plain_password="TryluRandomPass-2!@!",
)

VIENTOS = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]


# ---- Sensor simulado ----
def _normal_recortada(media, sigma, minimo, maximo):
    return min(max(random.gauss(media, sigma), minimo), maximo)


def generar_data():
    return {
        "temperatura": round(_normal_recortada(55, 15, 0, 110), 2),
        "humedad": int(round(_normal_recortada(55, 15, 0, 100))),
        "direccion_viento": random.choice(VIENTOS),
    }


# ---- Modo JSON ----
def to_json_bytes(data):
    return json.dumps(data).encode("utf-8")


def from_json_bytes(raw):
    return json.loads(raw.decode("utf-8"))


# ---- Modo 3 bytes (24 bits) ----
# Layout: [ temperatura 14 bits | humedad 7 bits | viento 3 bits ]
# La temperatura se guarda como entero en centésimas (0.00 a 110.00 -> 0 a 11000),
# y 11000 cabe en 14 bits (2^14 = 16384).

def encode(data):
    temp = round(data["temperatura"] * 100)
    hum = int(data["humedad"])
    viento = VIENTOS.index(data["direccion_viento"])
    packed = (temp << 10) | (hum << 3) | viento
    return packed.to_bytes(3, "big")


def decode(raw):
    packed = int.from_bytes(raw, "big")
    return {
        "temperatura": ((packed >> 10) & 0x3FFF) / 100,
        "humedad": (packed >> 3) & 0x7F,
        "direccion_viento": VIENTOS[packed & 0x7],
    }


if __name__ == "__main__":
    # prueba rápida del codec
    for _ in range(5):
        d = generar_data()
        raw = encode(d)
        print(d, "->", raw.hex(), "->", decode(raw), f"({len(raw)} bytes)")
