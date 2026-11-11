import argparse
import uuid
from datetime import datetime

import matplotlib.pyplot as plt
from kafka import KafkaConsumer

from common import BOOTSTRAP, SASL, TOPIC, VIENTOS, decode, from_json_bytes

parser = argparse.ArgumentParser()
parser.add_argument("--bytes", action="store_true", help="decodificar payload de 3 bytes en vez de JSON")
args = parser.parse_args()

deserializar = decode if args.bytes else from_json_bytes
salida = "grafica_3bytes.png" if args.bytes else "grafica_json.png"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=f"lab-iot-{uuid.uuid4().hex[:8]}",  # group nuevo en cada corrida
    auto_offset_reset="latest",
    **SASL,
)

all_temp, all_hume, all_wind, all_t = [], [], [], []

plt.ion()
fig, (ax_t, ax_h, ax_w) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
fig.suptitle(f"Estación meteorológica ({'3 bytes' if args.bytes else 'JSON'})")


def graficar():
    ax_t.clear()
    ax_h.clear()
    ax_w.clear()

    ax_t.plot(all_t, all_temp, marker="o", color="tab:red")
    ax_t.set_ylabel("Temp (°C)")
    ax_t.set_ylim(0, 110)

    ax_h.plot(all_t, all_hume, marker="o", color="tab:blue")
    ax_h.set_ylabel("Humedad (%)")
    ax_h.set_ylim(0, 100)

    # el viento es categórico, así que solo puntos (sin línea)
    ax_w.plot(all_t, [VIENTOS.index(v) for v in all_wind], marker="o", linestyle="", color="tab:green")
    ax_w.set_yticks(range(len(VIENTOS)))
    ax_w.set_yticklabels(VIENTOS)
    ax_w.set_ylim(-0.5, len(VIENTOS) - 0.5)
    ax_w.set_ylabel("Viento")
    ax_w.set_xlabel("Hora")

    for ax in (ax_t, ax_h, ax_w):
        ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()

    # guardar siempre con tamaño grande, sin importar el tamaño de la ventana
    tam_ventana = fig.get_size_inches().copy()
    fig.set_size_inches(10, 8)
    fig.tight_layout()
    fig.savefig(salida, dpi=150)
    fig.set_size_inches(tam_ventana)
    fig.tight_layout()
    fig.canvas.draw_idle()


print(f"Escuchando '{TOPIC}'... Ctrl+C para parar.")

try:
    while plt.fignum_exists(fig.number):
        lote = consumer.poll(timeout_ms=1000)
        for _, mensajes in lote.items():
            for m in mensajes:
                payload = deserializar(m.value)
                print(payload)
                all_temp.append(payload["temperatura"])
                all_hume.append(payload["humedad"])
                all_wind.append(payload["direccion_viento"])
                all_t.append(datetime.now())
                graficar()
        plt.pause(0.1)  # mantiene viva la ventana
except KeyboardInterrupt:
    print("Detenido.")
finally:
    consumer.close()