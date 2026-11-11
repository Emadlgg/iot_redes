# IoT: Estación Meteorológica con Apache Kafka

Laboratorio de CC3067 Redes, Universidad del Valle de Guatemala.

Simula un nodo de sensores de una estación meteorológica que manda su telemetría (temperatura, humedad relativa y dirección del viento) a un broker de Kafka en el Edge. Un consumidor se suscribe al topic y grafica los datos en tiempo real. Además de enviar JSON, el proyecto incluye un modo con payload de 3 bytes (24 bits) para simular redes IoT con restricciones como LoRa o Sigfox.

## Estructura

```
.
├── common.py          # config, generador de datos y encode/decode a 3 bytes
├── producer.py        # Kafka Producer (sensor simulado)
├── consumer.py        # Kafka Consumer + gráfica en vivo
├── requirements.txt
└── evidencia/         # capturas y gráficas generadas
```

## Requisitos

- Python 3.10 o superior
- Acceso de red al servidor Kafka del curso (puerto 9092)

```bash
pip install -r requirements.txt
```

Si usas Python 3.12 o superior y `kafka-python` falla al importar, cambia esa línea del `requirements.txt` por `kafka-python-ng`.

## Configuración

Todo está en `common.py`:

- `BOOTSTRAP`: dirección del servidor Kafka.
- `TOPIC`: topic único por pareja (se usa el carné de uno de los integrantes). Cámbialo antes de correr.
- `SASL`: parámetros de autenticación (`SASL_PLAINTEXT` con mecanismo `PLAIN`).

## Uso

Abre dos terminales en la carpeta del proyecto. Arranca primero el consumidor, porque lee solo los mensajes nuevos (`auto_offset_reset="latest"`).

**Modo JSON**

```bash
python consumer.py
python producer.py
```

**Modo 3 bytes**

```bash
python consumer.py --bytes
python producer.py --bytes
```

El productor envía un dato cada 15 a 30 segundos hasta que lo interrumpas con `Ctrl+C`. El consumidor actualiza la gráfica con cada mensaje y guarda una imagen (`grafica_json.png` o `grafica_3bytes.png`).

Para probar el codec sin conectarte a Kafka:

```bash
python common.py
```

## Datos simulados

| Sensor | Rango | Tipo | Generación |
|---|---|---|---|
| Temperatura | 0 a 110 °C | float, 2 decimales | Normal con media 55 y σ = 15, recortada al rango |
| Humedad relativa | 0 a 100 % | entero | Normal con media 55 y σ = 15, recortada al rango |
| Dirección del viento | N, NO, O, SO, S, SE, E, NE | categórico | Uniforme |

Ejemplo de mensaje en JSON:

```json
{"temperatura": 56.32, "humedad": 51, "direccion_viento": "SO"}
```

## Codificación a 3 bytes

El payload completo cabe en 24 bits:

```
| temperatura (14 bits) | humedad (7 bits) | viento (3 bits) |
```

- **Temperatura:** se multiplica por 100 y se guarda como entero (0.00 a 110.00 pasa a 0 a 11000). El máximo, 11000, cabe en 14 bits porque 2^14 = 16384.
- **Humedad:** entero de 0 a 100, entra en 7 bits.
- **Viento:** índice de 0 a 7 en la lista de direcciones, entra en 3 bits.

`encode()` empaqueta los tres valores en un entero de 24 bits y lo convierte a 3 bytes. `decode()` hace el proceso inverso.

## Nota sobre las credenciales

Las credenciales de `common.py` son las compartidas del servidor del curso. Si el repositorio es público, considera moverlas a variables de entorno o dejarlo privado.
