# Apache Kafka: Ingeniería de Eventos y Semántica de Producción
![image](https://github.com/user-attachments/assets/1fb46f66-541a-46e1-8371-a4515dde1ca9)
![image](https://github.com/user-attachments/assets/30738f9b-a634-4d64-93bf-e39b7278a4e7)
![image](https://github.com/user-attachments/assets/2ee16e74-8a79-4e41-9947-9eb8be438949)

________________________________________________________________________________________________________________________________________________________________________________________________________________

## 🎯 Descripción del Proyecto
Más allá de aprender las APIs de Kafka, este laboratorio está diseñado para entender cómo se comporta Kafka en entornos de producción real.

La mayoría de los tutoriales de Kafka se detienen en enseñar cómo enviar y recibir un mensaje. Este laboratorio va más allá: está diseñado para entender cómo se comporta Kafka en entornos de producción real, enfocándose en los desafíos de la ingeniería de eventos: la confiabilidad, la gestión de estado y las garantías de entrega bajo fallos de red.

A través de implementaciones prácticas, exploro la transición del modelo tradicional de solicitudes (Request-Response) a la Arquitectura Event-Driven (EDA), abordando los "trade-offs" reales que un ingeniero de datos debe tomar al diseñar flujos de streaming críticos.

## 🏗️ Arquitectura del Entorno (Modo KRaft)
Un detalle crítico de este proyecto es que no utiliza ZooKeeper. El entorno está desplegado utilizando KRaft (Kafka Raft Metadata mode), que es el estándar absoluto moderno para Kafka (eliminando la complejidad operativa de mantener un clúster de ZooKeeper separado).

  * Broker/Controller Combinado: Desplegado vía Docker Compose exponiendo listeners tanto para comunicación interna (PLAINTEXT) como externa desde el host (PLAINTEXT_EXTERNAL).
  * Interfaz de Monitoreo: Integración de kafka-ui para visualización de tópicos, consumidores y lag en tiempo real.


## 🛡️ El Desafío de la Confiabilidad (Delivery Semantics)
El núcleo de este laboratorio es la comprensión profunda de cómo evitar la pérdida o duplicación de datos en sistemas distribuidos. Se implementaron y compararon las tres semánticas de entrega:

### 1. Entrega "At-Most-Once" (Fire and Forget)

***Archivo:*** <mark>producer_retries.py</mark>

* **Implementación:** Envío de mensajes sin exigir confirmación (acks) del broker.
  
* **Caso de uso:** Telemetría o logs no críticos donde la velocidad de procesamiento prima sobre la garantía absoluta de entrega.
  
### 2. Gestión de Offsets y "At-Least-Once" (La trampa del procesamiento)

***Archivos:*** <mark>consumer.py vs manual_commit_consumer.py</mark>

* **El problema:** Usar enable_auto_commit=True es peligroso en producción. Si el consumidor lee el mensaje, el offset avanza automáticamente, y luego el código falla durante el procesamiento, el mensaje se pierde para siempre.
  
* **La solución:** Deshabilitar el auto-commit (enable_auto_commit=False) y ejecutar consumer.commit() solamente después de una lógica de procesamiento exitosa. Esto garantiza cero pérdida de datos, asumiendo la responsabilidad de manejar posibles duplicados en caso de reinicios abruptos.
  
### 3. Productores Idempotentes y "Exactly-Once" (Nivel Empresarial)

***Archivo:*** <mark>Producer_idempotent.py</mark>

* **Implementación:** Configuración a nivel de productor con enable.idempotence y transactional.id.
  
* **El impacto:** En pipelines financieros o de inventario, ni un solo duplicado es tolerable. Esta configuración le indica a Kafka que asigne un PID (Producer ID), rastree los números de secuencia de cada mensaje y filtre duplicados en el lado del broker antes de escribir en el log, garantizando la semántica "Exactamente Una Vez".
  
## 🧩 Serialización y Modelado de Eventos

***Archivo:*** <mark>json_producer.py</mark>

En sistemas reales, los eventos contienen estructuras de datos complejas, no solo cadenas de texto. Implementé el patrón de Serialización JSON inyectando un value_serializer con funciones lambda. Esto transforma diccionarios de Python en payloads estructurados, sentando las bases para integraciones futuras con Schema Registry y contratos de datos (Avro/Protobuf).

## ⚖️ Escalabilidad: Consumer Groups y Rebalancing

***Archivo:*** <mark>group_consumer.py</mark>

* **Concepto Clave:** Kafka no escala procesando más rápido un solo hilo; escala dividiendo el trabajo. Al levantar múltiples instancias de este consumidor bajo el mismo group_id, se observa el comportamiento de Rebalancing automático: Kafka asigna particiones distintas a cada proceso (PID) de forma autónoma, logrando procesamiento paralelo sin requerir balanceo manual de carga.


## 📂 Estructura del Laboratorio

├── docker-compose.yml&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Clúster KRaft moderno (Sin ZooKeeper)

├── producer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Producer básico con acks="all"

├── producer_retries.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Resiliencia de red.

├── Producer_idempotent.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Transacciones y Exactly-Once Semantics.

├── json_producer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Serialización de eventos complejos.

├── consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Consumer con Auto-Commit.

├── manual_commit_consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Control fino de Offsets (At-Least-Once).

└── group_consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Escalabilidad mediante Consumer Groups.

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
