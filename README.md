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

├── docker-compose.yml&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Clúster KRaft moderno (Sin ZooKeeper)

├── producer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Producer básico con acks="all"

├── producer_retries.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Resiliencia de red.

├── Producer_idempotent.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Transacciones y Exactly-Once Semantics.

├── json_producer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Serialización de eventos complejos.

├── consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Consumer con Auto-Commit.

├── manual_commit_consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Control fino de Offsets (At-Least-Once).

└── group_consumer.py&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;# Escalabilidad mediante Consumer Groups.

## 🏗️ DESARROLLO

1.	Creamos la carpeta kafka-lab desde la terminal y luego accedemos a ella.
     
![image](https://github.com/user-attachments/assets/766c4b80-be59-4184-8659-2ea142259a9d)

2.	Ahora abrimos el editor de código VSC y creamos la carpeta Docker-compose.yaml

Código:

        services:
          kafka:
            image: apache/kafka:latest
            container_name: kafka
            ports:
              - "9092:9092"
              - "29092:29092"
            environment:
              # ---- KRaft core ----
              KAFKA_NODE_ID: 1
              KAFKA_PROCESS_ROLES: broker,controller
              KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

              # ---- Listeners (ALL roles must appear here) ----
             KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093

              # ---- Advertised addresses ----
              KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://host.docker.internal:29092

              # ---- Controller configuration ----
              KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
              KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

              KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

              # ---- Protocol mapping ----
              KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

              # ---- Storage ----
              KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

          kafka-ui:
            image: provectuslabs/kafka-ui:latest
            container_name: kafka-ui
            depends_on:
              - kafka
            ports:
              - "8080:8080"
            environment:
              KAFKA_CLUSTERS_0_NAME: local-kafka
              KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092



Ahora en la misma terminal de Docker desktop ejecutamos el siguiente comando.

Terminal:

          Docker-compose up -d


![image](https://github.com/user-attachments/assets/4582ae1f-842d-4c27-b804-9c34004f6523)

![image](https://github.com/user-attachments/assets/8b2cfc55-228a-429f-8e64-b9a9e3a6b6f9)

Luego hacemos click en el puerto 8080:8080 para acceder a la interface de usuario de Kafka.

![image](https://github.com/user-attachments/assets/598df57b-ad1e-444f-93d7-0888ced7c930)

### Laboratorio 01: Creación de temas (CLI de Kafka)
Kafka CLI se ejecuta dentro del contenedor.

* **Paso 1:** Entrar en el contenedor Kafka.
  
ahora en terminal de docker desktop escribimos el siguiente codigo.

código:

        docker exec -it kafka bash


código:

        cd /opt/kafka/bin

* **Paso 2:** Crear un tema.

código:

        ./kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server host.docker.internal:29092

        
Deberías ver la confirmación de la creación del tema.

* **Paso 3:** Verifica el tema.

  código:
  
          ./kafka-topics.sh --list --bootstrap-server host.docker.internal:29092


![image](https://github.com/user-attachments/assets/3357e0a2-a334-4947-8dba-0ae20d92b6bc)


Producción esperada:

Órdenes

Lo que esto demuestra

•	Los temas son entidades explícitas.

•	Kafka no hace nada hasta que existen temas.

•	La organización de datos es intencionada, no automática.


Sal del contenedor:

código:

         exit

### Laboratorio 02: Inspección de particiones

Kafka ya está huyendo desde el laboratorio anterior.

**Paso 1:** Entrar en el contenedor Kafka

terminal docker desktop:

                         docker exec -it kafka bash

terminal docker desktop:

                         cd /opt/kafka/bin

**Paso 2:** Describe el tema

terminal docker desktop:

                          ./kafka-topics.sh --describe --topic orders --bootstrap-server host.docker.internal:29092

* La producción esperada incluye:

   •	Nombre del tema
   •	Recuento de particiones
   •	Líder
   •	Réplicas
   •	ISR (Réplicas Sincronizadas — réplicas completamente al día)
   •	ELR = Réplicas de Líder Elegibles

•	Ahora observamos en docker desktop.

![image](https://github.com/user-attachments/assets/ef4d99c6-2dcb-467e-b1e1-c02952704b41)

![image](https://github.com/user-attachments/assets/a9d7307e-68cf-4c75-8736-b2c085c64047)


•	Y también observamos en Kafka UI (localhost:8080) en la pestaña de brokers y topics.

![image](https://github.com/user-attachments/assets/8613db9f-cc22-42d5-854f-bcaf09a04129)

![image](https://github.com/user-attachments/assets/5065df14-8d5d-4b5d-895b-bebbccd0511b)

**Paso 3:** Qué debes saber

   •	Cada partición se lista por separado
   
   •	Cada partición tiene:
   
      o	Su propio líder
      
      o	Su propio juego réplica
      
   •	Kafka trata las particiones como unidades independientes
   
   En esta fase:

   •	Sin productores

   •	Sin consumidores

   Este laboratorio trata sobre la estructura, no el flujo de datos.

   Paso 4: Sal del contenedor

   Terminal: 
    
             exit


### Laboratorio 03: Observación de desplazamientos y retraso del consumidor

Este es el primer laboratorio donde se observan desplazamientos mediante herramientas Kafka.

Kafka ya está huyendo.

**Paso 1:** Producir datos de prueba

Entra en escena el contenedor Kafka:

terminal docker desktop:

          docker exec -it kafka bash

terminal docker desktop:

          cd /opt/kafka/bin

          
Empieza un productor de consola:

terminal docker desktop:

                         ./kafka-console-producer.sh --topic orders --bootstrap-server host.docker.internal:29092

Escribe algunos mensajes manualmente:

Orden-1

Orden-2

Orden-3

Orden-4

•	Observamos en docker desktop.

![image](https://github.com/user-attachments/assets/784adde6-a232-4d84-8d67-d51a8d15c9fc)

•	También observamos en Kafka UI

![image](https://github.com/user-attachments/assets/91fab1f2-7466-4aef-b99d-bdcc829767b4)

![image](https://github.com/user-attachments/assets/ebb4c875-4336-43a7-998d-bdabc06235e1)

Salir del productor con Ctrl+C.

**Paso 2:** Crear un grupo de consumidores.

Ejecuta una consola para consumidores.

terminal docker desktop:

                         ./kafka-console-consumer.sh --topic orders --group orders-consumer-group --from-beginning --bootstrap-server host.docker.internal:29092


![image](https://github.com/user-attachments/assets/617de1f1-359f-467c-8439-b5969fb96b6f)

•	Verificamos en Kafka UI

![image](https://github.com/user-attachments/assets/9b951e3a-5850-437d-ba96-55a1fa5f641f)

![image](https://github.com/user-attachments/assets/abf4236f-eb2b-4e7b-a4c1-701bdef8eb36)

Déjalo leer mensajes y luego detenerlo con Ctrl+C.

Lo que acaba de pasar:

•	Se creó un grupo de consumidores (grupo de consumidores = compartidos coordinados de los consumidores)

•	Se comprometían los desplazamientos  para cada partición

**Paso 3:** Inspeccionar el retardo del grupo de consumidores

Corre:

       ./kafka-consumer-groups.sh --describe --group orders-consumer-group --bootstrap-server host.docker.internal:29092

•	Aplicamos el comando en docker desktop.

![image](https://github.com/user-attachments/assets/32ddb248-2a0e-4c5e-a8dd-471624e1237b)

•	Verificamos los resultados en Kafka UI.

![image](https://github.com/user-attachments/assets/a41036fd-ebc4-46b0-bc96-d47900832380)

![image](https://github.com/user-attachments/assets/d99d58b2-3ede-49d1-aa13-957636698f42)

![image](https://github.com/user-attachments/assets/5deeabe8-bea8-4db3-bf71-c616807242c7)

Explícado explícitamente en pantalla:

•	DESPLAZAMIENTO DE CORRIENTE→ dónde se encuentra ahora el consumidor.

•	LOG-END-OFFSET →  último evento en la partición.

•	LAG → qué tan atrasado está el consumidor.

Esto demuestra tres hechos clave:

•	Kafka rastrea posiciones, no borra mensajes.

•	Kafka sabe exactamente cuánto atrasa un consumidor.

•	Los mensajes siguen existiendo incluso después de ser consumidos.

**Paso 4:** Sal del contenedor

Terminal: 

          exit

### Laboratorio 03: Productor de Python (Desde Cero)

Este es el primer cliente Kafka real, así que la configuración es explícita e innegociable.

1. Configuración del entorno (Python)
   
* Paso 1: Verificar Python

  Abre una terminal en power shwell

Código:

        Python --versión

Debes ver Python 3.10 o superior.

* Paso 2: Crear un entorno virtual (recomendado)

 Código:

         python -m venv kafka-env


* **En Windows**

Código:

        kafka-env\Scripts\activate 

* **# Linux / macOS**

Código:

        kafka-env/bin/activate 


Esto aísla las dependencias de Kafka de tu sistema en Python.

**Paso 3:** Instalar la biblioteca cliente de Kafka

Código:

        pip install kafka-python-ng

![image](https://github.com/user-attachments/assets/c37b3a4b-c9b5-4e5d-825c-df93e9f3a521)

**2. Código productor en Python**

Crea un archivo llámala <mark>"producer.py"</mark>

![image](https://github.com/user-attachments/assets/fc44c790-7af1-4f3d-a792-f6385e252851)

Pega exactamente el siguiente código.

Código:

        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            acks="all",
            retries=5
        )
        print("Conectado a Kafka. Enviando mensaje...")
        producer.send("orders", b"order_created")
        producer.flush()
        print("¡Mensaje enviado exitosamente al tópico 'orders'!")
        producer.close()


![image]()
