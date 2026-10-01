# Informe Trabajo Practico Coordinación

## 1. Introducción

El sistema procesa registros de fruta enviados por múltiples clientes a través de un pipeline de nodos (Gateway → Sum → Aggregation → Join → Gateway), comunicados por colas y exchanges de RabbitMQ. Tanto Sum como Aggregation pueden tener con múltiples réplicas para distribuir el trabajo. Este informe describe cómo esas réplicas se coordinan entre sí y cómo el diseño escala en tres dimensiones: cantidad de clientes, volumen de datos, y cantidad de réplicas.

## 2. Identificación de clientes

Cada cliente que se conecta al Gateway recibe un `client_id` único, generado por el `MessageHandler` correspondiente. Ese `client_id` viaja en **todos** los mensajes internos del sistema y es la clave que permite que todos los nodos mantengan el estado de cada cliente de forma completamente independiente. Ningún nodo mezcla datos de clientes distintos.

## 3. Reparto de trabajo entre réplicas

### 3.1 Sum

Las instancias de Sum consumen todas de la misma cola compartida (`input_queue`). RabbitMQ reparte los mensajes entre las réplicas disponibles, de forma que cada registro de un cliente puede ser procesado por cualquiera de ellas. De esta forma, los datos de una misma fruta para un mismo cliente pueden quedar repartidos entre varias réplicas de Sum, cada una acumulando una suma parcial local por fruta que luego sera enviada al Aggregation para ir formando los tops parciales. 

A la hora de trabajar con replicas nos encontramos con un desafió principal: saber cuándo un cliente
terminó de enviar **todos** sus datos, es decir, el EOF. Como mencionamos anteriormente, los mensajes del cliente son divididos entre las diferentes instancias de Sum, de forma tal que el EOF solo seria recibido por una única replica. De esta forma, es necesario encontrar una forma para que el resto de las replicas puedan obtener el aviso de EOF.

A partir de esto, se decidió utilizar 2 exchanges de control para comunicar las distintas instancias de Sum. En primer lugar, tenemos un exchange que tiene el rol de publisher donde se publicaran los mensajes de control como por ejemplo el EOF. Por otro lado, tenemos el exchange que tendrá el rol de consumer el cual consumirá los mensajes de control en otro hilo separado. De esta forma, cuando una de las replicas de sum reciba el EOF le notificara automáticamente al resto de las replicas para que estas puedan mandar sus datos acumulados de un cliente en particular al nodo Aggregation. 

Sin embargo, esto nos genera un nuevo problema: saber que efectivamente el nodo proceso todos los datos de un cliente antes de recibir el EOF. Para esto, utilizamos un conteo de mensajes. 

El `MessageHandler` del Gateway cuenta cuántos mensajes de datos envía por cliente y adjunta ese total al mensaje de EOF. Cuando una réplica de Sum recibe ese aviso, lo retransmite a **todas** las réplicas (incluida ella misma) a través del exchange de control para que todas sepan cual es el total de mensajes que debe ser procesado. 

Antes de recibir el aviso de EOF cada una de las replicas fue sumando la cantidad de mensajes que proceso de un cliente en particular sin avisarle el resto. Una vez que llega el EOF y se sabe cual es el total esperado de ese cliente, cada replica manda su estado actual, es decir, la cantidad de mensajes que había procesado. Una vez que recibe el total, si llega a procesar un nuevo mensaje (es decir, procesa un mensaje después del EOF) manda un aviso al resto de las replicas que procesó un nuevo mensaje. Cada vez que una réplica recibe un aviso de conteo (ya sea el estado inicial de otra réplica, o un nuevo mensaje que esta u otra réplica procesó algo), lo suma a un contador de mensajes recibidos por cliente. Como cada aviso representa un incremento que nunca se retransmite dos veces, la suma de todos los avisos recibidos refleja exactamente la cantidad total de mensajes procesados entre todas las réplicas. Cuando ese acumulado iguala al total esperado, cada réplica sabe que ya no queda ningún mensaje del cliente pendiente de procesar en ninguna instancia, y envía su propia suma parcial hacia Aggregation.

Este mecanismo evita depender del orden entre canales distintos, que en un sistema distribuido no están garantizados: el envío del aviso de fin puede, en teoría, adelantarse a la llegada de los últimos datos si ambos viajan por canales separados.

### 3.2 Aggregation

Cada instancia de Sum, al enviar sus sumas parciales hacia Aggregation, elige la instancia de destino aplicando una función de hash determinística sobre el nombre de la fruta. Esto garantiza que **todas** las sumas parciales de una misma fruta se dirigen siempre a la misma instancia de Aggregation, que es la única responsable de consolidar el total real de esa fruta. Esto es una optimización de la implementación original donde el sum simplemente hacia broadcast hacia las diferentes instancias de Aggregation. 

Cada instancia de Aggregation recibe un aviso de fin por cada réplica de Sum (`SUM_AMOUNT` en total, para cada cliente). A diferencia del caso anterior, acá no hace falta un conteo cruzado de mensajes: cada réplica de Sum, gracias a la coordinación descripta anteriormente, ya garantiza que su aviso de fin llega **después** de haber enviado todos sus datos. Aggregation simplemente cuenta cuántos avisos de fin recibió por cliente y, al llegar al total (`SUM_AMOUNT`), arma su top parcial y lo envía al Join.

### 3.3 Join

De forma similar al Aggregation, el Join cuenta cuántos tops parciales recibió por cliente y, al llegar a `AGGREGATION_AMOUNT`, fusiona las listas recibidas, las ordena y recorta a `TOP_SIZE`, obteniendo así el top final que se devuelve al cliente a través del Gateway.

## 4. Escalabilidad

### 4.1 Respecto a la cantidad de clientes

Para manejar a muchos clientes al mismo tiempo, el sistema usa una regla muy simple: cada mensaje esta identificado según el cliente utilizando un client_id.

Gracias a esto, cuando los datos entran a los nodos, el sistema no mezcla todo sino que, guarda la información separando por cliente. Lo que le pasa al cliente A no afecta en nada al cliente B, toda su información pasa por caminos completamente independientes.

Esto hace que el sistema pueda atender a muchos clientes en paralelo sin pisarse los datos y sin grandes problemas en performance. Sin embargo, hay que tener algo en cuenta: el uso de memoria. Cada cliente deja activo un lugar en memoria, es decir, un lugar del diccionario, de forma tal que si hay miles de clientes cada vez se va a ir utilizando mas memoria. Para solucionar esto, una vez que un cliente termina, cada nodo se encarga de limpiar su memoria para que no queden datos viejos ocupando lugar. 

En términos simples, el sistema escala frente a muchos clientes porque la información está totalmente aislada, siempre y cuando se limpie el estado de los que ya terminaron.

### 4.2 Respecto al volumen de datos

El sistema escala de forma favorable con respecto al volumen de datos porque la suma y la agregación se distribuyen en varias etapas. En Sum, cada mensaje de datos entra a una cola compartida y RabbitMQ lo entrega a una de las réplicas disponibles. Como cada instancia mantiene un estado local por fruta y por cliente, el volumen total de registros de los clientes se reparte entre varias areas de procesamiento en lugar de concentrarse en una sola. Esto permite que, aunque un cliente envíe millones de frutas, el trabajo no se vuelva un cuello de botella. Cada replica procesa solo una parte del total y luego envía su estado parcial para su consolidación posterior.

La ventaja se repite en Aggregation: en lugar de que todas las sumas parciales de un cliente sean reenviadas a todas las instancias, se aplica una función de hash sobre la fruta para que cada subtotal vaya siempre a la misma instancia. Esto permite evitar el procesamiento redundante, mejorando la eficiencia del nodo.

### 4.3 Respecto a la cantidad de réplicas (controles)

Como mencionamos anteriormente, agregar replicas mejora la eficiencia ante grandes volúmenes de datos. Sin embargo, agregar replicas también tiene un costo de coordinación, especialmente en Sum. En Sum, la coordinación entre réplicas exige un mecanismo de conteo distribuido para saber cuándo un cliente terminó de enviar todos sus mensajes y cuándo ya no quedan entradas pendientes en ninguna instancia. Ese mecanismo implica mensajes de control y broadcast de estados parciales por cliente, por lo que el costo de coordinación crece con la cantidad de réplicas.

Este overhead es el precio que paga la division del trabajo: cuantas más replicas tenga, más difícil resulta saber que el conjunto completo ya fue procesado. Aun así, este costo se concentra en la fase de cierre del cliente. Por eso, el aumento de réplicas sigue siendo útil en la práctica y no invalida la escalabilidad del sistema.

En cambio, en Aggregation y Join la coordinación es mucho más simple. Cada réplica de Sum ya envía su suma parcial con la garantía de que el cliente está cerrado, y cada instancia de Aggregation solo debe contar cuántos tops parciales o mensajes de cierre recibió para completar una barrera por cliente. Esto hace que el costo de escalar Aggregation sea mucho menor que el de escalar Sum: el intercambio de control es más sencillo y no requiere un conteo cruzado de todos los datos procesados.

En síntesis, la cantidad de réplicas puede aumentar el rendimiento del sistema, pero no lo hace gratis: en Sum, la coordinación de cierre se vuelve más costosa.