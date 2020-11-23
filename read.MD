# Praticando mensageria com Kafka

## O que precisa para rodar o projeto?

* Efetuar o download do kafka (https://kafka.apache.org/);


### Rodando o Kafka localmente
 
 rodar primeiro o comando do zookeeper, acessando o terminal da máquina, acesse o diretório/pasta onde foi descompactado o kafka e execute o comando abaixo:
 
 `bin/zookeeper-server-start.sh config/server.properties` 
 
 após executar o comando para o zookeeper, execute o comando para o kafka conforme abaixo: 
 
 `bin/kafka-server-start.sh config/server.properties`
 
 ### Criando um tópico
 
 para criarmos um tópico precisamos rodar o seguinte comando:
 
 `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic loja_novo_pedido`
 
 ### Verificando os tópicos criados
 
 para que seja verificado os tópicos criados:
 
 `bin/kafka-topics.sh --list -- bootstrap-server localhost:9092`
 
 ### produzindo mensagens
 
 `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic loja_novo_pedido`
 
 ### consumindo mensagens armazenadas desde o início
 
 `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic loja_novo_pedido --from-beginning`
 
 ### Reparticionar um tópico 
 
 O n° máximo de paralelizações vai ser o n° de partições
 
 `bin/kafka-topics.sh -alter --zookeeper localhost:9092 --topic ecommerce_new_order --partitions 3`
`