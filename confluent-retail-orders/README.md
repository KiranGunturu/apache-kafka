


![alt text](image.png)

## Installation

### Confluent CLI installation
```sh
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/4c728263-d674-441c-b342-981232b21ea0)

### Confluent login
```sh
./bin/confluent login --save
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/d05ee850-1195-45f6-a7d8-f8df9e58bc0f)

### Confluent Env List
```sh
./bin/confluent environment list
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/2039bd56-8a8e-4f94-8e4e-5865aa6e010f)

### Confluent Clusters List
```sh
./bin/confluent kafka cluster list
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/553fdfdf-2a98-4d99-89b8-b1bc0e565ba3)

#### Switch to Spec Env
```sh
./bin/confluent environment use env-p9j1qm
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/40e0d73f-5e2e-4758-835c-e7488af53e8b)

#### Switch to Spec Kakfa Cluseer
```sh
./bin/confluent kafka cluster use lkc-33n7nj
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/5bec5c81-e097-4863-bb57-577269a9f043)

#### Create an API key to produce and consume from the cluster
```sh
./bin/confluent api-key create --resource lkc-33n7nj
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/b528f6a3-04d4-4efa-b33a-726870b5e221)

#### Assign the API Key to spec to cluster
```sh
./bin/confluent api-key use IVC7WBIODUWJIG2K --resource lkc-33n7nj
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/3f442f50-3d7e-4da9-9b55-69c9209deed9)

#### List Kafka Topcis
```sh
./bin/confluent kafka topic list
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/ce23a1d7-35b4-4409-b6fb-8d46a69f82a6)


#### Consume from topic
```sh
./bin/confluent kafka topic consume --from-beginning retail_orders
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/f96cffb0-6f3b-48ed-9c4f-a1a4db1c311a)

#### Create topic
```sh
./bin/confluent kafka topic create --partitions 4 retail_orders_history
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/117c930f-7118-4a36-98b9-fcde58c8715d)

![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/3441d024-b035-4f90-9114-7daf2729b520)

#### Describe topic
```sh
./bin/confluent kafka topic describe retail_orders_history
```
![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/117c930f-7118-4a36-98b9-fcde58c8715d)

![image](https://github.com/KiranGunturu/apache-kafka/assets/91672788/453fd4f3-8064-49f3-a141-e1056aa38850)







