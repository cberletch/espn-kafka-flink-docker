# ESPN API Streaming Project

## Overview
This project implements a real-time data streaming pipeline that fetches NFL data from ESPN's public API. The architecture uses Docker to orchestrate multiple services, including Apache Kafka for message queuing and Apache Flink for stream processing. An optional Apache Airflow integration is included for potential batch processing capabilities.

## Architecture
- **Data Source**: ESPN NFL Public API
- **Message Queue**: Apache Kafka with Zookeeper
- **Stream Processing**: Apache Flink
- **Visualization**: Kafdrop (Kafka UI)
- **Container Orchestration**: Docker & Docker Compose
- **Optional Scheduler**: Apache Airflow

## Prerequisites
- Operating System: Developed on Ubuntu (compatible with other OS)
- Docker & Docker Compose
- Python 3.11
- OpenJDK 11
- Python virtual environment

## Installation Steps

### 1. System Dependencies

```bash
# Install Docker
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker packages
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Install Java
sudo apt install openjdk-11-jdk

# Configure Java Home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
```

### 2. Python Environment Setup

```bash
# Install Python 3.11
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-distutils

# Create and activate virtual environment
python3.11 -m venv venv311
source venv311/bin/activate

# Install Python dependencies
pip install --upgrade pip
pip install kafka-python requests apache-flink setuptools
```

### 3. Project Structure
```
project/
├── src/
│   ├── producers/
│   │   └── espn_producer.py
│   ├── processors/
│   │   └── flink_processor.py
│   ├── transformers/
│   └── utils/
├── config/
├── tests/
├── libs/
│   ├── flink-connector-kafka-3.0.0-1.17.jar
│   ├── flink-json-1.20.0.jar
│   └── kafka-clients-3.2.3.jar
└── kafka/
    └── docker-compose.yml
```

### 4. Kafka Setup
The `docker-compose.yml` includes:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Kafdrop UI (port 9000)

To start the Kafka environment:
```bash
cd kafka/
docker-compose up -d
```

### 5. Flink Dependencies
Download required JAR files:
```bash
mkdir -p libs
cd libs
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar
wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.20.0/flink-json-1.20.0.jar
```

## Running the Project

1. Start Kafka services:
```bash
cd kafka/
docker-compose up -d
```

2. Run the ESPN data producer:
```bash
python3 src/producers/espn_producer.py
```

3. Run the Flink processor:
```bash
python src/processors/flink_processor.py
```

4. Access Kafdrop UI:
Open `http://localhost:9000` in your browser to monitor Kafka topics and messages.

## Optional: Airflow Integration
The project includes a placeholder for Apache Airflow integration. To use Airflow:

1. Set up Airflow directory structure:
```bash
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

2. Download Airflow docker-compose file:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.2/docker-compose.yaml'
```

3. Initialize Airflow:
```bash
docker-compose up airflow-init
```

## Troubleshooting

1. Docker Permission Issues:
```bash
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
```

2. Python Environment Issues:
- Ensure you're using Python 3.11
- Verify virtual environment activation
- Check all required dependencies are installed

## Future Enhancements
- Implementation of Airflow DAGs for scheduled processing
- Additional data transformations in Flink
- Enhanced error handling and monitoring
- Data persistence layer integration
