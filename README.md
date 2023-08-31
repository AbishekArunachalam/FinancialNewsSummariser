# Financial News Summariser
Ingest financial news data in real-time using Kafka publisher subscriber broker. Summarise the news using Large Languge Model(LLM) and send alerts to the user to support investment decisions.

# Getting Started
Create a virtual environment for the project:

python3 -m venv env_name

source env_name/bin/activate

### Install package dependencies in the virtual environment:

pip install -r requirements.txt

### Running Script
Execute the script from project root directory:

python kafka_stream.py
