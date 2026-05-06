FROM prefecthq/prefect:2-python3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Los flows se montan como volumen en docker-compose
# no se copian aquí para facilitar el desarrollo