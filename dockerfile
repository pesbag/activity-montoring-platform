FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY seed_data_script.py .
COPY Data ./Data
CMD ["python","seed_data_script.py"]