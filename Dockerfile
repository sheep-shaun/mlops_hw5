FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 PYTHONPATH=/app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY common common
COPY fraud_detector fraud_detector
COPY pipeline pipeline
COPY interface interface
COPY score_writer score_writer
COPY transaction_simulator transaction_simulator
CMD ["python", "-m", "fraud_detector.app.app"]
