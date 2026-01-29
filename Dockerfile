FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app/src

CMD gunicorn -w 1 --threads 4 --timeout 120 -b 0.0.0.0:$PORT kalshi_bot.web:app
