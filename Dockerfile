FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD gunicorn -w 1 --threads 4 -b 0.0.0.0:${PORT:-8000} kalshi_bot.web:app
