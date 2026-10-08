FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc libffi-dev python3-dev ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd --create-home --uid 10001 bot && chown -R bot:bot /app
USER bot
EXPOSE 10000
CMD ["python", "bot.py"]
