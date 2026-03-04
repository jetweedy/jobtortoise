FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

RUN chmod -R 755 /app/templates /app/static

CMD ["python", "app.py"]
