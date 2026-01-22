FROM python:3.12-slim

WORKDIR /app
COPY kpi_service/app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kpi_service/app /app
COPY site_coords.json /app/site_coords.json

EXPOSE 8000
ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8011"]
