FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /service
COPY pyproject.toml ./
COPY app ./app
RUN pip install . && useradd --create-home service
COPY alembic.ini ./
COPY migrations ./migrations
USER service
EXPOSE 8000
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
