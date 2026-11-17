FROM python:3.12.16-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# check.py puts exactly one django-ox wheel, built from this checkout, here.
COPY wheel/ /tmp/wheel/
RUN pip install --no-cache-dir --no-deps /tmp/wheel/*.whl && rm -rf /tmp/wheel

COPY manage.py .
COPY demo/ demo/

RUN useradd --uid 10001 --no-create-home app
USER 10001
