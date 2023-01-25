FROM python:3.11-slim

COPY requirements.txt /

RUN pip install -r /requirements.txt \
	&& rm -rf /root/.cache

COPY src/ /code

ENV ENVIRONMENT_FILE=".env"

WORKDIR /code

EXPOSE 8085

