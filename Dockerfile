FROM python:3.10

RUN mkdir -p /src
WORKDIR /app

# Default variables
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=1

# System deps
RUN pip3 install --upgrade pip
RUN apt update
RUN apt install nano

COPY . /app

# Install dependencies
RUN pip3 install -r /app/requirements.txt

#CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
FROM python:3.10

RUN mkdir -p /src
WORKDIR /app

# Default variables
ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=1

# System deps
RUN pip3 install --upgrade pip
RUN apt update
RUN apt install nano

COPY . /app

# Install dependencies
RUN pip3 install -r /app/requirements.txt

CMD python main.py
