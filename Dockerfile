FROM python:3.8

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

ADD ./ ./

EXPOSE 8080

CMD ["python", "server.py"]
