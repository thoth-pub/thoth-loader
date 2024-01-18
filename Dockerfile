FROM python:3.10

WORKDIR /usr/src/app

COPY ./requirements.txt ./
COPY ./data/RangeMessage.xml ./
RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

RUN python3 /usr/local/lib/python3.10/site-packages/isbn_hyphenate/isbn_xml2py.py ./RangeMessage.xml > /usr/local/lib/python3.10/site-packages/isbn_hyphenate/isbn_lengthmaps.py

ADD ./ ./
