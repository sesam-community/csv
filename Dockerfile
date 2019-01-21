FROM python:3-alpine

LABEL Maintainer="Egemen Yavuz <melih.egemen.yavuz@sysco.no>"

RUN pip install --upgrade pip

RUN apk add --update curl gcc g++ && rm -rf /var/cache/apk/*
RUN ln -s /usr/include/locale.h /usr/include/xlocale.h
RUN pip install numpy
RUN pip install bottle cython pandas

COPY ./service/requirements.txt /service/requirements.txt
RUN pip install -r /service/requirements.txt
COPY ./service /service

EXPOSE 5000/tcp

CMD ["python3", "-u", "./service/service.py"]
