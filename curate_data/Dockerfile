FROM cgodiksen/modelardb

RUN apt-get update

RUN apt-get install -y python3-pip

WORKDIR /usr/src/app

COPY ./requirements.txt /usr/src/app/requirements.txt

RUN pip install --no-cache-dir --upgrade --break-system-packages -r /usr/src/app/requirements.txt

COPY ./main.py /usr/src/app/crates/modelardb_embedded/bindings/python

CMD ["python3", "/usr/src/app/crates/modelardb_embedded/bindings/python/main.py"]
