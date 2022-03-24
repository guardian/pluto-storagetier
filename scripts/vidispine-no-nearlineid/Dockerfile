FROM python:3.9-alpine

COPY gnmvidispine-1.9.DEV.tar.gz /tmp
RUN cd /tmp && tar xzf gnmvidispine-1.9.DEV.tar.gz && cd gnmvidispine-1.9.DEV && python3 ./setup_py3.py install && cd / && rm -rf /tmp/gnmvidispine-1.9.DEV

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

COPY requeue-missing-nearlineid.py /usr/local/bin/requeue-missing-nearlineid.py
USER daemon
CMD /usr/local/bin/requeue-missing-nearlineid.py