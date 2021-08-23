FROM alpine:3.7

RUN apk update \
&& apk upgrade \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
&& apk add --no-cache openjdk8-jre

RUN apk add --no-cache python3 \
&& python3 -m ensurepip \
&& pip3 install --upgrade pip setuptools \
&& rm -r /usr/lib/python*/ensurepip && \
if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
rm -r /root/.cache

RUN mkdir -p /src
COPY requirements.txt /src
COPY src /src
RUN pip3 install --no-cache-dir -r /src/requirements.txt

ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk"
ENV PYSPARK_PYTHON="python3"
EXPOSE 4040

ENTRYPOINT ["python", "/src/process_transactions.py", \
            "--input-accounts", "/src/data/accounts/*", \
            "--input-cards", "/src/data/cards/*", \
            "--input-saving-accounts", "/src/data/savings_accounts/*", \
            "--wait-for", "600"]