# set base image (host OS)
FROM python:3.12-bookworm

# install dependencies
RUN apt-get -y update
RUN apt-get -y --no-install-recommends install  \
           build-essential \
           curl \
           gcc \
           git \
           iputils-ping \
           jq \
           libmagic-dev \
           libpq-dev \
           postgresql-client \
           python3-dev \
           unzip \
           vim \
           automake \
           libtool \
	   wget \
           ca-certificates

RUN rm -rf /var/lib/apt/lists/*

ENV GOPATH=/go
ENV PATH=$GOPATH/bin:/usr/local/go/bin:$PATH
RUN wget https://go.dev/dl/go1.22.5.linux-amd64.tar.gz && \ 
	tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz && \ 
	rm go1.22.5.linux-amd64.tar.gz

RUN mkdir -p $GOPATH/src $GOPATH/bin

# set the working directory in the container
WORKDIR /root

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm awscliv2.zip

SHELL ["/bin/bash", "-c"]
RUN python3 -m venv venv
RUN source venv/bin/activate && pip install wheel
RUN source venv/bin/activate && pip install yq

RUN mkdir ~/.aws
RUN mkdir ~/.gen3

ENV PYTHONUNBUFFERED=1
COPY ./requirements.txt /root
RUN source venv/bin/activate && pip install -r requirements.txt

# move these out of requirements.txt, so that builds are quicker when we change
RUN source venv/bin/activate && pip install "iceberg_tools>=0.0.q8rc3"
RUN source venv/bin/activate && pip install "gen3_tracker==0.0.5rc2"
RUN source venv/bin/activate && pip install "aced-submission==0.0.9rc26"
ENV GOPROXY=direct
RUN git clone https://github.com/bmeg/jsonschemagraph.git && \
	cd jsonschemagraph && \
	git checkout fix/duplicate-edges && \
	go build . && \
	go install .
    
RUN git clone https://github.com/bmeg/iceberg.git && \
	cd iceberg && \
	git checkout feature/FHIR-resource-type

COPY . /root
RUN chmod +x /root/entrypoint.sh

RUN mkdir /root/studies
RUN ls -la /root
RUN pwd
RUN ls -l /root

# TODO - should this belong in etl pod repo / helm?
RUN curl  https://raw.githubusercontent.com/bmeg/iceberg-schema-tools/development/config.yaml -o config.yaml
#ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

ENTRYPOINT [ "/root/entrypoint.sh" ]
CMD [ "python", "fhir_import_export.py" ]
