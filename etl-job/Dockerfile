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
RUN source venv/bin/activate && pip install "iceberg_tools>=0.0.8rc3"
RUN source venv/bin/activate && pip install "gen3_tracker==0.0.5rc13"
RUN source venv/bin/activate && pip install "aced-submission==0.0.9rc37"
    
RUN git clone https://github.com/bmeg/iceberg.git && \
	cd iceberg && \
	git checkout feature/FHIR-resource-type

#Add jsonschemagraph exe to image
RUN wget https://github.com/bmeg/jsonschemagraph/releases/download/v0.0.2/jsonschemagraph-linux.amd64 -P /usr/local/bin/
RUN mv /usr/local/bin/jsonschemagraph-linux.amd64 /usr/local/bin/jsonschemagraph
RUN chmod +x /usr/local/bin/jsonschemagraph
ENV PATH="/usr/local/bin:$PATH"


COPY . /root
RUN chmod +x /root/entrypoint.sh

RUN mkdir /root/studies
RUN ls -la /root
RUN pwd
RUN ls -l /root

ENTRYPOINT [ "/root/entrypoint.sh" ]
CMD [ "python", "fhir_import_export.py" ]
