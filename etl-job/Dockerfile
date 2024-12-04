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
           python3-dev \
           unzip \
           vim \
           automake \
           libtool \
	   wget

RUN rm -rf /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /root

SHELL ["/bin/bash", "-c"]
RUN python3 -m venv venv
RUN source venv/bin/activate && pip install wheel
RUN mkdir ~/.gen3

ENV PYTHONUNBUFFERED=1
COPY ./requirements.txt /root
RUN source venv/bin/activate && pip install -r requirements.txt

# move these out of requirements.txt, so that builds are quicker when we change
RUN source venv/bin/activate && pip install "gen3_tracker==0.0.7rc3"
RUN source venv/bin/activate && pip install "aced-submission==0.0.10rc5"
ENV PATH="/usr/local/bin:$PATH"


COPY . /root
RUN chmod +x /root/entrypoint.sh

RUN mkdir /root/studies
RUN ls -la /root
RUN pwd
RUN ls -l /root

ENTRYPOINT [ "/root/entrypoint.sh" ]
CMD [ "python", "fhir_import_export.py" ]
