# FROM python:3.10-slim-bookworm
FROM public.ecr.aws/docker/library/python:3.10-slim
 
ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT
 
 
ENV PYTHONFAULTHANDLER=1
ENV PYTHONUNBUFFERED=1
 
RUN pip install --no-cache-dir cython
 
RUN apt update && apt install -y ca-certificates --no-install-recommends \
    gcc g++ make git libc-dev curl proj-bin libproj-dev procps \
    gdal-bin gdal-data awscli postgresql-client jq && \
    rm -rf /var/lib/apt/lists/*
 
 
#### Todo: For development only
# Copy cert to container store
COPY certs/Zscaler-ca.pem /usr/local/share/ca-certificates/root-ca.crt
# Update system certificate trust store
# Scan certs in '/usr/local/share/ca-certificates/certs/*.crt' and adds to '/etc/ssl/certs/ca-certificates.crt'
RUN update-ca-certificates
# Point git to updated certificate bundle
RUN git config --system http.sslCAInfo /etc/ssl/certs/ca-certificates.crt
# Skip verification
ENV GIT_SSL_NO_VERIFY=1
#### Todo: For development only
 
 
COPY request-processor/requirements/requirements.txt requirements/requirements.txt
COPY request-processor/makefile makefile
COPY request-processor/makerules/makerules.mk makerules/makerules.mk
COPY request-processor/makerules/python.mk makerules/python.mk
RUN make init
 
 
COPY .git/ .
COPY request-processor/src/. .
 
COPY ../request_model ./request_model
COPY ../task_interface ./task_interface
 
COPY request-processor/docker-healthcheck.sh docker-healthcheck.sh
 
COPY request-processor/healthcheck-output-template.json healthcheck-output-template.json
 
HEALTHCHECK CMD "./docker-healthcheck.sh"
 
COPY request-processor/docker-entrypoint.sh docker-entrypoint.sh
 
ENTRYPOINT ["./docker-entrypoint.sh"]