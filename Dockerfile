FROM python:3.8

USER root

COPY ./extraction_app/requirements.txt /requirements.txt
# Install dependencies from requirements.txt
RUN pip install -r /requirements.txt

RUN cd /opt/

# Copy files to working directory
COPY ./extraction_app /opt/extraction_app
WORKDIR /opt/extraction_app/
VOLUME  /opt/extraction_app/

ENV PYTHONPATH "${PYTHONPATH}:/opt/extraction_app/"
