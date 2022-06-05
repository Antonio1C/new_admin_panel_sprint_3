FROM python:3.10.4

# set work directory
WORKDIR /home/app

# update pip
RUN pip install --upgrade pip

# install requirements
COPY ./01_etl/requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY ./01_etl .

CMD ["python3", "-m", "etl"]