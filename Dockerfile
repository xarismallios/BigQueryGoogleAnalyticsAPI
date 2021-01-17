FROM python:alpine3.7
MAINTAINER Xaris Mallios "mallioscharis@gmail.com"
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8080
CMD python ./app.py
