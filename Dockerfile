FROM tiangolo/uwsgi-nginx-flask:flask-python3.5

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ADD ./app /app


