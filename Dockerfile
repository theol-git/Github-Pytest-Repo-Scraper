FROM python:3.11-buster
ADD src/ /code
ADD requirements.txt .
RUN pip install -r requirements.txt
WORKDIR /code
CMD python main.py