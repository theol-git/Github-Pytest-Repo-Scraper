FROM python:3.11-buster
ADD src/ /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD python app.py