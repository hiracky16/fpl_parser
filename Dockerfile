FROM python:3
WORKDIR /app
COPY main.py /app
COPY requirements.txt /app

RUN pip install -r requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS /tmp/credentials/credentials.json
ENV RAW_BUCKET fpl-japan-rawfiles
ENTRYPOINT ["python", "main.py"]
