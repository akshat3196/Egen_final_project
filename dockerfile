FROM python:3.7.3-stretch
LABEL maintainer="AKSHAT SHARMA<akshatsharma3107@gmail.com>"
WORKDIR /usr/src/app

COPY Twitter_Pubsub_v2.py .
COPY requirements.txt .
COPY secret.json .
COPY twitter_api_cred.json .
ENV PORT 8080
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./Twitter_Pubsub_v2.py"]
