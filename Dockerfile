FROM python:3.9

WORKDIR C:/Users/David/Documents/ibmChallenge/pymethod/app

COPY requierments.txt .

RUN pip install -r requierments.txt

COPY . .d

#CMD [ "python", "./app.py"]