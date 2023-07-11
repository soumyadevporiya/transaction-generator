FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./transaction_generator.py ./transaction_generator.py
CMD ["python3","./transaction_generator.py"]
