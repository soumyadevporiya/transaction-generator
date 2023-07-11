import random
import string
import pandas as pd
from time import sleep
import datetime
from google.cloud import storage
from confluent_kafka import Producer
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket
import json
import fsspec
import gcsfs

if __name__ == '__main__':

    # Read dataframe from a pickle file
    # df_external_table = pd.read_pickle('C:/Users/Satyaki/df_external_table.pkl')
    df_external_table = pd.read_pickle('gs://tran_datagen/df_external_table.pkl')
    # print(df_external_table)

    # df_account_table = pd.read_pickle('C:/Users/Satyaki/df_account_table.pkl')
    df_account_table = pd.read_pickle('gs://tran_datagen/df_account_table.pkl')
    # print(df_account_table)

    external_table_length = len(df_external_table)
    account_table_length = len(df_account_table)

    # print(external_table_length)
    # print(account_table_length)

    list_creditDebit = []
    list_myTranDesc = []
    list_myTranType = []
    list_myAccountNumber = []
    list_otherAccountNum = []
    list_otherAccountHolderName = []
    list_myTranAmount = []
    list_myTranId = []
    list_myTranTimeStamp = []

    # Producer authentication and RBAC roles at bootstrap server
    conf = {'bootstrap.servers': 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092',
            'client.id': socket.gethostname(),
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': 'ZG6PM5MCZNVNUT2Y',
            'sasl.password': 'D8g6m8fQUsMvXyrsM4CJ63Zu6uRIf3xtxs3NsJ6x0y4aMMHdOPmqiie7qH8UDKCP'
            }

    topic = 'transaction'

    NUMBER_OF_SAMPLES = 9_00_00

    consumer = KafkaConsumer('signal', bootstrap_servers=['35.222.33.26:9094'], auto_offset_reset='latest')
    producer_1 = KafkaProducer(bootstrap_servers=['35.222.33.26:9094'], api_version=(0, 10))

    for message in consumer:
        print(message.value)
        producer = Producer(conf)
        start_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        producer_1.send('reply', json.dumps(start_time).encode('utf-8'))
        for i in range(7_00_00_01, NUMBER_OF_SAMPLES + 7_00_00_01):
            creditDebit_rand_int = random.randint(0, 1)
            if creditDebit_rand_int == 0:
                creditDebit = 'C'
                myTranDesc = 'Credit from external party'
                myTranType = 'CWTF'
            else:
                creditDebit = 'D'
                myTranDesc = 'Debit from external party'
                myTranType = 'DWTF'

            # list_creditDebit.append(creditDebit)
            # list_myTranDesc.append(myTranDesc)
            # list_myTranType.append(myTranType)

            account_index = random.randint(0, account_table_length - 1)
            myAccountNumber = df_account_table.loc[account_index]['internal_acct_id']
            # print(myAccountNumber)
            # list_myAccountNumber.append(myAccountNumber)

            external_table_index = random.randint(0, external_table_length - 1)
            otherAccountNum = df_external_table.loc[external_table_index]['ext_acct_id']
            otherAccountHolderName = df_external_table.loc[external_table_index]['ext_customer_name']
            # print(otherAccountNum)
            # print(otherAccountHolderName)
            # list_otherAccountNum.append(otherAccountNum)
            # list_otherAccountHolderName.append(otherAccountHolderName)

            myTranAmount = random.uniform(0.5, 10000.57).__format__('.2f')
            myTranId = i
            myTranTimeStamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "Z"

            # list_myTranAmount.append(myTranAmount)
            # list_myTranId.append(myTranId)
            # list_myTranTimeStamp.append(myTranTimeStamp)
            # sleep(0.000001)

            # print(creditDebit, myAccountNumber, myTranAmount, myTranDesc, myTranId, myTranTimeStamp, myTranType,
            # otherAccountHolderName, otherAccountNum)

            transaction_dict = dict(creditDebit=creditDebit,
                                    myTranAmount=myTranAmount,
                                    myTranDesc=myTranDesc,
                                    myTranId=myTranId,
                                    myTranTimeStamp=myTranTimeStamp,
                                    myTranType=myTranType,
                                    otherAccountHolderName=otherAccountHolderName,
                                    otherAccountNum=otherAccountNum
                                    )
            key = int(myAccountNumber)

            producer.produce(topic=topic, value=json.dumps(transaction_dict).encode('utf-8'),
                             key=json.dumps(key).encode('utf-8'))

        producer.flush()
        end_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        producer_1.send('reply', json.dumps(end_time).encode('utf-8'))
        producer_1.close()
