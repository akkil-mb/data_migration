import json, os, pprint, pickle
from io import StringIO

import pandas as pd
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text

path_to_json = "/Users/akkil/Documents/ak_work/git/data_migration/companyfacts"
def get_json_data(path_to_json):
    json_files = [file for file in os.listdir(path_to_json) if file.endswith('.json')]
    json_data = []
    count = 1
    for index, js_file in enumerate(json_files):
        # print(index, js_file)
        with open(os.path.join(path_to_json, js_file)) as json_file:
            json_text = json.load(json_file)
            json_data.append(json_text)
        count += 1
        if (count % 200) == 0:
            print('--------------')
            df = pd.DataFrame(json_data)
            df.to_csv(f'/Users/akkil/Documents/ak_work/git/data_migration/exportdata/file-{count}.csv')
            json_data = []
        return json_text

def upload_file(file_path, bucket):
    object_name = file_path.split('/')[-1]
    print(object_name)
    s3_client = boto3.client('s3',
                             aws_access_key_id="AKIAW3G5MIQQUISDUJPP",
                             aws_secret_access_key="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")
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def read_csv(file_path, bucket):
    # df = pd.read_csv("/Users/akkil/Documents/ak_work/git/data_migration/exportdata/file-200.csv")
    # print(df.columns)
    client = boto3.client('s3',
                          aws_access_key_id="AKIAW3G5MIQQUISDUJPP",
                          aws_secret_access_key="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"
                          )
    object_key = file_path.split('/')[-1]
    print(f"Start reading CSV {object_key} from S3")
    csv_obj = client.get_object(Bucket=bucket, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    print(csv_string)
    df = pd.read_csv(StringIO(csv_string))
    print("returning df from S3")
    print(df.columns)
    df = df[['cik', 'entityName', 'facts']]
    return df


# PYTHON FUNCTION TO CONNECT TO THE MYSQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection():
    user = 'admin'
    password = 'mbakkil21'
    host = 'database-1.c12y68w4usdu.us-east-1.rds.amazonaws.com'
    port = 3306
    database = 'testDB'

    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )


def get_mysql_engine():
    mysql_engine = get_connection()
    # with mysql_engine.connect() as connection:
    #     result = connection.execute(text("SHOW tables;"))
    #     for row in result.mappings():
    #         print(row)
    print("returning mysql engine")
    return mysql_engine


def write_df_to_db(df, mysql_engine):
    print("start writing to DB")
    table_name = "test_table"
    df.to_sql(table_name, mysql_engine, if_exists='append', index=False)


# get_json_data(path_to_json)
file_path = "/Users/akkil/Documents/ak_work/git/data_migration/exportdata/file-200.csv"
bucket = 'ak-data-migration'
# upload_file(file_path, bucket)
engine = get_connection()
df = read_csv(file_path, bucket)
write_df_to_db(df, engine)


















































# print(company_facts_df)
# with open('company_facts_dict.json', 'w') as f:
#     json.dump(company_facts_df, f)

#
#
# def get_df_from_json(json_text):
#     # print(json.dumps(json_data, indent=2))
#     # for json_text in json_data:
#     df = pd.DataFrame.from_dict(json_text, orient='columns')
#     pprint.pprint(json_text)
#     print(df)

        # json_to_dict = {}
        # entity_share_outstanding = json_text['facts']['dei']['EntityCommonStockSharesOutstanding']
        # entity_public_float = json_text['facts']['dei']['EntityPublicFloat']
        # unit_shares_dict = {}
        # unit_usd_dict = {}
        #
        # for idx, unit_share in enumerate(entity_share_outstanding['units']['shares'], start=1):
        #     unit_share = {idx: unit_share}
        #     unit_shares_dict.update(unit_share)
        # for idx, unit_usd in enumerate(entity_public_float['units']['USD'], start=1):
        #     unit_usd = {idx: unit_usd}
        #     unit_usd_dict.update(unit_usd)
        #
        # json_to_dict['cik'] = json_text['cik']
        # share_outstanding_dict = {
        #     'label': entity_share_outstanding['label'],
        #     'description': entity_share_outstanding['description'],
        #     'unit_shares': unit_shares_dict
        # }
        # entity_public_dict = {
        #     'label': entity_public_float['label'],
        #     'description': entity_public_float['description'],
        #     'unit_usd': unit_usd_dict
        # }
        #
        # json_to_dict['EntityCommonStockSharesOutstanding'] = share_outstanding_dict
        # json_to_dict['EntityPublicFloat'] = entity_public_dict
        #
        # us_gaap_dict = {}
        # for key, value in json_text['facts']['us-gaap'].items():
        #     segement_dict = {key: {
        #                             'label': value['label'],
        #                             'description': value['description']
        #                           }}
        #     all_unit_dict = {}
        #     unitname = 'None'
        #     for unit_name in value['units']:
        #         unitname = unit_name
        #         for idx, unit_value in enumerate(value['units'][unit_name], start=1):
        #             each_unit = {idx : unit_value}
        #             all_unit_dict.update(each_unit)
        #     segement_dict[key]['units'] = {unitname: all_unit_dict}
        #     us_gaap_dict.update(segement_dict)
        #
        # json_to_dict['us_gaap'] = us_gaap_dict
        # # pprint.pprint(json_to_dict)
