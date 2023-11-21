import requests
import boto3

"""
ファイルを指定したフォルダにS3バケットにアップロードします。

引数:
    file_name (str): アップロードするファイルの名前。
    bucket_name (str): S3バケットの名前。
    folder (str): S3バケット内のフォルダの名前。

戻り値:
    dict: 次のキーと値を含む辞書:
        - 'last_run_file_name' (str): アップロードされたファイルのS3 URL。
        - 'status_code' (int): アップロードの応答のHTTPステータスコード。
"""


def upload_file_to_s3(file_name, bucket_name, folder):
    print(f'Getting the {file_name} from gharchive')
    res = requests.get(f'https://data.gharchive.org/{file_name}')

    print(f'Uploading {file_name} to s3 under s3://{bucket_name}/{folder}')
    s3_client = boto3.client('s3')
    upload_res = s3_client.put_object(
        Bucket=bucket_name,
        Key=f'{folder}/{file_name}',
        Body=res.content
    )

    return {
        'last_run_file_name': f's3://{bucket_name}/{folder}/{file_name}',
        'status_code': upload_res['ResponseMetadata']['HTTPStatusCode']
    }
