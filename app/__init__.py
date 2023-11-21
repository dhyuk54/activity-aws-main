import os
from util.bookmark import get_job_details, \
    get_next_file_name, save_job_run_details, \
    get_job_start_time
from ghactivity_ingest import upload_file_to_s3
from ghactivity_transform import transform_to_parquet

"""
openソースカラデータを取り込み、S3バケットに保存します。

パラメータ:
    なし

戻り値:
    job_run_details (dict): ジョブ実行の詳細情報をを返す。
"""


def ghactivity_ingest_to_s3():
    bucket_name = os.environ.get('BUCKET_NAME')
    folder = os.environ.get('FOLDER')
    job_details = get_job_details('ghactivity_ingest')
    job_start_time, next_file = get_next_file_name(job_details)
    job_run_details = upload_file_to_s3(next_file, bucket_name, folder)
    save_job_run_details(job_details, job_run_details, job_start_time)
    return job_run_details


"""
データ取り込み処理を実行します。処理された内容(jsonファイルをS3に保存します)。


戻り値:
    dict: ジョブ実行の詳細情報を含む辞書。
"""


def lambda_ingest(event, context):
    job_run_details = ghactivity_ingest_to_s3()

    return {
        'status': 200,
        'body': job_run_details
    }


"""
この関数は、指定されたファイルを変換してParquet形式に変換します。

パラメータ:
    file_name (str): 変換するファイルの名前。

戻り値:
    job_run_details (str): ジョブ実行の詳細情報。
"""


def ghactivity_transform_to_parquet(file_name):
    bucket_name = os.environ.get('BUCKET_NAME')
    tgt_folder = os.environ.get('TARGET_FOLDER')
    job_start_time = get_job_start_time()
    job_details = get_job_details('ghactivity_transform')
    print(f'Job Details: {job_details}')
    job_run_details = transform_to_parquet(file_name, bucket_name, tgt_folder)
    print(f'Job Run Details: {job_run_details}')
    save_job_run_details(job_details, job_run_details, job_start_time)
    return job_run_details


def lambda_transform(event, context):
    file_name = event['jobRunDetails']['last_run_file_name']
    job_run_details = ghactivity_transform_to_parquet(file_name)
    return {
        'statusCode': 200,
        'statusMessage': 'File Transformed Successfully',
        'jobRunDetails': job_run_details
    }


"""
2つのパラメータを受け取って
`event` (dict) - Lambda関数に渡されるイベントデータです。
`context` (LambdaContext) - Lambda関数のランタイム情報です。
イベントデータから `file_name` を抽出し、`file_nameでparquet形式のファイルを変換する`関数`を呼び出します。

戻り値:
dict: ステータスコード、ステータスメッセージ、ジョブ実行の詳細を含む辞書。

- 'statusCode' (int): レスポンスのステータスコード。
- 'statusMessage' (str): ファイル変換の成功を示すメッセージ。
- 'jobRunDetails' (dict): ジョブ実行の詳細。

戻り値:
dict: ステータスコード、ステータスメッセージ、ジョブ実行の詳細を含む辞書。
"""


def lambda_transform_trigger(event, context):
    print(event)
    file_name = event['Records'][0]['s3']['object']['key'].split('/')[-1]
    job_run_details = ghactivity_transform_to_parquet(file_name)
    return {
        'statusCode': 200,
        'statusMessage': 'File Transformed Successfully',
        'jobRunDetails': job_run_details
    }
