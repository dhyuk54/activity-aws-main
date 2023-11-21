from datetime import datetime as dt
from datetime import timedelta as td
import time

import boto3

"""
'jobs' DynamoDB テーブルからジョブの詳細を取得します。

パラメータ:
    job_name (str): 詳細を取得するジョブの名前。

戻り値:
    dict: ジョブの詳細を含む辞書。
"""


def get_job_details(job_name):
    # dynamodbテーブルを作成
    dynamodb = boto3.resource('dynamodb')
    # dynamodbテーブルにjobというキーを割り当て
    table = dynamodb.Table('jobs')
    # job_idで特定されたjobの詳細をデータベースから取得
    job_details = table.get_item(Key={'job_id': job_name})['Item']
    return job_details


"""
ジョブの開始時刻を取得します。

:return: ジョブの開始時刻を Unix タイムスタンプとして返します。
:rtype: int
"""


def get_job_start_time():
    return int(time.mktime(dt.now().timetuple()))


"""
ジョブの詳細をもとに、次のファイル名を生成します。

パラメータ:
    job_details (dict): ジョブの詳細情報を含む辞書。

戻り値:
    tuple: ジョブの開始時刻と次のファイル名を含むタプル。
"""


def get_next_file_name(job_details):
    job_start_time = get_job_start_time()
    job_run_bookmark_details = job_details.get('job_run_bookmark_details')
    baseline_days = int(job_details['baseline_days'])
    # job_run_bookmark_detailsがあれば 前回の作業のファイル名を取得する
    # ファイル名は YYYY-MM-DD-0.json.gz
    if job_run_bookmark_details:
        # job_run_bookmark_details は YYYY-MM-DD-0.json.gz の形式formatする
        dt_part = job_run_bookmark_details['last_run_file_name'].split('.')[0].split('/')[-1]
        next_file_name = f"{dt.strftime(dt.strptime(dt_part, '%Y-%m-%d-%H') + td(hours=1), '%Y-%m-%d-%-H')}.json.gz"
    else:
        # 前回の作業がなければ今日の作業のファイル名を取得する
        # ファイル名は YYYY-MM-DD-0.json.gz
        next_file_name = f'{dt.strftime(dt.now().date() - td(days=baseline_days), "%Y-%m-%d")}-0.json.gz'
    return job_start_time, next_file_name


"""
ジョブの実行の詳細を保存します。

パラメータ:
    job_details (dict): ジョブの詳細情報。
    job_run_details (dict): ジョブの実行の詳細情報。
    job_start_time (datetime): ジョブの実行の開始時刻。

戻り値:
    None
"""


def save_job_run_details(job_details, job_run_details, job_start_time):
    dynamodb = boto3.resource('dynamodb')
    job_run_details_item = {
        'job_id': job_details['job_id'],
        'job_run_time': job_start_time,
        'job_run_bookmark_details': job_run_details,
        'create_ts': int(time.mktime(dt.now().timetuple()))
    }
    job_run_details_table = dynamodb.Table('job_run_details')
    job_run_details_table.put_item(Item=job_run_details_item)

    job_details_table = dynamodb.Table('jobs')
    job_details['job_run_bookmark_details'] = job_run_details
    job_details_table.put_item(Item=job_details)
