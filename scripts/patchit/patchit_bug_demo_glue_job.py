import json


def run_job(event):
    payload = json.loads(event)
    # Intentional bug: misspelled key 'records'
    rows = payload['records']
    return {'count': len(rows)}


if __name__ == '__main__':
    sample = json.dumps({'records': [1, 2, 3]})
    print(run_job(sample))
