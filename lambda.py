import json
import urllib

import boto3
from botocore.vendored import requests


def lambda_handler(event, context):
    """
    Listens to new log files written to the `bitgo-indexer-health` bucket.
    Parses the newly uploaded file; if any indexers are behind chain head, the
    fcn looks back at the last 3 log files to see if the same indexer has been
    behind chain head for an extended period of time. If so, the fcn notifies
    the team about the issue.
    """
    # The number of log files to look through to determine whether or not an
    # indexer has been down for some length of time (and needs to be investigated)
    LOGS_TO_LOOK_THROUGH = 4

    # Ignore updates to `latest.json` ... only parse timestamped log files
    object_key_name = event['Records'][0]['s3']['object']['key']
    print(object_key_name)
    if object_key_name == 'latest.json':
        print('Do not parse `latest.json`')
        return

    # Get the most recently uploaded log file and check if there are any
    # indexers behind chain head
    res = requests.get('https://s3-us-west-2.amazonaws.com/bitgo-indexer-health/{}'.format(
        object_key_name
    ))
    log_data = json.loads(res.content)
    print(log_data)

    # Iterate over each coin + network (environment) to see if any are behind
    # chain head.
    #
    # Construct a dictionary that looks like:
    # {
    #   'XRP': {
    #     'MainNet': 0  # zero, being the number of consecutive times the coin has fallen behind chain head
    #   }
    # }
    indexers_behind_chain_head = {}
    for coin, indexer_data in log_data.get('indexers', {}).items():
        for env in indexer_data['environments']:
            if env.get('status', True) is False:
                indexers_behind_chain_head.setdefault(coin, {})
                indexers_behind_chain_head[coin].setdefault(env['network'], 0)

    # If no indexers were behind chain head, bail; no need to alert the team
    if len(indexers_behind_chain_head) == 0:
        print('No coins were behind chain head, great success.')
        return

    # However, if any indexer is behind chain head, look back at the 3 most
    # recently published log files to see if that same indexer has been
    # historically behind so we can notify the team
    print('Some indexers are behind chain head!!')
    print(indexers_behind_chain_head)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("bitgo-indexer-health")

    # This is hacky, but boto3 does not allow you to access a buckets contents in
    # descending order .... so,
    #
    # Get a prefix based on the current file path that triggered the lambda which
    # allows us to capture all log files for the current hour (obviously this
    # will fail if the log file we are processing is the first one within the current hour)
    #
    # ie: Convert this path, '2019-01-21 11:01:12.570522-08:00.json' to this
    # prefix, '2019-01-21 11', to use for fetching a constrained list of keys
    # which we can then sort in ram.
    decoded_key_name = urllib.parse.unquote(object_key_name).replace('+', ' ')
    shallow_prefix = decoded_key_name.split('.')[0].rsplit(':', 2)[0]

    objs = bucket.objects.filter(Prefix=shallow_prefix).limit(31)
    get_last_modified = lambda obj: obj.last_modified
    sorted_log_file_s3_objects = [obj for obj in sorted(objs, key=get_last_modified, reverse=True)][0:LOGS_TO_LOOK_THROUGH]

    # Iterate over the 4(ish) most recently published logs to see if the offending
    # coins + networks (currently behind chain head) have been historically
    # behind chain head during consecutive status checks.
    for obj in sorted_log_file_s3_objects:
        historical_log_data = json.loads(obj.get()['Body'].read())
        for coin, historical_status_data in historical_log_data['indexers'].items():
            if coin not in indexers_behind_chain_head:
                continue

            # If the coin IS in the indexers_behind_chain_head dict, look at which
            # network the failure was logged (MainNet vs TestNet) and incr the
            # count (ie: the number of consecutive times that the indexer has
            # fallen behind chain head).
            for env in historical_status_data['environments']:
                if env['network'] in indexers_behind_chain_head[coin]:
                    indexers_behind_chain_head[coin][env['network']] += 1

    print(indexers_behind_chain_head)

    # Iterate over each indexer + network that has consecutively fallen behind
    # chain head more than 4 times and alert the team
    for coin, network in indexers_behind_chain_head.items():
        for env, consecutive_failures in network.items():
            print('Indexer: {} on Environment: {} has been behind chain head more than {} consecutive times.'.format(
                coin, env, consecutive_failures)
            )
