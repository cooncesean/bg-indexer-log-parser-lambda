# Overview
An AWS Lambda function definition that listens to new log files posted to BitGo's indexer health s3 bucket.

The Lambda parses the most recent file `latest.json` to see if any of BitGo's indexers are behind chain head. If they are, take a look at the 3 most recently posted log files to see if that same indexer has been behind chain head for an extended period of time.

If an indexer has fallen behind chain head for more than ~5 minutes (3 log files), notify the team.

# How It Works

1. The Lambda fcn defined in this project listens to an update to this s3 file: https://s3-us-west-2.amazonaws.com/bitgo-indexer-health/latest.json
2. If the script detects that an indexer has fallen behind chain head, it looks back through the 3 most recently posted log files to see if that same indexer has had historical issues.
3. If the indexer has been behind chain head for more than 3 consecutive log files, we send an SNS message to the team.

# References
This work builds on the lambda that produces these log files: https://github.com/cooncesean/bg-indexer-health-lambda
