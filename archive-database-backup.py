#!python2.7

# Standard libs
import argparse
import collections
from datetime import datetime
from datetime import timedelta
import logging
import os
import re
import sys

# Third party libs
import boto

# Constants
BACKUP_FILENAME_FORMAT = '%Y-%m-%d_%H-%M-%S.gz'

# Logging
logger = logging.getLogger('archive-database-backup')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

def main():
    # Retrieve parameters from environment variables
    access_key_id     = os.getenv('AWS_ACCESS_KEY_ID')
    secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not access_key_id:
        logger.error('AWS_ACCESS_KEY_ID environment variable must be set')
        return 1
    if not secret_access_key:
        logger.error('AWS_SECRET_ACCESS_KEY environment variable must be set')
        return 1

    # Retrieve arguments from command line
    parser = argparse.ArgumentParser(
                 description='Archive a database backup by copying it to '
                             'another bucket in S3')
    parser.add_argument('bucket_name')
    parser.add_argument('source_dir')
    parser.add_argument('dest_dir')
    parser.add_argument('mode', choices=['daily', 'monthly'])
    parser.add_argument('--max-backups', type=int, default=0)
    args = parser.parse_args()

    # Normalize source and destination directory paths
    source_dir = args.source_dir.rstrip('/') + '/'
    dest_dir = args.dest_dir.rstrip('/') + '/'

    # Connect to S3 bucket
    s3 = boto.connect_s3(access_key_id, secret_access_key)
    bucket = s3.get_bucket(args.bucket_name)

    # See if we have a backup file from this period in the destination directory
    backup_file = get_backup_from_this_period(bucket, dest_dir, args.mode)

    # If we already have a backup from this period, we can bail here
    if backup_file:
        logger.info('Already have a backup from this period')
        return 0

    # Otherwise, try to find a backup from this period in the source directory
    backup_file = get_backup_from_this_period(bucket, source_dir, args.mode)

    # If we didn't find a backup from this period, give up
    if not backup_file:
        logger.warn("Didn't find a backup from this period")
        return 1

    # Otherwise, copy the backup from the source dir to the destination dir
    dest_filename = re.sub(source_dir, dest_dir, backup_file.name)
    backup_file.copy(args.bucket_name, dest_filename)

    # Only keep a certain number of backups
    if args.max_backups > 0:
        logger.info('Removing old backups, '
                    'keeping only the latest %d...' % (args.max_backups))
        removed_backups = remove_old_backups(bucket, args.max_backups, backup_prefix=dest_dir)
        logger.info('Removed %d old backups!' % (len(removed_backups)))

    return 0

def get_backup_from_this_period(bucket, dir_name, mode):
    now = datetime.utcnow()

    if mode == 'daily':
        pattern = r'%s%s-%s-%s_\d{2}-\d{2}-\d{2}.gz' % (dir_name,
                                                        now.strftime('%Y'),
                                                        now.strftime('%m'),
                                                        now.strftime('%d'))
    elif mode == 'monthly':
        pattern = r'%s%s-%s-\d{2}_\d{2}-\d{2}-\d{2}.gz' % (dir_name,
                                                           now.strftime('%Y'),
                                                           now.strftime('%m'))
    else:
        raise Exception('Invalid mode')

    for backup_file in bucket.list(prefix=dir_name):
        if re.match(pattern, backup_file.name):
            return backup_file

    return None

def remove_old_backups(bucket, max_backups, backup_prefix=''):
    removed_backups = []

    if backup_prefix:
        filename_format = '%s/%s' % (backup_prefix.rstrip('/'), BACKUP_FILENAME_FORMAT)
    else:
        filename_format = BACKUP_FILENAME_FORMAT

    Backup = collections.namedtuple('Backup', ['key', 'date'])

    # Aggregate a list of all the stored backups
    all_backups = []
    for key in bucket.list(prefix=backup_prefix):
        try:
            backup_date = datetime.strptime(key.key, filename_format)
        except ValueError:
            pass
        else:
            backup = Backup(key.key, backup_date)
            all_backups.append(backup)

    # Sort backups in reverse chronological order
    all_backups.sort(key=lambda x: x.date, reverse=True)

    # Keep the N most recent backups and remove the rest
    removed_backups = all_backups[max_backups:]
    for backup in removed_backups:
        bucket.delete_key(backup.key)

    return removed_backups

if __name__ == '__main__':
    sys.exit(main())

