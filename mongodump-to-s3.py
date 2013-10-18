#!python2.7

# Standard libs
import argparse
import collections
import datetime
import logging
import os
import random
import sys

# Third party libs
import boto
import envoy
import pymongo
import tempdir

# Constants
BACKUP_FILENAME_FORMAT = '%Y-%m-%d_%H-%M-%S.gz'

# Logging
logger = logging.getLogger('mongodump-to-s3')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

def main():
    # Retrieve AWS credentials from environment variables
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
                 description='Backup mongo database via mongodump and store '
                             'the dump in S3')
    parser.add_argument('db_name')
    parser.add_argument('s3_bucket_name')
    parser.add_argument('--db-host', default='localhost:27017')
    parser.add_argument('--db-replica-set', default='')
    parser.add_argument('--db-username', default='')
    parser.add_argument('--db-password', default='')
    parser.add_argument('--backup-prefix', default='')
    parser.add_argument('--max-backups', type=int, default=0)
    parser.add_argument('--require-secondary-read', action='store_true')
    args = parser.parse_args()
    
    # If we're requiring a read from a secondary within a replica set, identify
    # the hostnames for the secondary nodes
    if args.require_secondary_read:
        logger.info('Requiring that we read from a secondary')

        logger.info('Connecting to replica set: %s/%s' % (args.db_replica_set,
                                                          args.db_host))
        try:
            mongo_client = pymongo.MongoReplicaSetClient(args.db_host,
                                                         replicaSet=args.db_replica_set)
            logger.info('Connected to replica set!')
        except Exception as e:
            mongo_client = None
            logger.error('Failed to connect to replica set: %s' % (str(e)))
            logger.error('Exiting...')
            return 1

        secondary_nodes = mongo_client.secondaries

        if not secondary_nodes:
            logger.error('Replica set doesn\'t have any secondaries')
            logger.error('Exiting...')
            return 1

        # The nodes are tuples of (hostname, port)
        # Join them into strings of hostname:port
        secondary_hostnames = [':'.join([str(i) for i in s])
                               for s in secondary_nodes]
        db_host = random.choice(secondary_hostnames)
    else:
        # If we're not reading from a secondary, we'll read from the given
        # hostname
        db_host = args.db_host

    logger.info('Backing up Mongo database to S3...')

    # Connect to S3
    logger.info('Connecting to S3...')
    s3_conn = boto.connect_s3(access_key_id, secret_access_key)
    logger.info('Connected to S3!')

    # Dump the database contents to a temporary directory.
    # The tempdir library will automatically delete this directory and its
    # contents once we leave the scope of this block.
    with tempdir.TempDir() as mongodump_dir:
        logger.info('Dumping Mongo database to local filesystem...')
        do_mongodump(mongodump_dir,
                     args.db_name,
                     host=db_host,
                     username=args.db_username,
                     password=args.db_password)
        logger.info('Dumped Mongo database to local filesystem!')

        # Create a separate temp directory to store the gzipped mongodump
        with tempdir.TempDir() as gzip_dir:
            logger.info('Gzipping Mongo dump...')
            gzipped_mongodump = gzip_mongodump(mongodump_dir, gzip_dir)
            logger.info('Gzipped Mongo dump!')
    
            logger.info('Uploading Mongo dump to S3...')
            upload_mongodump_to_s3(gzipped_mongodump,
                                   s3_conn,
                                   args.s3_bucket_name,
                                   args.backup_prefix)
            logger.info('Uploaded Mongo dump to S3!')

            # Removing old backups
            if args.max_backups > 0:
                logger.info('Removing old backups, '
                            'keeping only the latest %d...' % (
                                args.max_backups))
                removed_backups = remove_old_backups(s3_conn,
                                                     args.s3_bucket_name,
                                                     args.max_backups,
                                                     args.backup_prefix)
                logger.info('Removed %d old backups!' % (len(removed_backups)))

    logger.info('Finished backing up Mongo database to S3!')

    return 0

def do_mongodump(dump_dir,
                 db,
                 host='localhost',
                 username='',
                 password=''):
    cmd = 'mongodump ' \
          '--host %(host)s ' \
          '--db %(db)s ' \
          '--out %(dump_dir)s ' % {
          'host'     : host,
          'db'       : db,
          'dump_dir' : dump_dir}

    logger.debug('Executing: %s' % (cmd))
    logger.debug('(Omitted username and password for security)')

    if username and password:
        cmd += '--username %(username)s --password %(password)s' % {
               'username' : username,
               'password' : password}

    envoy_response = envoy.run(cmd)

    # Check status code to verify that the command succeeded
    if envoy_response.status_code != 0:
        raise Exception(envoy_response.std_err)

def gzip_mongodump(dump_dir, gzip_dir):
    now = datetime.datetime.utcnow()

    # Construct file path to the gzipped mongodump
    backup_filename = now.strftime(BACKUP_FILENAME_FORMAT)
    backup_filepath = os.path.join(gzip_dir, backup_filename)

    cmd = 'tar -zcvf %(backup_filepath)s %(dump_dir)s' % {
          'backup_filepath' : backup_filepath,
          'dump_dir' : dump_dir}

    logger.debug('Executing: %s' % (cmd))

    # Check status code to verify that the command succeeded
    envoy_response = envoy.run(cmd)
    if envoy_response.status_code != 0:
        raise Exception(envoy_response.std_err)

    return backup_filepath

def upload_mongodump_to_s3(gzipped_mongodump,
                           s3_conn,
                           bucket_name,
                           backup_prefix=''):
    # Get S3 bucket
    bucket = s3_conn.get_bucket(bucket_name)

    k = boto.s3.key.Key(bucket)

    # Prepend an optional prefix to the mongodump filename
    k.key = os.path.join(backup_prefix,
                         os.path.basename(gzipped_mongodump)).lstrip('/')

    # Do the upload
    k.set_contents_from_filename(gzipped_mongodump,
                                 cb=s3_upload_progress,
                                 num_cb=10)

# Upload callback that logs transfer progress
def s3_upload_progress(so_far, total):
    so_far_formatted = '{:,}'.format(so_far)
    total_formatted = '{:,}'.format(total)
    percent = '%.1f%%' % (float(so_far)/total*100)
    logger.debug('%s/%s bytes transferred (%s)' % (
                 so_far_formatted, total_formatted, percent))

def remove_old_backups(s3_conn, bucket_name, max_backups, backup_prefix=''):
    removed_backups = []

    bucket = s3_conn.get_bucket(bucket_name)

    if backup_prefix:
        filename_format = '%s/%s' % (backup_prefix, BACKUP_FILENAME_FORMAT)
    else:
        filename_format = BACKUP_FILENAME_FORMAT

    Backup = collections.namedtuple('Backup', ['key', 'date'])

    # Aggregate a list of all the stored backups
    all_backups = []
    for key in bucket.list(prefix=backup_prefix):
        try:
            backup_date = datetime.datetime.strptime(key.key, filename_format)
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
