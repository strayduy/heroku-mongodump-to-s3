#!python2.7

# Standard libs
import argparse
import datetime
import logging
import os
import sys

# Third party libs
import boto
import envoy
import tempdir

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
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=27017)
    parser.add_argument('--db-username', default='')
    parser.add_argument('--db-password', default='')
    parser.add_argument('--backup-prefix', default='')
    args = parser.parse_args()
    
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
                     host=args.db_host,
                     port=args.db_port,
                     username=args.db_username,
                     password=args.db_password)
        logger.info('Dumped Mongo database to local filesystem!')
        return 0

        logger.info('Gzipping Mongo dump...')
        gzipped_mongodump = gzip_mongodump(mongodump_dir)
        logger.info('Gzipped Mongo dump!')

        logger.info('Uploading Mongo dump to S3...')
        upload_mongodump_to_s3(gzipped_mongodump,
                               s3_conn,
                               args.s3_bucket_name,
                               args.backup_prefix)
        logger.info('Uploaded Mongo dump to S3!')

    logger.info('Finished backing up Mongo database to S3!')

    return 0

def do_mongodump(dump_dir,
                 db,
                 host='localhost',
                 port=27017,
                 username='',
                 password=''):
    cmd = 'mongodump ' \
          '--host %(host)s ' \
          '--port %(port)d ' \
          '--db %(db)s ' \
          '--out %(dump_dir)s ' % {
          'host'     : host,
          'port'     : port,
          'db'       : db,
          'dump_dir' : dump_dir}

    logger.debug('Executing: %s' % (cmd))
    logger.debug('(Omitted username and password for security)')

    if username and password:
        cmd += '--username %(username)s --password %(password)s' % {
               'username' : username,
               'password' : password}

    logger.debug(envoy.expand_args(cmd))
    return
    envoy_response = envoy.run(cmd)

    # Check status code to verify that the command succeeded
    if envoy_response.status_code != 0:
        raise Exception(envoy_response.std_err)

def gzip_mongodump(dump_dir):
    now = datetime.datetime.utcnow()

    # Construct file path to the gzipped mongodump
    backup_filename = now.strftime('%Y-%m-%d_%H-%M-%S.gz')
    backup_filepath = os.path.join(dump_dir, backup_filename)

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

if __name__ == '__main__':
    sys.exit(main())
