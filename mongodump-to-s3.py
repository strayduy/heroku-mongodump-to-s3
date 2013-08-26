#!python2.7

# Standard libs
import argparse
import datetime
import logging
import os

# Third party libs
import envoy
import tempdir

# Logging
logger = logging.getLogger('mongodump-to-s3')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

def main():
    # Retrieve arguments from command line
    parser = argparse.ArgumentParser(
                 description='Backup mongo database via mongodump and store '
                             'the dump in S3')
    parser.add_argument('db_name')
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=27017)
    parser.add_argument('--db-username', default='')
    parser.add_argument('--db-password', default='')
    args = parser.parse_args()
    
    logger.info('Backing up Mongo database to S3...')

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

        logger.info('Gzipping Mongo dump...')
        gzip_mongodump(mongodump_dir)
        logger.info('Gzipped Mongo dump!')

    logger.info('Finished backing up Mongo database to S3!')

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
        cmd += '--username %(username)s --password %(password)s' % (
               username, password)

    envoy_response = envoy.run(cmd)

def gzip_mongodump(dump_dir):
    now = datetime.datetime.utcnow()

    # Construct file path to the gzipped mongodump
    backup_filename = now.strftime('%Y-%m-%d_%H-%M-%S.gz')
    backup_filepath = os.path.join('/tmp', backup_filename)

    cmd = 'tar -zcvf %(backup_filepath)s %(dump_dir)s' % {
          'backup_filepath' : backup_filepath,
          'dump_dir' : dump_dir}

    logger.debug('Executing: %s' % (cmd))

    envoy_response = envoy.run(cmd)

if __name__ == '__main__':
    main()
