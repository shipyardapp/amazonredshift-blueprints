from sqlalchemy import create_engine, text
import argparse
import os
from sqlalchemy.engine.url import URL


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=False)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--host', dest='host', required=False)
    parser.add_argument('--database',
                        dest='database', required=False)
    parser.add_argument('--port', dest='port', default='5439', required=False)
    parser.add_argument(
        '--db-connection-url',
        dest='db_connection_url',
        required=False)
    parser.add_argument(
        '--url-parameters',
        dest='url_parameters',
        required=False)
    parser.add_argument('--query', dest='query', required=True)
    args = parser.parse_args()

    if not args.db_connection_url and not (
            args.host or args.database or args.username) and not os.environ.get('DB_CONNECTION_URL'):
        parser.error(
            """This Blueprint requires at least one of the following to be provided:\n
            1) --db-connection-url\n
            2) --host, --database, and --username\n
            3) DB_CONNECTION_URL set as environment variable""")
    if args.host and not (args.database or args.username):
        parser.error(
            '--host requires --database and --username')
    if args.database and not (args.host or args.username):
        parser.error(
            '--database requires --host and --username')
    if args.username and not (args.host or args.username):
        parser.error(
            '--username requires --host and --username')
    return args


def create_connection_string(args):
    """
    Set the database connection string as an environment variable using the keyword arguments provided.
    This will override system defaults.
    """
    if args.db_connection_url:
        os.environ['DB_CONNECTION_URL'] = args.db_connection_url
    elif (args.host and args.username and args.database):
        os.environ['DB_CONNECTION_URL'] = f'postgresql://{args.username}:{args.password}@{args.host}:{args.port}/{args.database}?{args.url_parameters}'

    db_string = os.environ.get('DB_CONNECTION_URL')
    return db_string

def create_connection_url(host:str, password:str, user:str, database:str,port = 5439):
    url = URL.create(drivername= 'redshift+redshift_connector', host = host, password= password, username= user, port = port, database=database)
    return url

def main():
    args = get_args()
    query = text(args.query)
    database = args.database
    host = args.host
    password = args.password
    user = args.username
    port = args.port

    # db_string = create_connection_string(args)
    db_string = create_connection_url(host = host , password = password, user = user, database = database, port = port)
    try:
        db_connection = create_engine(db_string)
    except Exception as e:
        print(f'Failed to connect to database {database}')
        raise(e)
    db_connection.execute(query)
    db_connection.dispose()
    print('Your query has been successfully executed.')


if __name__ == '__main__':
    main()
