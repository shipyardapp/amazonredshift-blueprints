import argparse
import glob
import os
import re

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=False)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--host', dest='host', required=False)
    parser.add_argument('--database', dest='database', required=False)
    parser.add_argument('--port', dest='port', default='5439', required=False)
    parser.add_argument(
        '--url-parameters',
        dest='url_parameters',
        required=False)
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        default='exact_match',
                        choices={
                            'exact_match',
                            'regex_match'},
                        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--table-name',
        dest='table_name',
        default=None,
        required=True)
    parser.add_argument(
        '--insert-method',
        dest='insert_method',
        choices={
            'fail',
            'replace',
            'append'},
        default='append',
        required=False)
    parser.add_argument(
        '--db-connection-url',
        dest='db_connection_url',
        required=False)
    parser.add_argument('--schema', dest='schema', required=False, default='')
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


def create_connection_url(host: str, password: str, user: str, database: str, port=5439):
    url = URL.create(drivername='redshift+redshift_connector', host=host, password=password, username=user, port=port,
                     database=database)
    return url


def find_all_local_file_names(source_folder_name):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f'{cwd}/{source_folder_name}/**')
    file_names = glob.glob(cwd_extension, recursive=True)
    return [file_name for file_name in file_names if os.path.isfile(file_name)]


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def upload_data(source_full_path, table_name, insert_method, db_connection, schema=None):
    # Resort to chunks for larger files to avoid memory issues.
    chunk_size = 10000

    conn = db_connection.connect()
    if schema:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for index, chunk in enumerate(pd.read_csv(source_full_path, chunksize=chunk_size)):
        '''
        For the first iteration of the loop, we will either drop the table or append to it based on the user's input.
        The following iterations will always append to the table.
        '''
        if index != 0:
            insert_method = 'append'
        if schema:
            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                schema=schema,
                if_exists=insert_method,
                method='multi',
                chunksize=chunk_size
            )
        elif not schema:
            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                if_exists=insert_method,
                method='multi',
                chunksize=chunk_size
            )
    if schema:
        print(f'{source_full_path} successfully uploaded to {schema}.{table_name}.')
    else:
        print(f'{source_full_path} successfully uploaded to {table_name}.')


def main():
    args = get_args()
    source_file_name_match_type = args.source_file_name_match_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    table_name = args.table_name
    insert_method = args.insert_method
    database = args.database
    schema = args.schema
    host = args.host
    password = args.password
    user = args.username
    port = args.port
    if schema == '':
        schema = None

    if port in ('', None):
        db_string = create_connection_url(host=host, password=password, user=user, database=database)
    else:
        db_string = create_connection_url(host=host, password=password, user=user, database=database, port=port)

    try:
        db_connection = create_engine(db_string)
    except Exception as e:
        print(f'Failed to connect to database {database}')
        raise (e)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            upload_data(
                source_full_path=key_name,
                table_name=table_name,
                insert_method=insert_method,
                db_connection=db_connection,
                schema=schema
            )

    else:
        upload_data(
            source_full_path=source_full_path,
            table_name=table_name,
            insert_method=insert_method,
            db_connection=db_connection,
            schema=schema
        )

    db_connection.dispose()


if __name__ == '__main__':
    main()
