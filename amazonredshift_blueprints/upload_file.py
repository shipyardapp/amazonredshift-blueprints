from sqlalchemy import create_engine
import argparse
import os
import glob
import re
import pandas as pd
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker


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
    parser.add_argument('--schema', dest = 'schema', required = False, default = '')
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


def create_connection_url(host:str, password:str, user:str, database:str,port = 5439):
    url = URL.create(drivername= 'redshift+redshift_connector', host = host, password= password, username= user, port = port, database=database)
    return url
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

def map_datatypes(pandas_type:str) -> str:
    """ Maps the pandas datatype to the corresponding SQL data type
    Args:
        pandas_type (str): the pandas datatype
    """
    if pandas_type == 'object':
        return 'VARCHAR(255)'
    elif pandas_type == 'int64':
        return 'INT'
    elif pandas_type == 'float64':
        return 'FLOAT'
    elif pandas_type == 'bool':
        return 'BOOLEAN'
    elif pandas_type == 'datetime64':
        return 'DATETIME'
    elif pandas_type == 'timedelta[ns]':
        return 'INTERVAL'
    elif pandas_type == 'category':
        return 'VARCHAR(255)'
    else:
        return 'VARCHAR(255)'

def create_table_query(table_name:str, df:pd.DataFrame, database:str, schema:str = 'public') -> str:
    """ Generates a create table sql command that translates the pandas data types to the appropriate sql data types
    Args:
        table_name: The name of the SQL table
        df: The data frame to load
    Returns: The create table sql command
        
    """
    cols = df.columns
    # populate the query with the columns and the datatypes
    columns = ',\n'.join([f' "{col}" {map_datatypes(str(df[col].dtype))}' for col in df.columns])
    query = f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (\n{columns}\n);"
    return query


def upload_data(source_full_path, table_name, insert_method, db_connection,database,schema = None):
   # Resort to chunks for larger files to avoid memory issues.
   # in order to load to a different schema we will have to 
   # 1. Load to public
   # 2. Copy from public into the desired schema
   # 3. Drop the table from public
    conn = db_connection.connect()
    if schema is not None:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=10000)):
            if index == 0:
                query = create_table_query(table_name, chunk,database, schema= schema )
                conn.execute(query)

            if insert_method == 'replace' and index > 0:
                # First chunk replaces the table, the following chunks
                # append to the end.
                insert_method = 'append'

            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                if_exists=insert_method,
                method='multi',
                chunksize=10000)
        # copy over from public to new table
        print(f"Copying table into {schema}")
        copy_query = f"INSERT INTO {schema}.{table_name} (SELECT * FROM {table_name})"
        conn.execute(copy_query)
        drop_query = f"DROP TABLE public.{table_name}"
        print(f'{source_full_path} successfully uploaded to {schema}.{table_name}.')
        
    
    else:
        for index, chunk in enumerate(
                pd.read_csv(source_full_path, chunksize=10000)):

            if insert_method == 'replace' and index > 0:
                # First chunk replaces the table, the following chunks
                # append to the end.
                insert_method = 'append'

            chunk.to_sql(
                table_name,
                con=db_connection,
                index=False,
                if_exists=insert_method,
                method='multi',
                chunksize=10000)

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

    db_string = create_connection_url(host = host, password = password, user = user, database = database)
    try:
        db_connection = create_engine(db_string)
    except Exception as e:
        print(f'Failed to connect to database {database}')
        raise(e)

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
                database= database,
                db_connection=db_connection, schema = schema)

    else:
        upload_data(source_full_path=source_full_path, table_name=table_name,
                    insert_method=insert_method, db_connection=db_connection,database= database, schema = schema)

    db_connection.dispose()


if __name__ == '__main__':
    main()
