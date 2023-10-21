import io
import psycopg2
import sqlalchemy as sa
from sqlalchemy import create_engine
from tools import credentials
import pg8000


def save_dataframe_in_postgres(dataframe, psql_table_name, action):
    engine = create_engine('postgresql+psycopg2://postgres:123@localhost:5432/postgres')

    dataframe.head(0).to_sql(f'{psql_table_name}', engine, if_exists=f'{action}',  # action = fail, replace, append,
                             index=False)  # drops old table and creates new empty table

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    dataframe.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    # contents = output.getvalue()
    try:
        cur.copy_from(output, f'{psql_table_name}', null="")  # null values become ''
    except psycopg2.DatabaseError:
        print(f'Error saving df in {psql_table_name}, action: {action}')
        #print(dataframe)
        return
    conn.commit()
    conn.close()


def update_db(q):
    conn = None
    updated_rows = 0
    try:
        conn = psycopg2.connect(user="postgres",
                                password="123",
                                host="127.0.0.1",
                                port="5432",
                                database="postgres")
        cur = conn.cursor()
        cur.execute(q)
        updated_rows = cur.rowcount
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return updated_rows


def save_df_to_psql(df, table_name, action):
    user = credentials.postgres_credentials()[0]
    password = credentials.postgres_credentials()[1]
    host = "localhost"
    port = 5432
    dbname = "postgres"
    db_string = sa.engine.url.URL.create(
                                       drivername="postgresql+pg8000",
                                       username=user,
                                       password=password,
                                       host=host,
                                       port=port,
                                       database=dbname,
                                       )
    db_engine = create_engine(db_string)
    df.to_sql(table_name, con=db_engine, if_exists=action, index=False,)
    return