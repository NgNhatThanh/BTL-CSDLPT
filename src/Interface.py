import psycopg2.extensions
import os
import time
from dotenv import load_dotenv

load_dotenv()

def getopenconnection():
    """Thiết lập kết nối đến PostgreSQL"""
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT'))
    )

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start_time = time.time()
    cur = openconnection.cursor()

    try:
        # Tạo bảng đích
        cur.execute(f"""
            DROP TABLE IF EXISTS {ratingstablename};
            CREATE TABLE {ratingstablename} (
                UserID INT,
                MovieID INT,
                Rating FLOAT,
                PRIMARY KEY (UserID, MovieID)
            );
        """)

        # Tạo bảng tạm đơn giản hơn để tăng tốc copy
        cur.execute("""
            CREATE TEMP TABLE temp_ratings (
                userid INT,
                movieid INT,
                rating FLOAT
            ) ON COMMIT DROP;
        """)

        # Đọc và xử lý dữ liệu, chỉ lấy 3 cột cần thiết (1, 3, 5)
        with open(ratingsfilepath, 'r') as infile:
            buffer = []
            for line in infile:
                parts = line.strip().split(':')
                if len(parts) >= 5:
                    buffer.append(f"{parts[0]}\t{parts[2]}\t{parts[4]}\n")

        # Viết vào bảng tạm
        from io import StringIO
        cur.copy_from(StringIO(''.join(buffer)), 'temp_ratings', sep='\t')

        # Chuyển sang bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} (UserID, MovieID, Rating)
            SELECT userid, movieid, rating FROM temp_ratings;
        """)

        openconnection.commit()
        print(f"[loadratings] Hoàn thành trong {time.time() - start_time:.2f} giây")
    except Exception as e:
        openconnection.rollback()
        print(f"[loadratings] Lỗi: {e}")
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be positive")

    start_time = time.time()
    try:
        cur = openconnection.cursor()
        RANGE_TABLE_PREFIX = 'range_part'

        step = 5.0 / numberofpartitions

        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            cur.execute(f"""
                DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i};
                CREATE TABLE {RANGE_TABLE_PREFIX}{i} (
                    userid INTEGER, 
                    movieid INTEGER, 
                    rating FLOAT,
                    PRIMARY KEY (userid, movieid)
                );
            """)

        # Sử dụng CASE WHEN để chia vào nhiều bảng trong một truy vấn duy nhất
        for i in range(numberofpartitions):
            lower = i * step
            upper = (i + 1) * step
            if i == 0:
                condition = f"rating >= {lower} AND rating <= {upper}"
            else:
                condition = f"rating > {lower} AND rating <= {upper}"

            cur.execute(f"""
                INSERT INTO {RANGE_TABLE_PREFIX}{i}
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE {condition};
            """)

        openconnection.commit()
        print(f"[rangepartition] Hoàn thành {numberofpartitions} partitions trong {time.time() - start_time:.2f} giây")
    except Exception as e:
        openconnection.rollback()
        print(f"[rangepartition] Lỗi: {e}")
        raise
    finally:
        cur.close()
        
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";")
    total_rows = (cur.fetchall())[0][0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows-1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()


def rangeinsert(_, userid, itemid, rating, openconnection: psycopg2.extensions.connection) -> None:
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    cursor = openconnection.cursor()
    start_time = time.time()

    prefix = "range_part"
    partitions_number = count_partitions(prefix, openconnection)
    print (f"[rangeinsert] Number of partitions: {partitions_number}")

    if not partitions_number:
        cursor.close()
        raise Exception(f"No partitions found with prefix '{prefix}'")

    delta = 5 / partitions_number
    idx = int(rating / delta)

    if rating % delta == 0 and idx:
        idx -= 1
    if idx >= partitions_number:
        idx = partitions_number - 1

    table_name = f"{prefix}{idx}"
    command = f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ('{userid}', '{itemid}', '{rating}');"
    cursor.execute(command)
    print (command)
    
    openconnection.commit()
    cursor.close()
    print (f"[rangeinsert] Done in {time.time() - start_time:.2f} seconds")


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def count_partitions(prefix, openconnection: psycopg2.extensions.connection) -> int:
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    cursor = openconnection.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%';"
    )

    count = cursor.fetchone()[0]
    cursor.close()
    return count
