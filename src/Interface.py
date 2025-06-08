import psycopg2.extensions
import os
import time
from dotenv import load_dotenv
from io import StringIO

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


def create_metadata_table_if_not_exists(cursor: psycopg2.extensions.cursor) -> None:
    command = (
        """
        CREATE TABLE IF NOT EXISTS partition_metadata 
        (
            partition_type VARCHAR(20) PRIMARY KEY,
            partition_count INT NOT NULL,
            last_used INT
        );
        """
    )

    cursor.execute(command)
    # print (command)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start_time = time.time()
    cur = openconnection.cursor()

    try:
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")
        cur.execute(f"""
            CREATE TABLE {ratingstablename} (
                userid INT,
                movieid INT,
                rating FLOAT
            );
        """)

        buffer = StringIO()
        with open(ratingsfilepath, 'r') as f:
            for line in f:
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    buffer.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        buffer.seek(0)

        cur.copy_from(buffer, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))

        cur.execute(f"ALTER TABLE {ratingstablename} ADD PRIMARY KEY (userid, movieid);")

        openconnection.commit()
        print(f"[loadratings] Completed in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        openconnection.rollback()
        print(f"[loadratings] Error: {e}")
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be positive")

    start_time = time.time()
    try:
        cur = openconnection.cursor()
        create_metadata_table_if_not_exists(cur)
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

        command = (f"""
            INSERT INTO partition_metadata (partition_type, partition_count, last_used)
            VALUES ('range', {numberofpartitions}, NULL)
        """)

        cur.execute(command)
        # print (command)

        openconnection.commit()
        print(f"[rangepartition] Completed {numberofpartitions} partitions in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        openconnection.rollback()
        print(f"[rangepartition] Error: {e}")
        raise
    finally:
        cur.close()
        
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        raise ValueError("Number of partitions must be positive")

    start_time = time.time()
    try:
        cur = openconnection.cursor()
        create_metadata_table_if_not_exists(cur)
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Tạo các bảng phân mảnh
        for i in range(numberofpartitions):
            cur.execute(f"""
                DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};
                CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (
                    userid INTEGER, 
                    movieid INTEGER, 
                    rating FLOAT,
                    PRIMARY KEY (userid, movieid)
                );
            """)
            cur.execute(f"""
                INSERT INTO {RROBIN_TABLE_PREFIX}{i}(userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM (
                    SELECT userid, movieid, rating,
                           row_number() over () - 1 as rn
                    FROM {ratingstablename}
                ) t
                WHERE mod(rn, {numberofpartitions}) = {i};
            """)

        command = (f"""
            INSERT INTO partition_metadata (partition_type, partition_count, last_used)
            VALUES ('rrobin', {numberofpartitions}, {time.time()})
        """)

        cur.execute(command)
        print (command)

        openconnection.commit()
        print(f"[roundrobinpartition] Completed {numberofpartitions} partitions in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        openconnection.rollback()
        print(f"[roundrobinpartition] Error: {e}")
        raise
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Kiểm tra số partition
        numberofpartitions = count_partitions('rrobin', openconnection)
        if not numberofpartitions:
            raise ValueError("No round-robin partitions found")

        # Insert vào bảng chính và lấy số hàng trong một query
        cur.execute(f"""
            WITH inserted AS (
                INSERT INTO {ratingstablename} (userid, movieid, rating) 
                VALUES (%s, %s, %s)
                RETURNING (SELECT COUNT(*) FROM {ratingstablename})
            )
            SELECT * FROM inserted;
        """, (userid, itemid, rating))
        
        total_rows = cur.fetchone()[0]
        index = (total_rows - 1) % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{index}"

        # Insert vào partition tương ứng
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating) 
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        openconnection.commit()
        print(f"[roundrobininsert] Successfully inserted into partition {index}")

    except Exception as e:
        openconnection.rollback()
        print(f"[roundrobininsert] Error: {e}")
        raise
    finally:
        cur.close()


def rangeinsert(_, userid, itemid, rating, openconnection: psycopg2.extensions.connection) -> None:
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    try:
        cursor = openconnection.cursor()
        start_time = time.time()

        type = "range"
        partitions_number = count_partitions(type, openconnection)
        print (f"[rangeinsert] Number of partitions: {partitions_number}")

        if not partitions_number:
            cursor.close()
            raise Exception(f"No partitions found with type '{type}'")

        if partitions_number is None:
            cursor.close()
            raise Exception(f"Error counting partitions with type '{type}'")

        delta = 5 / partitions_number
        idx = int(rating / delta)

        if rating % delta == 0 and idx:
            idx -= 1
        if idx >= partitions_number:
            idx = partitions_number - 1

        prefix = "range_part"
        table_name = f"{prefix}{idx}"
        command = f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});"
        cursor.execute(command)
        print (command)

        openconnection.commit()
        print (f"[rangeinsert] Completed in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        openconnection.rollback()
        raise Exception(f"[rangeinsert] Error: {e}")
    finally:
        cursor.close()


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


def count_partitions(type, openconnection: psycopg2.extensions.connection) -> int | None:
    """
    Function to count the number of partitions which type is @type.
    """
    try:
        cursor = openconnection.cursor()
        command = f"""
            SELECT partition_count
            FROM partition_metadata
            WHERE partition_type = '{type}';
        """
        
        cursor.execute(command)
        return cursor.fetchone()[0]
    except Exception as e:
        print (f"[count_partitions] Error: {e}")
        return None
    finally:
        cursor.close()