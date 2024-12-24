import psycopg2
import config
import traceback


def connect():
    try:
        connection = psycopg2.connect(
            user=config.user,
            password=config.password,
            database=config.database,
            host=config.host,
            port=config.port,
        )
        return connection
    except Exception as err:
        print("Error occurred in making connection …")
        traceback.print_exc()


def print_version(connection):
    cursor = connect().cursor()
    cursor.execute("SELECT version()")
    db_version = cursor.fetchone()
    print(db_version)
    cursor.close()
    connection.close()


def create(connection):
    cursor = connection.cursor()
    query = """
    create table person(
        id int primary key,
        first_name varchar(100),
        last_name varchar(100),
        city varchar(100)
    );
    """
    try:
        cursor.execute(query)
        connection.commit()
        print("table created successfully!")
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def insert(connection):
    cursor = connection.cursor()

    try:
        data = [
            (1, "James", "Bond", "NA"),
            (2, "James", "Monk", "NA"),
            (3, "James", "Billion", "NA"),
            (4, "James", "Road", "NA"),
            (5, "James", "Wick", "NA"),
            (6, "James", "Renold", "NA"),
            (7, "James", "D'cruz", "NA"),
            (8, "James", "White", "NA"),
            (9, "James", "Gomez", "NA"),
            (10, "James", "Williams", "NA"),
        ]
        data_records = ", ".join(["%s"] * len(data))
        query = f"""
        INSERT INTO person (id, first_name, last_name, city) VALUES {data_records};
        """
        cursor.execute(query, data)
        connection.commit()
        print("Record inserted successfully!")
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def read(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT * FROM person LIMIT 500;")
        records = cursor.fetchall()
        for record in records:
            print(
                f"Read successful: id = {record[0]}, name= {record[1]+' '+record[2]}, city= {record[3]}"
            )
        connection.commit()
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def update(connection):
    cursor = connection.cursor()
    query = """
    UPDATE person SET city=%s WHERE city=%s;
    """
    try:
        cursor.execute(query, ("Sydney", "NA"))
        cursor.execute("SELECT * FROM person WHERE city='NA';")
        records = cursor.fetchall()
        for record in records:
            print(
                f"Update successful : id = {record[0]}, name= {record[1]+' '+record[2]}, city= {record[3]}"
            )
        connection.commit()
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def delete(connection):
    cursor = connection.cursor()
    query = """
    DELETE FROM person WHERE city='Sydney';
    """
    try:
        cursor.execute(query)
        cursor.execute("select * from person;")
        record = cursor.fetchone()
        print(record)
        connection.commit()
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


if __name__ == "__main__":
    read(connect())
