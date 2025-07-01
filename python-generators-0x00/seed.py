# seed.py

import mysql.connector
import csv
import uuid

def connect_db():
    """Connect to MySQL server (without specifying database)"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',  # Update with your password if needed
            auth_plugin='mysql_native_password'  # sometimes needed
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL server: {err}")
        return None

def create_database(connection):
    """Create database ALX_prodev if it does not exist"""
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")

def connect_to_prodev():
    """Connect to ALX_prodev database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',  # Update with your password if needed
            database='ALX_prodev',
            auth_plugin='mysql_native_password'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to ALX_prodev database: {err}")
        return None

def create_table(connection):
    """Create user_data table if it does not exist"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age DECIMAL NOT NULL,
                INDEX idx_user_id (user_id)
            )
        """)
        connection.commit()
        print("Table user_data created successfully")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")

def insert_data(connection, csv_file):
    """Insert data from csv file if not exists"""
    try:
        cursor = connection.cursor()

        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get('user_id')
                name = row.get('name')
                email = row.get('email')
                age = row.get('age')

                if not user_id:
                    # Generate UUID if missing in CSV
                    user_id = str(uuid.uuid4())

                # Check if user_id exists
                cursor.execute("SELECT 1 FROM user_data WHERE user_id = %s", (user_id,))
                if cursor.fetchone():
                    continue  # skip insert if exists

                cursor.execute(
                    "INSERT INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)",
                    (user_id, name, email, age)
                )

        connection.commit()
        cursor.close()
    except FileNotFoundError:
        print(f"CSV file {csv_file} not found.")
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")
