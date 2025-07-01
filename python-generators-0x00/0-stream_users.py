import mysql.connector

def stream_users():
    """Generator that yields one user record at a time from user_data table."""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',  
            database='ALX_prodev',
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT user_id, name, email, age FROM user_data")

        for row in cursor:
            yield row

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
