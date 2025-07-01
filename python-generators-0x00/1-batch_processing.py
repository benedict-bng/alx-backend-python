import mysql.connector

def stream_users_in_batches(batch_size):
    """Generator that fetches user_data rows from DB in batches of batch_size."""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',  # update if needed
            database='ALX_prodev',
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT user_id, name, email, age FROM user_data")

        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Database error: {err}")

def batch_processing(batch_size):
    """Processes batches and yields users older than 25."""
    for batch in stream_users_in_batches(batch_size):
        # One loop over batches
        for user in batch:
            # One loop over users in batch
            if user['age'] > 25:
                yield user
        # No more loops; total loops = 2 here
