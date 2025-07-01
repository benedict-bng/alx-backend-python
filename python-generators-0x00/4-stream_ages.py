# 3-average_age.py

import seed  # Assuming seed.py provides connect_to_prodev()

def stream_user_ages():
    """Generator that yields user ages one by one from user_data table."""
    try:
        connection = seed.connect_to_prodev()
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT age FROM user_data")

        for row in cursor:
            yield row['age']

        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")

def calculate_average_age():
    total_age = 0
    count = 0

    for age in stream_user_ages():
        total_age += age
        count += 1

    if count == 0:
        return 0
    return total_age / count

if __name__ == "__main__":
    avg_age = calculate_average_age()
    print(f"Average age of users: {avg_age:.2f}")
