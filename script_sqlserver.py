import pyodbc
import pandas as pd
import smtplib
import schedule
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Database configuration
server = 'your_server'
database = 'your_database'
username = 'your_username'
password = 'your_password'
query = 'SELECT * FROM your_table'

# Email configuration
sender_email = 'your_email@example.com'
receiver_email = 'receiver_email@example.com'
email_password = 'your_email_password'
smtp_server = 'smtp.example.com'
smtp_port = 587

def fetch_and_send_data():
    # Connect to SQL Server
    try:
        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    # Fetch data
    try:
        df = pd.read_sql_query(query, connection)
        print("Data fetched successfully.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    finally:
        connection.close()

    # Convert data to CSV
    csv_filename = 'data.csv'
    df.to_csv(csv_filename, index=False)
    print(f"Data exported to {csv_filename}.")

    # Create email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Database Data CSV'

    # Email body
    body = 'Please find the attached CSV file with the latest data.'
    msg.attach(MIMEText(body, 'plain'))

    # Attach CSV file
    attachment = open(csv_filename, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= {csv_filename}')
    msg.attach(part)
    attachment.close()

    # Send email
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, email_password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")
    finally:
        server.quit()

# Schedule the task every hour
schedule.every().hour.do(fetch_and_send_data)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
