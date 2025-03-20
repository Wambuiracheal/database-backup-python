import psycopg2
import smtplib
import time
import logging
import os
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    filename="db_monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "host": os.getenv("DATABASE_HOST"),
    "port": os.getenv("DATABASE_PORT")
}

# Email configuration
EMAIL_CONFIG = {
    "SMTP_SERVER": os.getenv("SMTP_SERVER"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", 587)),
    "GMAIL_USER": os.getenv("GMAIL_USER"),
    "GMAIL_RECIPIENT": os.getenv("GMAIL_RECIPIENT"),
    "GMAIL_PASSWORD": os.getenv("GMAIL_PASSWORD"),
}

print("Starting DB Monitoring...")


def send_email(subject, message):
    """Send an email notification."""
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_CONFIG["GMAIL_USER"]
        msg["To"] = EMAIL_CONFIG["GMAIL_RECIPIENT"]
        msg["Subject"] = subject

        msg.attach(MIMEText(message, "plain"))

        server = smtplib.SMTP(EMAIL_CONFIG["SMTP_SERVER"], EMAIL_CONFIG["SMTP_PORT"])
        server.starttls()
        server.login(EMAIL_CONFIG["GMAIL_USER"], EMAIL_CONFIG["GMAIL_PASSWORD"])
        server.sendmail(
            EMAIL_CONFIG["GMAIL_USER"],
            EMAIL_CONFIG["GMAIL_RECIPIENT"],
            msg.as_string()
        )
        server.quit()

        logging.info(f"Email sent: {subject}")

    except Exception as e:
        logging.error(f"Failed to send email: {e}")


def check_db_status(config):
    """Check if the database is accessible."""
    try:
        conn = psycopg2.connect(
            dbname=config["dbname"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"]
        )
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Database down: {e}")
        send_email("ALERT: Database Down!", f"The main database is down.\nError: {e}")
        return False


def backup_database():
    """Backup the database and save it as a timestamped file."""
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    backup_file = f"db_backup_{timestamp}.sql"

    try:
        command = [
            "pg_dump",
            f"--dbname=postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        ]

        with open(backup_file, "w") as output_file:
            subprocess.run(command, stdout=output_file, check=True)

        logging.info(f"Database backup successful: {backup_file}")
        send_email("✅ Database Backup Successful", f"Backup created: {backup_file}")

    except Exception as e:
        logging.error(f"Database backup failed: {e}")
        send_email("❗ Database Backup Failed", f"Error: {e}")


# Monitor every 5 minutes
print("Monitoring loop started... (Press Ctrl+C to stop)")

try:
    while True:
        print("Checking database status...")
        if check_db_status(DB_CONFIG):
            print("✅ Database is up!")
            logging.info("Database is up!")

            # Run backup every loop (or schedule as needed)
            print("Running database backup...")
            backup_database()
        else:
            print("❗ Database is down!")
            logging.warning("Database is down!")

        print("Waiting 5 minutes before next check...")
        time.sleep(300)  # Wait for 5 minutes

except KeyboardInterrupt:
    print("\nMonitoring stopped by user.")
    logging.info("Monitoring stopped manually.")
