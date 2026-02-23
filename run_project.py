import subprocess
import time
import socket
import sys
import atexit

processes = []
BUCKET_NAME = "ecommerce-raw"

def cleanup():
    print("\nStopping background processes...")
    for p in processes:
        p.terminate()

atexit.register(cleanup)

def run_command(command, description, blocking=True):
    print(f"--- {description} ---")
    if blocking:
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            print(f"ERROR: {description} failed.")
            sys.exit(1)
    else:
        p = subprocess.Popen(command, shell=True)
        processes.append(p)
        return p

def wait_for_port(host, port, timeout=60):
    print(f"Waiting for {host}:{port} to be ready...")
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except (OSError, ConnectionRefusedError):
            if time.time() - start_time > timeout:
                print(f"ERROR: Timeout waiting for {host}:{port}.")
                sys.exit(1)
            time.sleep(2)

def setup_minio_bucket():
    """Creates the required MinIO bucket automatically using a one-liner."""
    print(f"--- Setting up MinIO Bucket: {BUCKET_NAME} ---")
    python_cmd = (
        f"import s3fs; "
        f"fs = s3fs.S3FileSystem(key='minioadmin', secret='minioadmin', "
        f"client_kwargs={{'endpoint_url': 'http://localhost:9000'}}); "
        f"fs.mkdir('{BUCKET_NAME}') if not fs.exists('{BUCKET_NAME}') else print('Bucket exists.')"
    )
    subprocess.check_call([sys.executable, "-c", python_cmd])

def main():
    print("Starting Real-Time E-commerce Pipeline\n")

    # 1. Start Infrastructure
    run_command("docker-compose up -d", "Starting Docker Containers")

    # 2. Wait for crucial services
    wait_for_port("localhost", 9092)
    wait_for_port("localhost", 7077)
    wait_for_port("localhost", 9000)

    # 3. Install Requirements
    run_command(f"{sys.executable} -m pip install -r requirements.txt", "Installing Dependencies")

    # 4. Create Bucket
    setup_minio_bucket()

    # 5. Start Generator
    run_command(f"{sys.executable} producer/generator.py", "Data Generator", blocking=False)
    time.sleep(5)

    # 6. Start Spark Streaming
    spark_cmd = "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/stream_processor.py"
    run_command(spark_cmd, "Spark Stream Processor", blocking=False)

    print("\nAllowing Spark 45 seconds to download packages and initialize...")
    time.sleep(45)

    # 7. Start Dashboard
    print("\nLaunching Streamlit Dashboard...")
    print("Press Ctrl+C in this terminal to stop everything.")
    try:
        subprocess.run(f"{sys.executable} -m streamlit run dashboard_s3.py", shell=True)
    except KeyboardInterrupt:
        print("\nShutting down pipeline...")

if __name__ == "__main__":
    main()