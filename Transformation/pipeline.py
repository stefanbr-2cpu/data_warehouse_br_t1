import importlib
import time
from datetime import datetime
import schedule



def run_module(module_name, label):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running {label}...")

    module = importlib.import_module(module_name)
    importlib.reload(module)

    if hasattr(module, "main"):
        module.main()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Finished {label} ")



def run_database():
    run_module("database", "database setup")


def run_tables():
    run_module("tables_creation", "table creation")


def run_ingest():
    run_module("ingest", "raw data ingestion")



def run_transformation():
    run_module("transformation", "transformation main")


def run_transformation1():
    run_module("transformation1", "transformation1")


def run_transformation2():
    run_module("transformation2", "transformation2")


def run_transformation3():
    run_module("transformation3", "transformation3")


def run_transformation4():
    run_module("transformation4", "transformation4")


def run_transformation5():
    run_module("transformation5", "transformation5")


    run_module("curated", "curated layer")


def run_full_pipeline():
    print("\n========================================")
    print(f"PIPELINE STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================")

    run_database()
    run_tables()
    run_ingest()

    run_transformation()
    run_transformation1()
    run_transformation2()
    run_transformation3()
    run_transformation4()
    run_transformation5()

    run_curated()

    print("\n========================================")
    print(f"PIPELINE FINISHED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================\n")



def run_nightly_pipeline():
    print("\n========================================")
    print(f"NIGHTLY PIPELINE STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================")

    run_ingest()

    run_transformation()
    run_transformation1()
    run_transformation2()
    run_transformation3()
    run_transformation4()
    run_transformation5()

    run_curated()

    print("\n========================================")
    print(f"NIGHTLY PIPELINE FINISHED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("========================================\n")


if __name__ == "__main__":
    run_full_pipeline()

    schedule.every().day.at("01:00").do(run_nightly_pipeline)

    print("Scheduler started...")

    while True:
        schedule.run_pending()
        time.sleep(5)