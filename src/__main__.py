"""Entry point for `python -m src` — delegates to run_demo."""
from src.run_demo import run_pipeline

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run full migration demo")
    parser.add_argument("--no-ui", action="store_true",
                        help="Skip launching Streamlit dashboard")
    args = parser.parse_args()
    run_pipeline(launch_ui=not args.no_ui)
