"""
Main entry point for GodLoop examples and benchmarks.
"""

import sys
import argparse


def run_basic():
    from eventloop.examples.basic import main as basic_main
    from eventloop import run
    run(basic_main())


def run_advanced():
    from eventloop.examples.advanced import main as advanced_main
    from eventloop import run
    run(advanced_main())


def run_benchmarks():
    from eventloop.benchmarks.benchmark import run_all_benchmarks
    run_all_benchmarks()


def run_comparison():
    from eventloop.benchmarks.comparison import run_comparison
    run_comparison()


def run_tests():
    import pytest
    sys.exit(pytest.main(["eventloop/tests/", "-v"]))


def main():
    parser = argparse.ArgumentParser(description="GodLoop - God-tier Event Loop")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    subparsers.add_parser("basic", help="Run basic examples")
    subparsers.add_parser("advanced", help="Run advanced examples")
    subparsers.add_parser("benchmarks", help="Run benchmarks")
    subparsers.add_parser("comparison", help="Run asyncio comparison")
    subparsers.add_parser("test", help="Run test suite")
    
    args = parser.parse_args()
    
    if args.command == "basic":
        run_basic()
    elif args.command == "advanced":
        run_advanced()
    elif args.command == "benchmarks":
        run_benchmarks()
    elif args.command == "comparison":
        run_comparison()
    elif args.command == "test":
        run_tests()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()