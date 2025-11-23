#!/usr/bin/env python3
"""
Run the FastAPI server

Usage:
    python run_api.py [--host HOST] [--port PORT] [--reload]

Local examples:
    python run_api.py
    python run_api.py --port 8080
    python run_api.py --reload  # For development
"""
import argparse
import uvicorn


def main():
    parser = argparse.ArgumentParser(description="Run the PostgreSQL Performance Analyser API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")

    args = parser.parse_args()

    print(f"Starting PostgreSQL Performance Analyser API")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Reload: {args.reload}")
    print()
    print(f"API documentation available at: http://{args.host}:{args.port}/docs")
    print()

    uvicorn.run(
        "src.api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload
    )


if __name__ == "__main__":
    main()
