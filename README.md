name: Run Python Script

on:
  push:
    branches: [ "main" ]

jobs:
  run-python:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Run Python file
        run: python main.py
