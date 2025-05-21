# Timesheet Reconciliation Tool

This Python script helps automate the process of timesheet reconciliation by:
- Reading input Excel files
- Performing field mapping
- Calculating time differences
- Generating reconciliation reports

## Setup Instructions

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
- On macOS/Linux:
```bash
source venv/bin/activate
```
- On Windows:
```bash
.\venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Place your input Excel files in the `input` directory
2. Run the script:
```bash
python timesheet_reconciliation.py
```
3. Find the generated report in the `output` directory

## Project Structure
- `input/`: Directory for input Excel files
- `output/`: Directory for generated reports
- `timesheet_reconciliation.py`: Main script
- `requirements.txt`: Project dependencies