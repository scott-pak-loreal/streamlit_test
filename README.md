# streamlit_test
# Revenue Calculator MVP

A minimal Streamlit app that reads brand‑level ROI data from an Excel file, lets the user enter a budget, and computes the expected revenue (ROI × Budget) per channel/tactic.

---

## 📋 Features

- **Brand selector**: pick from all unique brands in your data  
- **Budget input**: enter a dollar amount  
- **Calculate button**: triggers the revenue calculation  
- **Results table**: shows Channel, Tactic, ROI (2 decimals), and Expected Revenue (whole numbers with commas)  
- **MVP‑style**: simple, easy to extend

---

## 🚀 Getting Started

### Prerequisites

- Python 3.7 or higher  
- [pip](https://pip.pypa.io/en/stable/)  

### Installation

1. **Clone this repository**  
   ```bash
   git clone https://github.com/your‑username/streamlit_test.git
   cd streamlit_test

2. **Create and activate a virtual environment**  
python -m venv .venv
# Windows (PowerShell)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
. .\.venv\Scripts\Activate.ps1

# macOS/Linux
source .venv/bin/activate

3. **Install Dependencies**  
pip install -r requirements.txt

4. **Ensure your data file is present**  
Make sure streamlit_testdata.xlsx lives in the same folder as streamlit_app.py.