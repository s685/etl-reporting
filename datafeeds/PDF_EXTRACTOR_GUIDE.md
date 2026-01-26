# PDF Table Extractor - Usage Guide

## 🎯 Overview

This script extracts tables from PDF files and converts them to Excel or CSV format.

**Key Features:**
- ✅ Supports multiple extraction libraries (pdfplumber, tabula, camelot)
- ✅ Auto-detects tables in PDF
- ✅ Exports to Excel (multiple sheets) or CSV
- ✅ Handles multi-page PDFs
- ✅ Clean, production-ready code
- ✅ Comprehensive error handling and logging

---

## 📦 Installation

### Quick Start (Recommended)

```bash
# Install pdfplumber (easiest, works for most PDFs)
pip install pdfplumber pandas openpyxl
```

### Advanced Options

```bash
# Option 1: Install all libraries for maximum compatibility
pip install -r datafeeds/pdf_extractor_requirements.txt

# Option 2: Install specific library
pip install pdfplumber pandas openpyxl  # pdfplumber (default)
pip install tabula-py pandas openpyxl   # tabula (requires Java)
pip install camelot-py[cv] pandas openpyxl  # camelot (requires Ghostscript)
```

---

## 🚀 Usage

### Basic Examples

```bash
# Extract to Excel (default)
python datafeeds/pdf_table_extractor.py --input report.pdf --output report.xlsx

# Extract to CSV
python datafeeds/pdf_table_extractor.py --input report.pdf --output report.csv

# Specify library
python datafeeds/pdf_table_extractor.py --input report.pdf --output report.xlsx --library tabula
```

### Advanced Examples

```bash
# Use tabula for complex layouts
python datafeeds/pdf_table_extractor.py \
    --input complex_report.pdf \
    --output complex_report.xlsx \
    --library tabula

# Use camelot for bordered tables (highest accuracy)
python datafeeds/pdf_table_extractor.py \
    --input bordered_table.pdf \
    --output bordered_table.xlsx \
    --library camelot

# Extract to multiple CSVs (one per table)
python datafeeds/pdf_table_extractor.py \
    --input multi_table.pdf \
    --output tables.csv \
    --format csv
```

---

## 📋 Command Line Arguments

| Argument | Short | Required | Description | Default |
|----------|-------|----------|-------------|---------|
| `--input` | `-i` | Yes | Input PDF file path | - |
| `--output` | `-o` | Yes | Output file path (Excel/CSV) | - |
| `--library` | `-l` | No | Extraction library (`pdfplumber`, `tabula`, `camelot`) | `pdfplumber` |
| `--format` | `-f` | No | Output format (`excel`, `csv`) | Auto-detect |

---

## 🔧 Which Library to Use?

### **pdfplumber** (Default - Recommended)
✅ **Best for:** Most PDFs, simple to complex tables  
✅ **Pros:** Easy to install, no external dependencies, good accuracy  
✅ **Installation:** `pip install pdfplumber`

**Use when:**
- You want a quick, reliable solution
- You don't have Java or Ghostscript installed
- Your PDFs have standard table structures

---

### **tabula-py** 
✅ **Best for:** Complex layouts, scanned PDFs  
✅ **Pros:** Java-based (battle-tested), handles difficult cases  
⚠️ **Requires:** Java Runtime Environment (JRE)

**Installation:**
```bash
# 1. Install Java from https://www.java.com/download/
# 2. Install tabula
pip install tabula-py
```

**Use when:**
- pdfplumber misses tables
- PDF has complex multi-column layouts
- You have Java installed

---

### **camelot-py**
✅ **Best for:** Bordered tables (lattice mode)  
✅ **Pros:** Highest accuracy for structured tables  
⚠️ **Requires:** Ghostscript

**Installation:**
```bash
# 1. Install Ghostscript from https://ghostscript.com/releases/gsdnld.html
# 2. Install camelot
pip install camelot-py[cv]
```

**Use when:**
- Tables have clear borders/lines
- You need maximum extraction accuracy
- pdfplumber or tabula don't work well

---

## 📊 Output Formats

### Excel (`.xlsx`)
- **Single file** with multiple sheets
- Each sheet = one table
- Sheet names: `Page1_Table1`, `Page2_Table1`, etc.
- **Best for:** Viewing all tables in one file

### CSV (`.csv`)
- **Multiple files** if multiple tables found
- Filenames: `output_table_1.csv`, `output_table_2.csv`, etc.
- **Best for:** Data processing, database imports

---

## 💡 Examples by Use Case

### 1. Financial Reports (Bordered Tables)
```bash
python datafeeds/pdf_table_extractor.py \
    --input financial_report.pdf \
    --output financial_data.xlsx \
    --library camelot
```

### 2. Scanned Documents
```bash
python datafeeds/pdf_table_extractor.py \
    --input scanned_doc.pdf \
    --output scanned_data.xlsx \
    --library tabula
```

### 3. Standard Reports
```bash
python datafeeds/pdf_table_extractor.py \
    --input standard_report.pdf \
    --output standard_data.xlsx
# Uses pdfplumber by default
```

### 4. Bulk Processing
```bash
# Process multiple PDFs
for file in *.pdf; do
    python datafeeds/pdf_table_extractor.py \
        --input "$file" \
        --output "${file%.pdf}.xlsx"
done
```

---

## 🛠️ Troubleshooting

### No Tables Found?

1. **Try different library:**
```bash
# Try tabula if pdfplumber doesn't work
python pdf_table_extractor.py -i file.pdf -o output.xlsx -l tabula
```

2. **Check PDF quality:**
   - Is the PDF searchable (not scanned)?
   - Are tables clearly defined?

3. **Use camelot for bordered tables:**
```bash
python pdf_table_extractor.py -i file.pdf -o output.xlsx -l camelot
```

### ImportError for Libraries?

```bash
# Install missing library
pip install pdfplumber  # or tabula-py or camelot-py[cv]
```

### Java Not Found (for tabula)?

1. Download Java: https://www.java.com/download/
2. Install and restart terminal
3. Verify: `java -version`

### Ghostscript Not Found (for camelot)?

1. Download Ghostscript: https://ghostscript.com/releases/gsdnld.html
2. Install and restart terminal
3. Add to PATH if needed

---

## 📈 Performance Tips

1. **Start with pdfplumber** - It's fast and handles 80% of cases
2. **Switch to tabula** if pdfplumber misses tables
3. **Use camelot** for maximum accuracy on bordered tables
4. **For large PDFs**, consider processing page ranges separately

---

## 🎓 Integration with Existing Framework

You can integrate this with your Snowflake pipeline:

```python
from datamart_analytics.datafeeds.pdf_table_extractor import PDFTableExtractor

# Extract tables
extractor = PDFTableExtractor(
    input_pdf='client_report.pdf',
    output_file='temp_extract.xlsx',
    library='pdfplumber'
)
tables = extractor.extract_tables()

# Load to Snowflake
for df in tables:
    connector.save_as_table(df, table_name=f"pdf_table_{idx}")
```

---

## 📞 Support

If a library doesn't work:
1. Check PDF quality (searchable vs scanned)
2. Try different library (`-l tabula` or `-l camelot`)
3. Verify table structure in PDF is clear
4. Check logs for specific error messages

---

**Made with 30 years of Python experience standards** 🚀
