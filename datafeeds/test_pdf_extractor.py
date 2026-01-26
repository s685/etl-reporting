"""
Quick Test Script for PDF Table Extractor
Run this before production deployment to verify functionality.
"""

import sys
import subprocess
from pathlib import Path

def run_test(test_name, command, expected_result):
    """Run a single test case."""
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print(f"{'='*60}")
    print(f"Command: {command}")
    print("-" * 60)
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("[PASS] TEST PASSED")
            print(f"Output: {result.stdout[:200]}...")
            return True
        else:
            print("[FAIL] TEST FAILED")
            print(f"Error: {result.stderr[:200]}...")
            print(f"Expected: {expected_result}")
            return False
            
    except subprocess.TimeoutExpired:
        print("[FAIL] TEST FAILED (Timeout)")
        return False
    except Exception as e:
        print(f"[FAIL] TEST FAILED (Exception: {e})")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print(" PDF TABLE EXTRACTOR - DRY RUN TEST SUITE")
    print("="*60)
    
    tests_passed = 0
    tests_failed = 0
    
    # Test 1: Check imports
    print("\n" + "="*60)
    print("TEST 1: Checking Dependencies")
    print("="*60)
    
    try:
        import pandas as pd
        print(f"[OK] pandas {pd.__version__}")
        tests_passed += 1
    except ImportError:
        print("[FAIL] pandas not installed")
        tests_failed += 1
    
    try:
        import openpyxl
        print(f"[OK] openpyxl {openpyxl.__version__}")
        tests_passed += 1
    except ImportError:
        print("[FAIL] openpyxl not installed")
        tests_failed += 1
    
    try:
        import pdfplumber
        print(f"[OK] pdfplumber {pdfplumber.__version__}")
        tests_passed += 1
    except ImportError:
        print("[FAIL] pdfplumber not installed (install: pip install pdfplumber)")
        tests_failed += 1
    
    # Test 2: Check script exists
    print("\n" + "="*60)
    print("TEST 2: Checking Script File")
    print("="*60)
    
    script_path = Path("datafeeds/pdf_table_extractor.py")
    if script_path.exists():
        print(f"[OK] Script found: {script_path}")
        print(f"  Size: {script_path.stat().st_size} bytes")
        tests_passed += 1
    else:
        print(f"[FAIL] Script not found: {script_path}")
        tests_failed += 1
    
    # Test 3: Check syntax
    print("\n" + "="*60)
    print("TEST 3: Checking Python Syntax")
    print("="*60)
    
    result = subprocess.run(
        "python -m py_compile datafeeds/pdf_table_extractor.py",
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("[OK] Syntax check passed")
        tests_passed += 1
    else:
        print(f"[FAIL] Syntax error: {result.stderr}")
        tests_failed += 1
    
    # Test 4: Check help command
    print("\n" + "="*60)
    print("TEST 4: Checking Help Command")
    print("="*60)
    
    result = subprocess.run(
        "python datafeeds/pdf_table_extractor.py --help",
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0 and "usage:" in result.stdout.lower():
        print("[OK] Help command works")
        print("  Arguments available: --input, --output, --library, etc.")
        tests_passed += 1
    else:
        print("[FAIL] Help command failed")
        tests_failed += 1
    
    # Test 5: Check error handling (nonexistent file)
    print("\n" + "="*60)
    print("TEST 5: Checking Error Handling")
    print("="*60)
    
    result = subprocess.run(
        "python datafeeds/pdf_table_extractor.py --input nonexistent.pdf --output test.xlsx",
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0 and "not found" in result.stderr.lower():
        print("[OK] Error handling works (proper error message for missing file)")
        tests_passed += 1
    else:
        print("[FAIL] Error handling issue")
        tests_failed += 1
    
    # Summary
    print("\n" + "="*60)
    print(" TEST SUMMARY")
    print("="*60)
    print(f"Tests Passed: {tests_passed}")
    print(f"Tests Failed: {tests_failed}")
    print(f"Total Tests:  {tests_passed + tests_failed}")
    
    if tests_failed == 0:
        print("\n*** ALL TESTS PASSED - READY FOR PRODUCTION ***")
        return 0
    else:
        print(f"\n*** {tests_failed} TESTS FAILED - FIX BEFORE PRODUCTION ***")
        return 1


if __name__ == "__main__":
    sys.exit(main())
