#!/usr/bin/env python3
"""
Quick verification that plot_benchmarks.py works with sample data
"""

import subprocess
import sys
import os

def main():
    print("=" * 70)
    print("Testing Benchmark Plotting System")
    print("=" * 70)
    print()
    
    # Check if matplotlib is installed
    try:
        import matplotlib
        import pandas
        print("✓ matplotlib and pandas are installed")
    except ImportError as e:
        print("✗ Missing dependencies:")
        print(f"  {e}")
        print()
        print("Install with: pip install matplotlib pandas numpy")
        sys.exit(1)
    
    # Check if sample data exists
    sample_file = "benchmarks/sample-data.csv"
    if not os.path.exists(sample_file):
        print(f"✗ Sample data file not found: {sample_file}")
        sys.exit(1)
    
    print(f"✓ Sample data found: {sample_file}")
    print()
    
    # Try to generate plot
    print("Generating test plot from sample data...")
    try:
        result = subprocess.run([
            sys.executable,
            "benchmarks/plot_benchmarks.py",
            sample_file
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Plot generation successful!")
            print()
            print(result.stdout)
            
            # Check if output file was created
            expected_output = sample_file.replace('.csv', '_throughput_plot.png')
            if os.path.exists(expected_output):
                print(f"✓ Output file created: {expected_output}")
                print()
                print("=" * 70)
                print("SUCCESS! Plotting system is working correctly.")
                print("=" * 70)
                print()
                print("Next steps:")
                print("1. Start your GFS system with --loglevel NONE")
                print("2. Run: .\\benchmarks\\collect-benchmark-data.ps1")
                print("3. Run: python benchmarks\\plot_benchmarks.py benchmark-data.csv")
                print("4. Analyze the generated PNG files")
            else:
                print(f"✗ Expected output file not found: {expected_output}")
        else:
            print("✗ Plot generation failed:")
            print(result.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"✗ Error running plot script: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
