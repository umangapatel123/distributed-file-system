import matplotlib.pyplot as plt
import pandas as pd
import sys
from datetime import datetime

def plot_throughput(csv_file):
    """Plot throughput vs number of clients"""
    df = pd.read_csv(csv_file)
    
    # Filter throughput benchmarks
    throughput_df = df[df['benchmark_type'] == 'throughput']
    
    if len(throughput_df) == 0:
        print("No throughput data found")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('GFS Exactly-Once Append - Throughput Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Throughput vs Clients
    ax1 = axes[0, 0]
    ax1.plot(throughput_df['clients'], throughput_df['throughput_ops'], 'o-', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax1.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax1.set_title('Throughput vs Concurrent Clients')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Latency vs Clients
    ax2 = axes[0, 1]
    ax2.plot(throughput_df['clients'], throughput_df['avg_latency_ms'], 'ro-', label='Average', linewidth=2, markersize=8)
    ax2.plot(throughput_df['clients'], throughput_df['min_latency_ms'], 'go-', label='Min', linewidth=2, markersize=8)
    ax2.plot(throughput_df['clients'], throughput_df['max_latency_ms'], 'bo-', label='Max', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax2.set_ylabel('Latency (ms)', fontsize=12)
    ax2.set_title('Latency vs Concurrent Clients')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Data Throughput (MB/s)
    ax3 = axes[1, 0]
    ax3.plot(throughput_df['clients'], throughput_df['throughput_mbs'], 'mo-', linewidth=2, markersize=8)
    ax3.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax3.set_ylabel('Data Throughput (MB/s)', fontsize=12)
    ax3.set_title('Data Throughput vs Concurrent Clients')
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Efficiency (Throughput per Client)
    ax4 = axes[1, 1]
    efficiency = throughput_df['throughput_ops'] / throughput_df['clients']
    ax4.plot(throughput_df['clients'], efficiency, 'co-', linewidth=2, markersize=8)
    ax4.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax4.set_ylabel('Ops/sec per Client', fontsize=12)
    ax4.set_title('Client Efficiency (Throughput/Client)')
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_file = csv_file.replace('.csv', '_throughput_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Throughput plot saved to: {output_file}")
    plt.close()

def plot_latency(csv_file):
    """Plot latency distribution"""
    df = pd.read_csv(csv_file)
    
    # Filter latency benchmarks
    latency_df = df[df['benchmark_type'] == 'latency']
    
    if len(latency_df) == 0:
        print("No latency data found")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('GFS Exactly-Once Append - Latency Analysis', fontsize=16, fontweight='bold')
    
    # Convert latency strings to milliseconds (assuming format like "123ms" or "1.23s")
    def parse_latency(lat_str):
        if pd.isna(lat_str):
            return None
        lat_str = str(lat_str).strip()
        if lat_str.endswith('ms'):
            return float(lat_str.replace('ms', ''))
        elif lat_str.endswith('s'):
            return float(lat_str.replace('s', '')) * 1000  # Convert seconds to ms
        elif lat_str.endswith('µs'):
            return float(lat_str.replace('µs', '')) / 1000  # Convert microseconds to ms
        else:
            return float(lat_str)
    
    latency_df['p50_ms'] = latency_df['p50'].apply(parse_latency)
    latency_df['p95_ms'] = latency_df['p95'].apply(parse_latency)
    latency_df['p99_ms'] = latency_df['p99'].apply(parse_latency)
    latency_df['p999_ms'] = latency_df['p999'].apply(parse_latency)
    latency_df['avg_ms'] = latency_df['avg_latency'].apply(parse_latency)
    latency_df['max_ms'] = latency_df['max_latency'].apply(parse_latency)
    
    # Plot 1: Percentile Distribution
    ax1 = axes[0, 0]
    clients = latency_df['clients'].values
    ax1.plot(clients, latency_df['p50_ms'], 'o-', label='P50 (Median)', linewidth=2, markersize=8)
    ax1.plot(clients, latency_df['p95_ms'], 's-', label='P95', linewidth=2, markersize=8)
    ax1.plot(clients, latency_df['p99_ms'], '^-', label='P99', linewidth=2, markersize=8)
    ax1.plot(clients, latency_df['p999_ms'], 'd-', label='P99.9', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax1.set_ylabel('Latency (ms)', fontsize=12)
    ax1.set_title('Latency Percentiles vs Concurrent Load')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale('log')  # Log scale for better visualization
    
    # Plot 2: Average vs P50 (shows distribution skew)
    ax2 = axes[0, 1]
    ax2.plot(clients, latency_df['avg_ms'], 'ro-', label='Average', linewidth=2, markersize=8)
    ax2.plot(clients, latency_df['p50_ms'], 'bo-', label='Median (P50)', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax2.set_ylabel('Latency (ms)', fontsize=12)
    ax2.set_title('Average vs Median Latency (Distribution Skew)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_yscale('log')
    
    # Plot 3: Tail Latency (P99 and P99.9)
    ax3 = axes[1, 0]
    ax3.plot(clients, latency_df['p99_ms'], '^-', label='P99', linewidth=2, markersize=8, color='orange')
    ax3.plot(clients, latency_df['p999_ms'], 'd-', label='P99.9', linewidth=2, markersize=8, color='red')
    ax3.plot(clients, latency_df['max_ms'], 'x-', label='Max', linewidth=2, markersize=8, color='darkred')
    ax3.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax3.set_ylabel('Latency (ms)', fontsize=12)
    ax3.set_title('Tail Latency Analysis')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_yscale('log')
    
    # Plot 4: Latency Range (Stacked)
    ax4 = axes[1, 1]
    x_pos = range(len(clients))
    ax4.bar(x_pos, latency_df['max_ms'], color='lightcoral', alpha=0.6, label='Max')
    ax4.bar(x_pos, latency_df['p999_ms'], color='orange', alpha=0.7, label='P99.9')
    ax4.bar(x_pos, latency_df['p99_ms'], color='yellow', alpha=0.7, label='P99')
    ax4.bar(x_pos, latency_df['p50_ms'], color='green', alpha=0.8, label='P50')
    ax4.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax4.set_ylabel('Latency (ms)', fontsize=12)
    ax4.set_title('Latency Distribution by Client Count')
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(clients)
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.set_yscale('log')
    
    plt.tight_layout()
    output_file = csv_file.replace('.csv', '_latency_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Latency plot saved to: {output_file}")
    plt.close()

def plot_scaling(csv_file):
    """Plot scaling characteristics"""
    df = pd.read_csv(csv_file)
    
    # Filter scaling benchmarks
    scaling_df = df[df['benchmark_type'] == 'scaling']
    
    if len(scaling_df) == 0:
        print("No scaling data found")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('GFS Exactly-Once Append - Scaling Analysis', fontsize=16, fontweight='bold')
    
    # Group by payload size
    unique_payloads = scaling_df['payload_size'].unique()
    
    # Plot 1: Throughput vs Clients (grouped by payload)
    ax1 = axes[0, 0]
    for payload in unique_payloads:
        subset = scaling_df[scaling_df['payload_size'] == payload]
        ax1.plot(subset['clients'], subset['throughput_ops'], 'o-', label=f'{payload}B', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax1.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax1.set_title('Throughput Scaling by Payload Size')
    ax1.legend(title='Payload')
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Latency vs Clients (grouped by payload)
    ax2 = axes[0, 1]
    for payload in unique_payloads:
        subset = scaling_df[scaling_df['payload_size'] == payload]
        ax2.plot(subset['clients'], subset['avg_latency_ms'], 'o-', label=f'{payload}B', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Concurrent Clients', fontsize=12)
    ax2.set_ylabel('Average Latency (ms)', fontsize=12)
    ax2.set_title('Latency Scaling by Payload Size')
    ax2.legend(title='Payload')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Throughput vs Payload Size (grouped by clients)
    ax3 = axes[1, 0]
    unique_clients = sorted(scaling_df['clients'].unique())
    for clients in unique_clients:
        subset = scaling_df[scaling_df['clients'] == clients]
        ax3.plot(subset['payload_size'], subset['throughput_ops'], 'o-', label=f'{clients} clients', linewidth=2, markersize=8)
    ax3.set_xlabel('Payload Size (bytes)', fontsize=12)
    ax3.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax3.set_title('Throughput vs Payload Size')
    ax3.set_xscale('log')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Success Rate Heatmap
    ax4 = axes[1, 1]
    pivot = scaling_df.pivot_table(values='success_rate', index='clients', columns='payload_size')
    im = ax4.imshow(pivot.values, cmap='RdYlGn', aspect='auto', vmin=0, vmax=100)
    ax4.set_xticks(range(len(pivot.columns)))
    ax4.set_yticks(range(len(pivot.index)))
    ax4.set_xticklabels(pivot.columns)
    ax4.set_yticklabels(pivot.index)
    ax4.set_xlabel('Payload Size (bytes)', fontsize=12)
    ax4.set_ylabel('Number of Clients', fontsize=12)
    ax4.set_title('Success Rate Heatmap (%)')
    plt.colorbar(im, ax=ax4)
    
    plt.tight_layout()
    output_file = csv_file.replace('.csv', '_scaling_plot.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Scaling plot saved to: {output_file}")
    plt.close()

def plot_all(csv_file):
    """Generate all plots from benchmark data"""
    print(f"Reading benchmark data from: {csv_file}")
    
    try:
        # Read CSV with no header, then parse based on row type
        # Throughput: 10 columns
        # Latency: 12 columns (has P50, P95, P99, P99.9 percentiles)
        
        throughput_data = []
        latency_data = []
        
        with open(csv_file, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 10:
                    # Throughput row
                    throughput_data.append(parts)
                elif len(parts) >= 12:
                    # Latency row (may have extra columns)
                    latency_data.append(parts[:12])  # Take first 12 columns
        
        # Create DataFrames
        throughput_cols = ['timestamp', 'benchmark_type', 'clients', 'payload_size', 
                          'throughput_ops', 'throughput_mbs', 'avg_latency_ms', 
                          'min_latency_ms', 'max_latency_ms', 'duration']
        
        latency_cols = ['timestamp', 'benchmark_type', 'clients', 'payload_size',
                       'ops', 'avg_latency', 'p50', 'p95', 'p99', 'p999', 
                       'max_latency', 'duration']
        
        if throughput_data:
            throughput_df = pd.DataFrame(throughput_data, columns=throughput_cols)
            # Convert numeric columns
            numeric_cols = ['clients', 'payload_size', 'throughput_ops', 'throughput_mbs',
                          'avg_latency_ms', 'min_latency_ms', 'max_latency_ms', 'duration']
            for col in numeric_cols:
                throughput_df[col] = pd.to_numeric(throughput_df[col], errors='coerce')
            
            # Save to temporary file for throughput plotting
            throughput_df.to_csv('temp_throughput.csv', index=False)
            plot_throughput('temp_throughput.csv')
        
        if latency_data:
            latency_df = pd.DataFrame(latency_data, columns=latency_cols)
            # Convert numeric columns
            numeric_cols = ['clients', 'payload_size', 'ops']
            for col in numeric_cols:
                latency_df[col] = pd.to_numeric(latency_df[col], errors='coerce')
            
            # Save to temporary file for latency plotting
            latency_df.to_csv('temp_latency.csv', index=False)
            plot_latency('temp_latency.csv')
        
        print(f"\nFound {len(throughput_data)} throughput data points")
        print(f"Found {len(latency_data)} latency data points")
        print("\nAll plots generated successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python plot_benchmarks.py <benchmark_results.csv>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    plot_all(csv_file)
