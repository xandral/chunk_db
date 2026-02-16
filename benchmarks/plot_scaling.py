#!/usr/bin/env python3
"""
Plot column scaling benchmark results
Shows how performance varies with number of columns selected
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sys

def load_results(filename='benchmark_results.csv'):
    """Load benchmark results"""
    try:
        df = pd.read_csv(filename)
        print(f"✓ Loaded {len(df)} results from {filename}")
        return df
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        sys.exit(1)

def plot_scaling_curves(df):
    """Plot performance vs number of columns for each query type"""
    query_types = df['query_type'].unique()

    fig, axes = plt.subplots(1, len(query_types), figsize=(18, 5))
    if len(query_types) == 1:
        axes = [axes]

    for idx, query_type in enumerate(query_types):
        query_df = df[df['query_type'] == query_type]

        # Group by num_columns and compute statistics
        scaling = query_df.groupby('num_columns')['median_ms'].agg(['mean', 'std', 'min', 'max']).reset_index()

        # Plot mean with error bars
        axes[idx].errorbar(scaling['num_columns'], scaling['mean'],
                          yerr=scaling['std'], marker='o', capsize=5,
                          linewidth=2, markersize=8, label='Mean ± Std')

        # Plot min/max range as shaded area
        axes[idx].fill_between(scaling['num_columns'], scaling['min'], scaling['max'],
                               alpha=0.2, label='Min-Max Range')

        axes[idx].set_xlabel('Number of Columns Selected', fontsize=12)
        axes[idx].set_ylabel('Median Time (ms)', fontsize=12)
        axes[idx].set_title(f'{query_type}\nPerformance vs Column Count', fontsize=13, fontweight='bold')
        axes[idx].grid(True, alpha=0.3)
        axes[idx].legend()

        # Add value labels on points
        for _, row in scaling.iterrows():
            axes[idx].annotate(f'{row["mean"]:.1f}ms',
                              (row['num_columns'], row['mean']),
                              textcoords="offset points", xytext=(0,10),
                              ha='center', fontsize=9)

    plt.tight_layout()
    plt.savefig('scaling_curves.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: scaling_curves.png')
    plt.close()

def plot_overhead_per_column(df):
    """Plot overhead per additional column"""
    query_types = df['query_type'].unique()

    fig, axes = plt.subplots(1, len(query_types), figsize=(18, 5))
    if len(query_types) == 1:
        axes = [axes]

    for idx, query_type in enumerate(query_types):
        query_df = df[df['query_type'] == query_type]

        # Calculate average time per column count
        scaling = query_df.groupby('num_columns')['median_ms'].mean().reset_index()
        scaling = scaling.sort_values('num_columns')

        # Calculate marginal cost per column
        if len(scaling) > 1:
            marginal_costs = []
            col_ranges = []

            for i in range(1, len(scaling)):
                prev_cols = scaling.iloc[i-1]['num_columns']
                curr_cols = scaling.iloc[i]['num_columns']
                prev_time = scaling.iloc[i-1]['median_ms']
                curr_time = scaling.iloc[i]['median_ms']

                delta_time = curr_time - prev_time
                delta_cols = curr_cols - prev_cols
                cost_per_col = delta_time / delta_cols if delta_cols > 0 else 0

                marginal_costs.append(cost_per_col)
                col_ranges.append(f"{prev_cols}→{curr_cols}")

            # Plot bar chart
            x_pos = np.arange(len(col_ranges))
            bars = axes[idx].bar(x_pos, marginal_costs, color='steelblue', alpha=0.7)

            axes[idx].set_xlabel('Column Range', fontsize=12)
            axes[idx].set_ylabel('Cost per Additional Column (ms)', fontsize=12)
            axes[idx].set_title(f'{query_type}\nMarginal Cost per Column', fontsize=13, fontweight='bold')
            axes[idx].set_xticks(x_pos)
            axes[idx].set_xticklabels(col_ranges, rotation=45, ha='right')
            axes[idx].grid(True, alpha=0.3, axis='y')

            # Add value labels on bars
            for bar, cost in zip(bars, marginal_costs):
                height = bar.get_height()
                axes[idx].text(bar.get_x() + bar.get_width()/2., height,
                              f'{cost:.3f}ms',
                              ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig('overhead_per_column.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: overhead_per_column.png')
    plt.close()

def plot_config_comparison(df):
    """Compare best vs worst config for each column count"""
    query_types = df['query_type'].unique()

    fig, axes = plt.subplots(1, len(query_types), figsize=(18, 5))
    if len(query_types) == 1:
        axes = [axes]

    for idx, query_type in enumerate(query_types):
        query_df = df[df['query_type'] == query_type]

        col_counts = sorted(query_df['num_columns'].unique())

        best_times = []
        worst_times = []
        avg_times = []

        for num_cols in col_counts:
            col_data = query_df[query_df['num_columns'] == num_cols]
            best_times.append(col_data['median_ms'].min())
            worst_times.append(col_data['median_ms'].max())
            avg_times.append(col_data['median_ms'].mean())

        x = np.arange(len(col_counts))
        width = 0.25

        axes[idx].bar(x - width, best_times, width, label='Best Config', color='green', alpha=0.7)
        axes[idx].bar(x, avg_times, width, label='Average', color='blue', alpha=0.7)
        axes[idx].bar(x + width, worst_times, width, label='Worst Config', color='red', alpha=0.7)

        axes[idx].set_xlabel('Number of Columns', fontsize=12)
        axes[idx].set_ylabel('Median Time (ms)', fontsize=12)
        axes[idx].set_title(f'{query_type}\nBest vs Worst Configuration', fontsize=13, fontweight='bold')
        axes[idx].set_xticks(x)
        axes[idx].set_xticklabels(col_counts)
        axes[idx].legend()
        axes[idx].grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig('config_comparison.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: config_comparison.png')
    plt.close()

def plot_query_comparison(df):
    """Compare all query types on same plot"""
    query_types = df['query_type'].unique()

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = ['#2ecc71', '#3498db', '#e74c3c']
    markers = ['o', 's', '^']

    for idx, query_type in enumerate(query_types):
        query_df = df[df['query_type'] == query_type]
        scaling = query_df.groupby('num_columns')['median_ms'].agg(['mean', 'std']).reset_index()

        ax.errorbar(scaling['num_columns'], scaling['mean'],
                   yerr=scaling['std'], marker=markers[idx % len(markers)],
                   capsize=5, linewidth=2, markersize=8,
                   label=query_type, color=colors[idx % len(colors)])

    ax.set_xlabel('Number of Columns Selected', fontsize=13)
    ax.set_ylabel('Median Time (ms)', fontsize=13)
    ax.set_title('Query Type Comparison\nPerformance vs Column Count', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11)

    plt.tight_layout()
    plt.savefig('query_comparison.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: query_comparison.png')
    plt.close()

def generate_analysis_report(df):
    """Generate detailed scaling analysis"""
    with open('scaling_analysis.txt', 'w') as f:
        f.write("Column Scaling Analysis Report\n")
        f.write("=" * 60 + "\n\n")

        for query_type in df['query_type'].unique():
            query_df = df[df['query_type'] == query_type]

            f.write(f"Query Type: {query_type}\n")
            f.write("-" * 60 + "\n")

            scaling = query_df.groupby('num_columns')['median_ms'].agg(['mean', 'std', 'min', 'max']).reset_index()
            scaling = scaling.sort_values('num_columns')

            f.write(f"\nPerformance by Column Count:\n")
            f.write(f"{'Columns':<10} {'Mean (ms)':<12} {'Std (ms)':<12} {'Min (ms)':<12} {'Max (ms)':<12}\n")
            f.write("-" * 60 + "\n")

            for _, row in scaling.iterrows():
                f.write(f"{int(row['num_columns']):<10} {row['mean']:<12.2f} {row['std']:<12.2f} {row['min']:<12.2f} {row['max']:<12.2f}\n")

            # Scaling ratio
            if len(scaling) > 1:
                base_time = scaling.iloc[0]['mean']
                max_time = scaling.iloc[-1]['mean']
                base_cols = scaling.iloc[0]['num_columns']
                max_cols = scaling.iloc[-1]['num_columns']

                f.write(f"\nScaling Analysis:\n")
                f.write(f"  Baseline ({int(base_cols)} col):  {base_time:.2f}ms\n")
                f.write(f"  Maximum ({int(max_cols)} cols): {max_time:.2f}ms\n")
                f.write(f"  Overhead: +{max_time - base_time:.2f}ms ({max_time/base_time:.2f}x)\n")
                f.write(f"  Per extra column: {(max_time - base_time)/(max_cols - base_cols):.3f}ms\n")

            # Best configuration
            best_row = query_df.loc[query_df['median_ms'].idxmin()]
            f.write(f"\nBest Configuration:\n")
            f.write(f"  Columns: {best_row['num_columns']}\n")
            f.write(f"  Time: {best_row['median_ms']:.2f}ms\n")
            f.write(f"  Config: chunk_rows={int(best_row['chunk_rows'])}, ")
            f.write(f"hash_buckets={int(best_row['hash_buckets'])}, ")
            f.write(f"range_dim={int(best_row['range_dim_size'])}\n")

            f.write("\n" + "=" * 60 + "\n\n")

    print('✓ Saved: scaling_analysis.txt')

def main():
    print("\n" + "=" * 60)
    print("ChunkDB Column Scaling Analysis")
    print("=" * 60 + "\n")

    # Load data
    df = load_results()

    print(f"\nDataset summary:")
    print(f"  Total measurements: {len(df)}")
    print(f"  Query types: {df['query_type'].unique().tolist()}")
    print(f"  Column counts: {sorted(df['num_columns'].unique())}")
    print(f"  Configurations: {len(df.groupby(['chunk_rows', 'hash_buckets', 'range_dim_size']))}")
    print()

    # Generate plots
    print("Generating plots...")
    plot_scaling_curves(df)
    plot_overhead_per_column(df)
    plot_config_comparison(df)
    plot_query_comparison(df)

    # Generate analysis
    print("\nGenerating analysis report...")
    generate_analysis_report(df)

    print("\n" + "=" * 60)
    print("✓ All plots and reports generated!")
    print("=" * 60)
    print("\nGenerated files:")
    print("  • scaling_curves.png - Performance vs column count")
    print("  • overhead_per_column.png - Marginal cost per column")
    print("  • config_comparison.png - Best vs worst configs")
    print("  • query_comparison.png - Query type comparison")
    print("  • scaling_analysis.txt - Detailed analysis")
    print()

if __name__ == '__main__':
    main()
