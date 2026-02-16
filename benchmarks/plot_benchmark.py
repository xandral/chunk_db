#!/usr/bin/env python3
"""
Plot ChunkDB parametric benchmark results
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import sys

def load_results(filename='benchmark_results.csv'):
    """Load benchmark results from CSV"""
    try:
        df = pd.read_csv(filename)
        print(f"✓ Loaded {len(df)} results from {filename}")
        return df
    except FileNotFoundError:
        print(f"Error: {filename} not found. Run the benchmark first:")
        print("  cargo run --example parametric_benchmark --release")
        sys.exit(1)

def plot_heatmaps(df):
    """Generate heatmap for each query type"""
    queries = df['query'].unique()

    fig, axes = plt.subplots(1, len(queries), figsize=(18, 5))
    if len(queries) == 1:
        axes = [axes]

    for idx, query in enumerate(queries):
        query_df = df[df['query'] == query]

        # Average across range_dim_size
        pivot = query_df.groupby(['chunk_rows', 'hash_buckets'])['median_ms'].mean().reset_index()
        heatmap_data = pivot.pivot(index='chunk_rows', columns='hash_buckets', values='median_ms')

        sns.heatmap(heatmap_data, annot=True, fmt='.1f', cmap='RdYlGn_r',
                    ax=axes[idx], cbar_kws={'label': 'Median Time (ms)'})
        axes[idx].set_title(f'{query}\n(averaged over range_dim_size)')
        axes[idx].set_xlabel('Hash Buckets')
        axes[idx].set_ylabel('Chunk Rows')

    plt.tight_layout()
    plt.savefig('benchmark_heatmaps.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: benchmark_heatmaps.png')
    plt.close()

def plot_3d_surfaces(df):
    """Generate 3D surface plots for each query"""
    queries = df['query'].unique()

    fig = plt.figure(figsize=(18, 6))

    for idx, query in enumerate(queries):
        ax = fig.add_subplot(1, len(queries), idx + 1, projection='3d')

        query_df = df[df['query'] == query]

        # Average across range_dim_size
        pivot = query_df.groupby(['chunk_rows', 'hash_buckets'])['median_ms'].mean().reset_index()

        X = pivot['chunk_rows'].values
        Y = pivot['hash_buckets'].values
        Z = pivot['median_ms'].values

        # Create grid for surface
        x_unique = sorted(pivot['chunk_rows'].unique())
        y_unique = sorted(pivot['hash_buckets'].unique())
        X_grid, Y_grid = np.meshgrid(x_unique, y_unique)
        Z_grid = pivot.pivot(index='hash_buckets', columns='chunk_rows', values='median_ms').values

        surf = ax.plot_surface(X_grid, Y_grid, Z_grid, cmap='viridis', alpha=0.8)

        ax.set_xlabel('Chunk Rows')
        ax.set_ylabel('Hash Buckets')
        ax.set_zlabel('Median Time (ms)')
        ax.set_title(f'{query}')

        fig.colorbar(surf, ax=ax, shrink=0.5)

    plt.tight_layout()
    plt.savefig('benchmark_3d_surfaces.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: benchmark_3d_surfaces.png')
    plt.close()

def plot_parameter_effects(df):
    """Plot effect of each parameter independently"""
    queries = df['query'].unique()

    fig, axes = plt.subplots(3, len(queries), figsize=(18, 12))

    for q_idx, query in enumerate(queries):
        query_df = df[df['query'] == query]

        # Effect of chunk_rows
        chunk_effect = query_df.groupby('chunk_rows')['median_ms'].agg(['mean', 'std']).reset_index()
        axes[0, q_idx].errorbar(chunk_effect['chunk_rows'], chunk_effect['mean'],
                                yerr=chunk_effect['std'], marker='o', capsize=5)
        axes[0, q_idx].set_xlabel('Chunk Rows')
        axes[0, q_idx].set_ylabel('Median Time (ms)')
        axes[0, q_idx].set_title(f'{query}\nEffect of chunk_rows')
        axes[0, q_idx].grid(True, alpha=0.3)

        # Effect of hash_buckets
        hash_effect = query_df.groupby('hash_buckets')['median_ms'].agg(['mean', 'std']).reset_index()
        axes[1, q_idx].errorbar(hash_effect['hash_buckets'], hash_effect['mean'],
                                yerr=hash_effect['std'], marker='o', capsize=5)
        axes[1, q_idx].set_xlabel('Hash Buckets')
        axes[1, q_idx].set_ylabel('Median Time (ms)')
        axes[1, q_idx].set_title(f'Effect of hash_buckets')
        axes[1, q_idx].grid(True, alpha=0.3)

        # Effect of range_dim_size
        range_effect = query_df.groupby('range_dim_size')['median_ms'].agg(['mean', 'std']).reset_index()
        axes[2, q_idx].errorbar(range_effect['range_dim_size'], range_effect['mean'],
                                yerr=range_effect['std'], marker='o', capsize=5)
        axes[2, q_idx].set_xlabel('Range Dimension Size')
        axes[2, q_idx].set_ylabel('Median Time (ms)')
        axes[2, q_idx].set_title(f'Effect of range_dim_size')
        axes[2, q_idx].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('benchmark_parameter_effects.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: benchmark_parameter_effects.png')
    plt.close()

def plot_best_configs(df):
    """Plot comparison of best configurations for each query"""
    queries = df['query'].unique()

    best_configs = []
    for query in queries:
        query_df = df[df['query'] == query]
        best = query_df.loc[query_df['median_ms'].idxmin()]
        best_configs.append({
            'query': query,
            'median_ms': best['median_ms'],
            'config': f"cr={int(best['chunk_rows'])}\nhb={int(best['hash_buckets'])}\nrd={int(best['range_dim_size'])}"
        })

    best_df = pd.DataFrame(best_configs)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(range(len(best_df)), best_df['median_ms'], color=['#2ecc71', '#3498db', '#e74c3c'])

    ax.set_xlabel('Query Type')
    ax.set_ylabel('Median Time (ms)')
    ax.set_title('Best Configuration for Each Query')
    ax.set_xticks(range(len(best_df)))
    ax.set_xticklabels(best_df['query'])
    ax.grid(True, alpha=0.3, axis='y')

    # Add config labels on bars
    for idx, (bar, config) in enumerate(zip(bars, best_df['config'])):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                config, ha='center', va='bottom', fontsize=9)
        ax.text(bar.get_x() + bar.get_width()/2., height/2,
                f'{height:.2f}ms', ha='center', va='center',
                fontweight='bold', color='white', fontsize=11)

    plt.tight_layout()
    plt.savefig('benchmark_best_configs.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: benchmark_best_configs.png')
    plt.close()

def plot_performance_distribution(df):
    """Plot distribution of performance across all configurations"""
    queries = df['query'].unique()

    fig, axes = plt.subplots(1, len(queries), figsize=(18, 5))
    if len(queries) == 1:
        axes = [axes]

    for idx, query in enumerate(queries):
        query_df = df[df['query'] == query]

        axes[idx].hist(query_df['median_ms'], bins=30, edgecolor='black', alpha=0.7)
        axes[idx].axvline(query_df['median_ms'].mean(), color='red',
                         linestyle='--', linewidth=2, label=f"Mean: {query_df['median_ms'].mean():.2f}ms")
        axes[idx].axvline(query_df['median_ms'].median(), color='green',
                         linestyle='--', linewidth=2, label=f"Median: {query_df['median_ms'].median():.2f}ms")

        axes[idx].set_xlabel('Median Time (ms)')
        axes[idx].set_ylabel('Frequency')
        axes[idx].set_title(f'{query}\nPerformance Distribution')
        axes[idx].legend()
        axes[idx].grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig('benchmark_distribution.png', dpi=300, bbox_inches='tight')
    print('✓ Saved: benchmark_distribution.png')
    plt.close()

def generate_report(df):
    """Generate comprehensive text report"""
    with open('benchmark_analysis.txt', 'w') as f:
        f.write("ChunkDB Parametric Benchmark Analysis\n")
        f.write("=" * 60 + "\n\n")

        for query in df['query'].unique():
            query_df = df[df['query'] == query]

            f.write(f"Query: {query}\n")
            f.write("-" * 60 + "\n")

            best = query_df.loc[query_df['median_ms'].idxmin()]
            worst = query_df.loc[query_df['median_ms'].idxmax()]

            f.write(f"Configurations tested: {len(query_df)}\n")
            f.write(f"Performance range: {query_df['median_ms'].min():.2f}ms - {query_df['median_ms'].max():.2f}ms\n")
            f.write(f"Average: {query_df['median_ms'].mean():.2f}ms (±{query_df['median_ms'].std():.2f}ms)\n")
            f.write(f"Speedup (best vs worst): {worst['median_ms'] / best['median_ms']:.2f}x\n\n")

            f.write(f"BEST Configuration: {best['median_ms']:.2f}ms\n")
            f.write(f"  chunk_rows:      {int(best['chunk_rows'])}\n")
            f.write(f"  hash_buckets:    {int(best['hash_buckets'])}\n")
            f.write(f"  range_dim_size:  {int(best['range_dim_size'])}\n\n")

            f.write(f"WORST Configuration: {worst['median_ms']:.2f}ms\n")
            f.write(f"  chunk_rows:      {int(worst['chunk_rows'])}\n")
            f.write(f"  hash_buckets:    {int(worst['hash_buckets'])}\n")
            f.write(f"  range_dim_size:  {int(worst['range_dim_size'])}\n\n")

            # Parameter correlations
            f.write("Parameter Correlations:\n")
            f.write(f"  chunk_rows:      {query_df['chunk_rows'].corr(query_df['median_ms']):.3f}\n")
            f.write(f"  hash_buckets:    {query_df['hash_buckets'].corr(query_df['median_ms']):.3f}\n")
            f.write(f"  range_dim_size:  {query_df['range_dim_size'].corr(query_df['median_ms']):.3f}\n")
            f.write("\n" + "=" * 60 + "\n\n")

    print('✓ Saved: benchmark_analysis.txt')

def main():
    print("\n" + "=" * 60)
    print("ChunkDB Parametric Benchmark Plotter")
    print("=" * 60 + "\n")

    # Load data
    df = load_results()

    print(f"\nDataset summary:")
    print(f"  Total measurements: {len(df)}")
    print(f"  Queries: {df['query'].unique().tolist()}")
    print(f"  chunk_rows range: {df['chunk_rows'].min():.0f} - {df['chunk_rows'].max():.0f}")
    print(f"  hash_buckets range: {df['hash_buckets'].min():.0f} - {df['hash_buckets'].max():.0f}")
    print(f"  range_dim_size range: {df['range_dim_size'].min():.0f} - {df['range_dim_size'].max():.0f}")
    print()

    # Generate plots
    print("Generating plots...")
    plot_heatmaps(df)
    plot_3d_surfaces(df)
    plot_parameter_effects(df)
    plot_best_configs(df)
    plot_performance_distribution(df)

    # Generate report
    print("\nGenerating analysis report...")
    generate_report(df)

    print("\n" + "=" * 60)
    print("✓ All plots and reports generated!")
    print("=" * 60)
    print("\nGenerated files:")
    print("  • benchmark_heatmaps.png - Performance heatmaps")
    print("  • benchmark_3d_surfaces.png - 3D surface plots")
    print("  • benchmark_parameter_effects.png - Individual parameter effects")
    print("  • benchmark_best_configs.png - Best configuration comparison")
    print("  • benchmark_distribution.png - Performance distributions")
    print("  • benchmark_analysis.txt - Detailed text report")
    print()

if __name__ == '__main__':
    main()
