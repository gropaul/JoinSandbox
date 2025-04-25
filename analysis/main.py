import duckdb as ddb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

def load_data(file_glob='out/*.csv') -> pd.DataFrame:
    """Loads CSV data using DuckDB and returns a pandas DataFrame."""
    return ddb.query(f"SELECT * FROM '{file_glob}';").to_df()

def pair_partitioned_compressed(df: pd.DataFrame,
                                ht_type_compressed='PARTITIONED_COMPRESSED',
                                ht_type_partitioned='PARTITIONED') -> pd.DataFrame:
    """
    Returns a new DataFrame that pairs up the rows for PARTITIONED_COMPRESSED
    and PARTITIONED by taking them in steps of 2.
    Expects that every even row is compressed, and every odd row is partitioned.
    """
    records = []
    # We assume the data is well-formed and sorted so that consecutive pairs match.
    for i in range(0, len(df), 2):
        comp = df.iloc[i]
        part = df.iloc[i + 1]

        # Check HTTypes
        if comp['HTType'] != ht_type_compressed or part['HTType'] != ht_type_partitioned:
            # If there's a mismatch, just skip (or raise an error).
            continue

        # Combine relevant columns into one record
        record = {
            'CardinalityBuild': part['CardinalityBuild'],
            'CardinalityProbe': part['CardinalityProbe'],
            'Probe to Build Ratio': part['CardinalityProbe'] / part['CardinalityBuild'],
            'CardinalityResult': part['CardinalityResult'],
            'Partitioned_Probing': part['Probing'],
            'Compressed_Probing': comp['Probing'],
            'Partitioned_TotalTime': part['Total'],
            'Compressed_TotalTime': comp['Total'],
        }
        records.append(record)

    return pd.DataFrame.from_records(records)

def compute_speedups(df_pairs: pd.DataFrame,
                     col_partitioned='Partitioned_Probing',
                     col_compressed='Compressed_Probing') -> pd.DataFrame:
    """
    Given a DataFrame that already pairs partitioned/compressed runtimes,
    compute speedup, selectivity, etc. and return a new DataFrame
    (same rows) with new columns for these metrics.
    """
    df_pairs = df_pairs.copy()

    # Skip rows with zeros to avoid division by zero
    df_pairs = df_pairs[
        (df_pairs[col_partitioned] != 0) & (df_pairs[col_compressed] != 0)
        ].copy()

    df_pairs['Probe Speedup'] = df_pairs[col_partitioned] / df_pairs[col_compressed]
    df_pairs['Total Speedup'] = df_pairs['Partitioned_TotalTime'] / df_pairs['Compressed_TotalTime']
    df_pairs['Selectivity'] = df_pairs['CardinalityResult'] / df_pairs['CardinalityProbe']

    return df_pairs

def plot_speedup_over_cardinality(df: pd.DataFrame,
                                  speedup_col='Probe Speedup',
                                  cardinality_col='CardinalityBuild',
                                  out_path='out/speedup_over_cardinality.png'):
    plt.scatter(df[cardinality_col], df[speedup_col])
    plt.xlabel('Cardinality')
    plt.xscale('log')
    plt.ylabel('Speedup')
    plt.title('Speedup over Cardinality')
    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def plot_speedup_over_selectivity(df: pd.DataFrame,
                                  speedup_col='Probe Speedup',
                                  selectivity_col='Selectivity',
                                  out_path='out/speedup_over_selectivity.png'):
    plt.scatter(df[selectivity_col], df[speedup_col])
    plt.xlabel('Selectivity')
    plt.xscale('log')
    plt.ylabel('Speedup')
    plt.title('Speedup over Selectivity')
    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def plot_colored_by_selectivity(df: pd.DataFrame,
                                x_col='CardinalityBuild',
                                y_col='Probe Speedup',
                                c_col='Selectivity',
                                out_path='out/speedup_over_cardinality_colored_by_selectivity_probe.png'):
    plt.scatter(df[x_col], df[y_col], c=df[c_col], cmap='viridis', norm=LogNorm())
    plt.xlabel(x_col)
    plt.xscale('log')
    plt.ylabel(y_col)
    plt.title(f'{y_col} over {x_col}, colored by Selectivity')

    cbar = plt.colorbar()
    cbar.set_label('Selectivity')

    # Example y-limit, average speedup line
    plt.ylim(0, 4)
    plt.axhline(y=1, color='r', linestyle='--')

    avg_speedup = df[y_col].mean()
    plt.axhline(y=avg_speedup, color='g', linestyle='--')
    plt.text(1e6, avg_speedup, f'Average speedup: {avg_speedup:.2f}',
             fontsize=9, verticalalignment='top')

    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def factorize_ht_type(df: pd.DataFrame, col_name='HTType') -> pd.DataFrame:
    """Adds a numeric category column for the given HTType column."""
    df = df.copy()
    df['HTType_cat'], uniques = pd.factorize(df[col_name])
    return df, uniques

def plot_collisions_vs_probing(df: pd.DataFrame,
                               x_col_salt='ProbeCollisionRateSalt',
                               x_col_key='ProbeCollisionRateKey',
                               y_numer='CardinalityProbe',
                               y_denom='Probing',
                               cat_col='HTType_cat',
                               cat_labels=None,
                               out_path='out/collisions_vs_probing.png'):
    """
    Example scatter: (ProbeCollisionRateSalt + ProbeCollisionRateKey) vs (CardinalityProbe / Probing)
    colored by HTType_cat.
    """
    x_vals = df[x_col_salt] + df[x_col_key]
    y_vals = df[y_numer] / df[y_denom]
    scatter = plt.scatter(x_vals, y_vals, c=df[cat_col], cmap='viridis', alpha=0.8)

    # Add a legend with the original HTType labels
    if cat_labels is not None:
        handles, _ = scatter.legend_elements()
        plt.legend(handles, cat_labels, title='HTType')

    plt.xlabel('ProbeCollisionRateSalt + ProbeCollisionRateKey')
    plt.ylabel('Probing Speed [Tuples per ms]')
    plt.title('Collisions vs Probing Speed')
    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def plot_build_side_size_vs_probing(df: pd.DataFrame,
                                    x_col='CardinalityBuild',
                                    y_numer='CardinalityProbe',
                                    y_denom='Probing',
                                    cat_col='HTType_cat',
                                    cat_labels=None,
                                    out_path='out/build_side_size_vs_probing.png'):
    x_vals = df[x_col]
    y_vals = df[y_numer] / df[y_denom]
    scatter = plt.scatter(x_vals, y_vals, c=df[cat_col], cmap='viridis', alpha=0.8)

    if cat_labels is not None:
        handles, _ = scatter.legend_elements()
        plt.legend(handles, cat_labels, title='HTType')

    plt.xlabel('CardinalityBuild')
    plt.ylabel('Probing Speed [Tuples per ms]')
    plt.title('Build side size vs Probing Speed')
    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def plot_selectivity_vs_probing(df: pd.DataFrame,
                                selectivity_col='Selectivity',
                                y_numer='CardinalityProbe',
                                y_denom='Probe Speedup',
                                cat_col='HTType_cat',
                                cat_labels=None,
                                out_path='out/selectivity_vs_probing.png'):
    x_vals = df[selectivity_col]
    y_vals = df[y_numer] / df[y_denom]
    scatter = plt.scatter(x_vals, y_vals, c=df[cat_col], cmap='viridis', alpha=0.8)

    if cat_labels is not None:
        handles, _ = scatter.legend_elements()
        plt.legend(handles, cat_labels, title='HTType')

    plt.xlabel('Selectivity')
    plt.ylabel('Probing Speed [Tuples per ms]')
    plt.ylim(0, 4)
    plt.axhline(y=1, color='r', linestyle='--')
    plt.title('Selectivity vs Probing Speed')
    plt.tight_layout()
    plt.savefig(out_path)
    plt.clf()

def main():
    # 1) Load all data
    df_raw = load_data('out/*.csv')

    # 2) Factorize HTType if needed for color-coded plots
    df_fact, ht_labels = factorize_ht_type(df_raw, 'HTType')

    # 3) Pair partitioned + compressed rows for computing speedups
    df_pairs = pair_partitioned_compressed(df_raw)
    df_pairs = compute_speedups(df_pairs)  # adds 'Speedup' and 'Selectivity'

    # 4) Now generate any speedup-based plots
    plot_speedup_over_cardinality(df_pairs)
    plot_speedup_over_selectivity(df_pairs)
    plot_colored_by_selectivity(df_pairs)
    plot_colored_by_selectivity(df_pairs, x_col='Probe to Build Ratio', out_path='out/speedup_over_probe_build_ratio_colored_by_selectivity.png')
    plot_colored_by_selectivity(df_pairs, y_col='Total Speedup', x_col='Probe to Build Ratio', out_path='out/speedup_over_probe_build_ratio_colored_by_selectivity_total.png')
    plot_colored_by_selectivity(df_pairs, y_col='Total Speedup', out_path='out/speedup_over_cardinality_colored_by_selectivity_total.png')

    # 5) Print some summary metrics (example)
    avg_speedup = df_pairs['Probe Speedup'].mean()
    avg_runtime_partitioned = df_pairs['Partitioned_Probing'].mean()
    avg_runtime_compressed = df_pairs['Compressed_Probing'].mean()

    print("Average speedup:", avg_speedup)
    print("Average runtime partitioned:", avg_runtime_partitioned)
    print("Average runtime compressed:", avg_runtime_compressed)

    # 6) Plots that rely on raw data but color-coded by factorized HTType
    plot_collisions_vs_probing(df_fact,
                               cat_labels=ht_labels)
    plot_build_side_size_vs_probing(df_fact,
                                    cat_labels=ht_labels)
    # If you want to re-use the same selectivity logic from df_pairs,
    # you might re-merge it back onto df_fact or simply compute on the raw df.
    # df_fact['Selectivity'] = df_fact['CardinalityResult'] / df_fact['CardinalityProbe']
    # plot_selectivity_vs_probing(df_fact,
    #                             cat_labels=ht_labels)
    #


if __name__ == '__main__':
    main()
