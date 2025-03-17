import duckdb as ddb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm


def main():
    df = ddb.query('SELECT * FROM \'out/*.csv\';').to_df()

    speedups = []
    cardinalities = []
    selectivities = []
    runtimes_partitioned = []
    runtimes_compressed = []
    # iterate over the rows in steps of 2
    for i in range(0, len(df), 2):
        compressed = df.iloc[i]
        partitioned = df.iloc[i + 1]

        # check HTType of compressed and partitioned
        if compressed['HTType'] != 'PARTITIONED_COMPRESSED':
            print("HTType mismatch")
            continue

        if partitioned['HTType'] != 'PARTITIONED':
            print("HTType mismatch")
            continue

        # calculate the speedup
        speedup = partitioned['Probing'] / compressed['Probing']
        speedups.append(speedup)

        runtimes_partitioned.append(partitioned['Probing'])
        runtimes_compressed.append(compressed['Probing'])

        # calculate the cardinality
        cardinality = partitioned['CardinalityBuild']
        cardinalities.append(cardinality)

        # calculate the selectivity
        selectivity = partitioned['CardinalityResult'] / partitioned['CardinalityProbe']
        selectivities.append(selectivity)

    # plot speedups over cardinalities and selectivities
    plt.scatter(cardinalities, speedups)
    plt.xlabel('Cardinality')
    plt.xscale('log')
    plt.ylabel('Speedup')
    plt.title('Speedup over cardinality')
    plt.tight_layout()
    plt.savefig('out/speedup_over_cardinality.png')
    plt.clf()

    plt.scatter(selectivities, speedups)
    plt.xlabel('Selectivity')
    plt.xscale('log')
    plt.ylabel('Speedup')
    plt.title('Speedup over selectivity')
    plt.tight_layout()
    plt.savefig('out/speedup_over_selectivity.png')
    plt.clf()

    # Scatter plot with logarithmic color scale
    plt.scatter(cardinalities, speedups, c=selectivities, cmap='viridis', norm=LogNorm())

    # Labels and title
    plt.xlabel('Cardinality')
    plt.xscale('log')
    plt.ylabel('Speedup')
    plt.title('Speedup over cardinality, colored by selectivity')

    # Colorbar with label
    cbar = plt.colorbar()
    cbar.set_label('Selectivity')
    plt.ylim(0, 3)
    plt.axhline(y=1, color='r', linestyle='--')

    # Finalize plot
    plt.tight_layout()
    plt.savefig('out/speedup_over_cardinality_colored_by_selectivity.png')


    # Convert HTType to numeric categories
    df['HTType_cat'], uniques = pd.factorize(df['HTType'])

    # *** COLLISIONS VS PROBING ***
    scatter = plt.scatter(
        df['ProbeCollisionRateSalt'] + df['ProbeCollisionRateKey'],
        df['CardinalityProbe'] / df['Probing'],
        c=df['HTType_cat'],
        cmap='viridis',
        alpha=0.8
    )

    # Add a legend with original labels
    handles, _ = scatter.legend_elements()
    plt.legend(handles, uniques, title='HTType')

    plt.xlabel('ProbeCollisionRateSalt')
    plt.ylabel('Probing Speed [Tuples per ms]')
    plt.title('Scatter plot grouped by HTType')

    plt.tight_layout()
    plt.savefig('out/collisions_vs_probing.png')
    plt.clf()

    # *** BUILD SIDE SIZE VS PROBING ***

    scatter = plt.scatter(
        df['CardinalityBuild'],
        df['CardinalityProbe'] / df['Probing'],
        c=df['HTType_cat'],
        cmap='viridis',
        alpha=0.8
    )

    # Add a legend with original labels
    handles, _ = scatter.legend_elements()
    plt.legend(handles, uniques, title='HTType')

    plt.xlabel('CardinalityBuild')
    plt.ylabel('Probing Speed [Tuples per ms]')
    plt.title('Scatter plot grouped by HTType')

    plt.tight_layout()
    plt.savefig('out/build_side_size_vs_probing.png')
    plt.clf()

    # *** SELECTIVITY VS PROBING ***
    scatter = plt.scatter(
        df['CardinalityResult'] / df['CardinalityProbe'],
        df['CardinalityProbe'] / df['Probing'],
        c=df['HTType_cat'],
        cmap='viridis',
        alpha=0.8
    )

    # Add a legend with original labels
    handles, _ = scatter.legend_elements()
    plt.legend(handles, uniques, title='HTType')

    plt.xlabel('Selectivity')
    plt.ylabel('Probing Speed [Tuples per ms]')
    # set range from 0 to 3, add line at 1
    plt.ylim(0, 3)
    plt.axhline(y=1, color='r', linestyle='--')
    plt.title('Scatter plot grouped by HTType')

    plt.tight_layout()
    plt.savefig('out/selectivity_vs_probing.png')
    plt.clf()

    # print average speedup over all runs
    print("Average speedup: ", sum(speedups) / len(speedups))

    # print average runtime of partitioned and compressed
    print("Average runtime partitioned: ", sum(runtimes_partitioned) / len(runtimes_partitioned))
    print("Average runtime compressed: ", sum(runtimes_compressed) / len(runtimes_compressed))





if __name__ == '__main__':
    main()