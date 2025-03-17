import numpy as np
import matplotlib.pyplot as plt

def get_probability_for_full_bucket(bucket_size: int, fill_rate: float) -> float:
    prob_slot_full = fill_rate
    prob_bucket_full =  prob_slot_full ** bucket_size
    return prob_bucket_full


def main():

    fill_rates = np.linspace(0.1, 0.6, 100)
    bucket_sizes = [4, 8, 16, 32]

    for bucket_size in bucket_sizes:
        probabilities = [get_probability_for_full_bucket(bucket_size, fill_rate) for fill_rate in fill_rates]
        plt.plot(fill_rates, probabilities, label=f'bucket size {bucket_size}')

    # mark range between 0.28 and 0.5 as likely fill rates
    plt.axvspan(0.28, 0.5, color='gray', alpha=0.1, label='Likely fill rates')



    # add points for the fill rates 0.28 and 0.5, together with the probabilities
    for bucket_size in bucket_sizes:
        for fill_rate in [0.28, 0.5]:
            plt.scatter(fill_rate, get_probability_for_full_bucket(bucket_size, fill_rate), color='red')
            plt.text(fill_rate, get_probability_for_full_bucket(bucket_size, fill_rate), f'{get_probability_for_full_bucket(bucket_size, fill_rate)*100:.2f}%', fontsize=9, verticalalignment='bottom')

    plt.legend()
    plt.xlabel('Fill rate')
    plt.ylabel('Probability')
    plt.title('Probability of full bucket')

    plt.tight_layout()
    plt.savefig('ht_probs.png')


if __name__ == '__main__':
    main()
