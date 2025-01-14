#
# load useful libraries
#
import matplotlib.pyplot as plt

#
# Define a function for plotting loss/mse against
# validation loss/mse per training epoch
#
def plot_basic_loss(history, metric_base = 'loss', ylabel = 'Loss'):
    epochs = range(1, len(history[metric_base]) + 1)

    plt.figure()
    plt.plot(epochs, history[metric_base], '-.', label = ylabel)
    plt.plot(epochs, history['val_' + metric_base], '-.', label = 'Validation ' + ylabel)
    plt.xlabel('Training Epoch')
    plt.ylabel(ylabel)
    plt.legend()
    plt.tight_layout()
    plt.show()
    plt.close()
