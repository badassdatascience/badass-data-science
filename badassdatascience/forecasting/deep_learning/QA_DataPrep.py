import numpy as np
import pickle
import matplotlib.pyplot as plt


file_uuid = '053ed580-bcab-45d1-9872-eae70bb66059'
filepath = 'output/' + file_uuid + '_train_val_test_dict.pickled'

with open(filepath, 'rb') as fff:
    train_val_test_dict = pickle.load(fff)

print(train_val_test_dict.keys())
print(train_val_test_dict['train'].keys())


M = train_val_test_dict['train']['M']
y_forward = train_val_test_dict['train']['y_forward']
y = train_val_test_dict['train']['y']



print(M.shape)
print(y_forward.shape)
print(y.shape)




for i in range(0, 200, 10):
    X_range = np.arange(0, M.shape[1])
    y_range = np.arange(M.shape[1], M.shape[1] + y_forward.shape[1])

    yy_min = y[i, 0]
    yy_max = y[i, 1]
    
    
    plt.figure()
    plt.plot(X_range, M[i, :, 0], color = 'blue')
    plt.plot(y_range, y_forward[i], color = 'green')
    plt.axhline(yy_min, color = 'brown')
    plt.axhline(yy_max, color = 'brown')
    plt.show()
    plt.close()

