
import numpy as np
from matplotlib import pyplot as plt

def plot_yo(yo, profile_times):
    """Plot the glider yo and the indexed profiles"""
    
    plt.plot(yo[:,0], -yo[:,1], 'k.')
    
    for p in profile_times:
    
        # Create the profile by finding all timestamps in yo that are included in the
        # window p
        pro = yo[np.logical_and(yo[:,0] >= p[0], yo[:,0] <= p[1])]
        
        pro = pro[np.all(~np.isnan(pro),axis=1)]

        plt.plot(pro[:,0], -pro[:,1])
        
    plt.show()

