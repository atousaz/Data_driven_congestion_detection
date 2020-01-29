import numpy as np
import seaborn
import cProfile
import pandas as pd
import bayesian_changepoint_detection.offline_changepoint_detection as offcd
from functools import partial
from multiprocessing  import Pool
import time

start = time.time()
def cp_func1(d):
    signal= (d[['smooth_speed']]).values

    #print(signal)
    Q, P, Pcp = offcd.offline_changepoint_detection(signal, partial(offcd.const_prior, l=(len(signal)+1)), offcd.gaussian_obs_log_likelihood, truncate=-50)
    j2=(np.exp(Pcp).sum(0))
    j2 = np.insert(j2,0,0)
    series = pd.DataFrame({'Speed': signal.flatten(),'Prob': j2,'time':d.time, 'segment':d.segment, 'date':d.date, 'raw_speed':d.speed,'Traveltime':d.traveltime,'score':d.score_30,'ref_speed':d.ref_speed })

    #series.to_csv('C:/Users/atousaz/Desktop/loop/CP/'+str(segment)+'/'+str(date1)+'.csv')

    #complete = time.time()
    #print(c, 'done in ', (complete-now), ' s')
    return series

seg_list= pd.read_csv('C:/Users/atousaz/Desktop/loop/Seg_half1.csv')
seg=seg_list.seg
for s in seg:
    df = pd.read_csv('C:/Users/atousaz/Desktop/loop/wavelet/Wavelet_'+str(s)+'.csv')
    print(s)
    if __name__ == '__main__':
        seq = [df[df.date==date] for date in np.unique(df.date)]
        pool=Pool(4)
        results = pd.concat(pool.map(cp_func1, seq))
        print('got results')
        pool.close()
        pool.join()
        results.to_csv('C:/Users/atousaz/Desktop/loop/CP/'+str(s)+'/'+str(s)+'.csv')


end = time.time()
print((end - start)*0.000277778)




