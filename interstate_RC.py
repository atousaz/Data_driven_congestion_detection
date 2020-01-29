#!/usr/bin/env python3
import numpy as np
import pandas as pd
from multiprocessing  import Pool
import time
from sklearn.cluster import KMeans
from numpy import matlib as mb
from kneed import KneeLocator

start = time.time()
def RC_func(dataset):
    dataset=dataset.reset_index(drop=True)
    try:
        dataset['time1'] = pd.to_datetime(dataset['time'])
        dataset['time1'] = dataset['time1'].dt.time
        dataset=dataset.sort_values(['date','time1'])
        dataset['cp']=0
        dataset['group']="NA"

        # traffic state
        ind=0
        while ind<dataset.shape[0]:
            ind=ind+1
            if(ind+1==dataset.shape[0]):
                break
            else :
                if  (dataset['Prob'][ind]>0.01):
                    for i in range(ind+1,dataset.shape[0]+1) :
                        if (dataset['Prob'][i]<0.01):
                            d=dataset.loc[ind:i-1,'Prob']
                            posiion=max(d)
                            test=dataset.loc[ind:i-1,'Prob']==posiion
                            v=dataset.loc[ind:i-1][test].index
                            dataset.cp[v]=1
                            break
                    ind=i

        #grouping
        counter=1
        l=0
        for ind in range(0,dataset.shape[0]):
            if(dataset.loc[ind,'cp']==1):
                dataset.loc[l:ind,'group']=counter
                counter=counter+1
                l=ind+1
        dataset.loc[dataset['group']=='NA', 'group'] = counter

        #dataset['time1'] = pd.to_datetime(dataset['time'])
        #dataset['time1'] = dataset['time1'].dt.time



        #quantile calculation
        total=pd.DataFrame()
        group=np.unique(dataset.group)
        for g in (group):
            filter=dataset.loc[dataset['group'] == g]
            filter['Q15']=np.quantile(filter.Speed,0.15)
            filter['Q50']=np.quantile(filter.Speed,0.5)
            filter['Q85']=np.quantile(filter.Speed,0.85)
            total=total.append(filter)

        #Labeling
        from sklearn.cluster import KMeans
        kmeans2 = KMeans(n_clusters=2)
        y_kmeans2 = kmeans2.fit(total[['Q15', 'Q50', 'Q85']])
        total["cluster"] = y_kmeans2.labels_
        m=total.groupby('cluster')['Speed'].mean()
        #m.idxmin()
        differ=abs(m[0]-m[1])

        #congestion detection
        total['visual'] = np.where((total['cluster'] ==m.idxmin()) & (differ >16.54)  , total['Speed'], 0)

        #elbow percentages
        r=pd.DataFrame()
        timespan=np.unique(total.time)
        for t in (timespan):
            time_filter=total.loc[total['time']==t]
            con_filter=total.loc[(total['visual']!=0 )&(total['time']==t)]
            time=pd.Series(np.unique(t))
            count=pd.Series(con_filter.shape[0])
            con_percentage=pd.Series(con_filter.shape[0]/time_filter.shape[0])
            percentage=pd.concat([con_percentage,time,count], axis=1)
            r=r.append(percentage)
        r.columns =['con_percentage', 'time','count']
        sorted_percentages=r.sort_values('con_percentage',ascending=False)
        sorted_percentages=pd.DataFrame(sorted_percentages).reset_index(drop=True)

        #Elbow
        curve= sorted_percentages.con_percentage
        nPoints = len(curve)
        allCoord = np.vstack((range(nPoints), curve)).T
        np.array([range(nPoints), curve])
        firstPoint = allCoord[0]
        lineVec = allCoord[-1] - allCoord[0]
        lineVecNorm = lineVec / np.sqrt(np.sum(lineVec**2))
        vecFromFirst = allCoord - firstPoint
        scalarProduct = np.sum(vecFromFirst * mb.repmat(lineVecNorm, nPoints, 1), axis=1)
        vecFromFirstParallel = np.outer(scalarProduct, lineVecNorm)
        vecToLine = vecFromFirst - vecFromFirstParallel
        distToLine = np.sqrt(np.sum(vecToLine ** 2, axis=1))
        idxOfBestPoint = np.argmax(distToLine)
        el_thre=sorted_percentages.loc[idxOfBestPoint]['con_percentage']
        total1=pd.merge(total, sorted_percentages, on='time', how='left')
        cutt_off=total1[total1['con_percentage']>el_thre]
        cutted_time=np.unique(cutt_off['time'])
        RC=total1[total1['visual']!=0]
        out_RC=RC[~RC['time'].isin(cutted_time)]

        return RC
    except KeyError:
        print(dataset.segment,"KEY ERROR HAPPENING HERE")

#seg_list= pd.read_csv('C:/Users/atousaz/Desktop/loop/Seg_half1.csv')
#seg=seg_list.seg
#for s in seg:
#print(s)
    #if __name__ == '__main__':
#df = pd.read_csv('1450447673.csv',header=None)

#seg_list= pd.read_csv('C:/Users/atousaz/Box/Loop/Seg_half1.csv')
#seg=seg_list.seg
#seg=['1485655352','1485848225']
#df = pd.read_csv('/lss/research/itrns-otohpc/inrix_partitioned/2018/'+str(s)+'.csv',header=None)
df = pd.read_csv('C:/Users/atousaz/Box/Loop/MultipleSegments.csv')

if __name__ == '__main__':
    seq = [df[df.segment==seg] for seg in np.unique(df.segment)]
    pool=Pool(4)
    results = pd.concat(pool.map(RC_func, seq))
    #print(pool.map(RC_func, seq))
    print('got results')
    pool.close()
    pool.join()
    results.to_csv('C:/Users/atousaz/Box/Loop/Multiple_result.csv')
    #results.to_csv('C:/Users/atousaz/Desktop/Interstates_RC'+'test.csv')


end = time.time()
print((end - start)*0.000277778)




