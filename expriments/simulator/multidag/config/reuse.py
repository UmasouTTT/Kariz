import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


    
def reuse_calculate(fname):
    df = pd.read_csv(fname, sep=',',skipinitialspace=True, header = None)
    #print(df)
    df_t = df.T
    df_t.columns = ['filename']
    print('filename',fname, 'Trace size:',len(df_t),"Number of unique files in the trace:",len(df_t.filename.unique()))

    reuse_time = {}
    last_access = {}
    counter = 0
    for ind in df_t.index:
        size, name = 1, df_t['filename'][ind]
        if name not in last_access.keys():
            counter += size
            last_access[name] = counter
        else:
            diff = counter - last_access[name]
            counter += size
            last_access[name] = counter
            if diff not in reuse_time.keys():
                reuse_time[diff] = 1
            else:
                reuse_time[diff] += 1
    print("Reuse Distance Distribution by Bytes (each file has same 1 bytes)")
    print('reuse distance', '#number of file')
    df = pd.DataFrame(list(reuse_time.items()),columns = ['files','dist']) 
    print(reuse_time)
    dist=[]
    files=[]
    for key in sorted(reuse_time):
        print ("%s, %s" % (key, reuse_time[key]))
        dist.append(key)
        files.append(reuse_time[key])
        #print ("reuse distance %s: #number of file %s" % (key, reuse_time[key]))
    fig = plt.figure()
    sns.barplot(x = 'files', y = 'dist',data=df,  palette = 'hls')
    #ax = fig.add_axesdd([0,0,1,1])
    #ax.bar(dist,files)
    yint = []
    locs, labels = plt.yticks()
    for each in locs:
        yint.append(int(each))
    plt.yticks(yint)
    plt.ylabel('Number of files')
    plt.xlabel('Reuse Distance (in files)')
    plt.show()
    plt.savefig(fname+'.png')

if __name__ == '__main__':

    liste = [0,20,40,60,80,100] 
    for i in liste:
        fname = 'synthetic_worload_mdmc_'+str(i)+'p.g'
        reuse_calculate(fname) 
