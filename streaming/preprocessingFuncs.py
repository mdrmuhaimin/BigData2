# PART OF THIS IS USED TO OBTAIN THE TRIAL TYPE,SUBTYPE,NUMBER,ETC THAT ARE USED FOR THE DIFFERENT GROUP_BY
# OPERATIONS. YOU MIGHT NEED SOME OF THIS. OR NONE, DEPENDING ON HOW THE STREAMING IS BEING DONE.


# FOR TRIAL TYPE AND SUBJECT : 
files_path = '../data/IMU Dataset'
files = glob.glob(files_path)
folders = ['ADLs', 'Falls', 'Near_Falls']

df = pd.DataFrame()

for i in range(1,11):
    print('Subject: '+str(i))
    for folder in folders:
        print('-- Trial Type: '+str(folder))
        path = files_path+'/sub'+str(i)+'/'+str(folder)+'/*.xlsx'
        files = glob.glob(path)

        # create a list of dataframes, one for each file, and assign a column with the name of the file
        dfs_list = [pd.read_excel(fp).assign(FileName = os.path.basename(fp)) for fp in files]

        df_temp = pd.DataFrame()
        df_temp = pd.concat(dfs_list)
        df_temp['subject'] = i
        df_temp['trial_type'] = folder
        
        df = df.append(df_temp)


# FOR TRIAL NUMS,SUBTYPES, AND DATE/TIME STUFF : 

# 'Trial Type' is not ordinal, so we will need dummy variables

# extraction of the trial number from the file name
# Not Ordinal: necessary dummy variables
df['trial_num_original'] = df['FileName'].apply(lambda x: x.replace('.xlsx','').replace('trial','').split('_')[-1]).astype(int)

# extraction of the trial subtype (slip, trip, pick object from ground, etc) from the file name
# Necessary dummy variables
df['trial_subtype'] = df['FileName'].apply(lambda x: x.split('_')[1])

# column with the real date and time of the measurement
# according to the README.txt, the Time column is the number of microseconds from 01/01/1970
initial = datetime.datetime(1970,1,1,0,0,0)
df['time_datetime'] = df.Time.apply(lambda x: (initial + datetime.timedelta(microseconds=x)))
# creating the column 'Time_datetime' in a pandas datetime format
df['time_datetime'] = df.time_datetime.apply(lambda t: pd.datetime(t.year,t.month,t.day,t.hour,t.minute,t.second,t.microsecond))

df['time_seconds'] = (df.Time - df.min_time)/1000000

# THIS IS FOR THE TRIAL NUMS
# Trial_num original is sometimes numbers like 2 4 6. This part makes it so that the trial_num
# column is always between 1 and 3
list_trial_nums = []
for type in list(df.trial_type.unique()):
    for subtype in list(df[df.trial_type == type].trial_subtype.unique()):
        for subject in list(df[(df.trial_type == type) & (df.trial_subtype == subtype)].subject.unique()):
#             print(subject)
            trials = list(df[(df.trial_type == type) & (df.trial_subtype == subtype) & (df.subject == subject)].trial_num_original.unique())
            trials = sorted(trials)
            trials = [int(x) for x in trials]
            
            trials_correct = [i+1 for i in range(len(trials))]
            trial_dict = dict()
            for i in range(3):
                trial_dict[trials[i]] = trials_correct[i]
                
            list_trial_nums.append([type, subtype, subject, trial_dict])

list_dfs = []
for index, item in enumerate(list_trial_nums):
    df_adjusts = df[(df.trial_type == item[0])&
                    (df.trial_subtype == item[1])&
                    (df.subject == item[2])                    
                    ]
    df_adjusts['trial_num'] = df_adjusts.trial_num_original.apply(lambda x: item[3][x])
    
    list_dfs.append(df_adjusts)


# THIS IS USED TO CALCULATE THE RESULTANT OF EACH COLUMN. 
# The formula for the resultant is sqrt((metricX^2) + (metricY^2) + (metricZ^2))
# where metric is either acceleration,magnetic field or velocity.
## TAKES LONG TIME TO RUN
for body_part in list(meta.body_location.unique())[1:]:
    print(body_part)
    for measure in list(meta.measure.unique())[1:]:
        col_name = str(body_part + ' resultant ' + measure)
        col_to_calculate = list(meta[(meta.body_location == body_part) & (meta.measure == measure)].index)
        df_new[col_name] = df_new.apply(lambda x: np.sqrt(x[col_to_calculate[0]]**2 + x[col_to_calculate[1]]**2 + x[col_to_calculate[2]]**2), axis=1)


# THIS PART CREATES 0.5 SECOND WINDOWS AND CALCULATES THE MEAN AND VARIANCE OF EACH COLUMN

# group in intervals of 0.5 seconds, calculating the mean
df2_window_mean = df2.groupby(['subject','trial_type','trial_subtype','trial_num','trial_num_original',pd.Grouper(key='time_datetime', freq='500000us')]).mean()
df2_window_mean = df2_window_mean.reset_index()

# renaming the acceleration measurement columns, including a '_mean' in the end
for col in df2.columns.values :
    if ('Acceleration' in col) or ('Velocity' in col) or ('Magnetic' in col) or ('resultant' in col) :
        df2_window_mean.rename(columns={col: str(col+'_mean')}, inplace=True)

# group in intervals of 2 seconds, calculating the variance

df2_window_variance = df2.groupby(['subject','trial_type','trial_subtype','trial_num','trial_num_original',pd.Grouper(key='time_datetime', freq='500000us')]).var()
df2_window_variance = df2_window_variance.reset_index()

# renaming the acceleration measurement columns, including a '_variance' in the end
for col in df2.columns.values :
    if ('Acceleration' in col) or ('Velocity' in col) or ('Magnetic' in col) or ('resultant' in col) :
        df2_window_variance.rename(columns={col: str(col+'_var')}, inplace=True)

# final dataframe, with all accelerometer columns (means and variances)
df2_all_windows = pd.merge(df2_window_mean, df2_window_variance,on=['subject', 'trial_type', 'trial_subtype', 'trial_num','trial_num_original','time_datetime'])

# This dataframe will be used in case we decide to try different preprocessing steps
df2_all_windows = df2_all_windows.dropna(axis=0, how='any')



# THIS CODE IS USED WHEN LABELING THE TARGETS. BASICALLY EVERYTHING IS 0 (NOT-FALL) EXCEPT
# THE WINDOW AROUND THE MEAN RESULTANT ACCELERATION MEAN OF THE WAIST. 

# # Creating the window for each subject,trialtype, subtype and number and combine them all into one single dataframe
df2List = []
for sub in dfResWindows['subject'].unique() :
    for trialtype in ['Falls'] :
        for subtype in dfResWindows['trial_subtype'].unique() :
            for num in dfResWindows['trial_num'].unique() :
                aux1 = dfResWindows[(dfResWindows['subject'] == sub) & 
                                         (dfResWindows['trial_type'] == trialtype) & 
                                         (dfResWindows['trial_subtype'] == subtype) & 
                                         (dfResWindows['trial_num'] == num)]
                if (aux1.shape[0] > 0) :
                    peak_index = aux1['waist resultant acceleration_mean'].idxmax()
#                     time_peak = aux1.iloc[peak_index,aux1.columns.get_loc('time_seconds')]
#                     aux2 = aux1[(aux1.index < peak_index+2) & (aux1.index > peak_index-2)]
                    for i in range(peak_index-4,peak_index+4) : # Add the target 1 to the window
                        aux1.set_value(i, 'target', 1)
                    df2List.append(aux1)



# I doubt I missed anything, but just in case, I'll leave the instructions on what you'd need to do
# provided you have a trained model in the server and are receiving raw streamed data from the
# original files. 

# If you just want to predict what you're receiving, then you most likely won't need the whole
# trial type,subtype,etc part at the beginning. What you'd need to do for preprocessing
# is the following :

# 1. Store rows of data until you have 0.5seconds worth of rows

# 2. Drop magnetic field and velocity columns as well as any columns related to
# any body parts that aren't waist, r.thigh and l.thigh. (Or just keep waist,
# the difference when using the thighs isn't big)

# 3. Calculate the resultant of each of the rows you have from the 0.5 second window
# you collected and store it as another column

# 4. Calculate mean and variance of every column from that 0.5 second window including
# the resultants you just calculated

# 5. Drop any columns related to time, subject, trial type, etc.  The columns used for training
# are acceleration mean and variance of each axis for each body part used and the resultant accelerations
# means and variances for each body part used (either just waist, or waist+thighs)

# If I missed anything, check the mlfull and data_consolidation jupyter notebooks