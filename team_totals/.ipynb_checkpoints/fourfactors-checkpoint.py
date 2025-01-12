import pandas as pd
import requests
def four_factors_data(df,frame2,year,ps=False,vs=False):
    df2=frame2.reset_index(drop=True)

    if ps == False:
        avg = pd.read_csv('team_averages.csv')
    else:
        avg = pd.read_csv('team_averages_ps.csv')

    avg['fgoreb%']=avg['total_fgoreb'].astype(int)/(avg['total_fgoreb'].astype(int)+ avg['total_opp_fgdreb'].astype(int))

    avg.rename(columns={'oreb%':'ORB%', 'ortg':'ORtg', 'season':'Season'},inplace=True)
    #avg = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/master/avg_shooting.csv')
  
    avg=avg[avg.year==year]
    season=str(year-1)+'-'+str(year)[-2:]
 
   


    

    df.fillna(0,inplace=True)
    df2.fillna(0,inplace=True)


    df2['opp_DREB']=df2['DefRebounds']
    df2=df2[['TeamId','opp_DREB']]
    df=df.merge(df2,on='TeamId')


    
    for col in df.columns:
        if 'reb' in col.lower():
            pass

    df['FG2_miss']=df['FG2A']-df['FG2M']

    df['FG3_miss']=df['FG3A']-df['FG3M']
    df['fg_miss']=df['FG3_miss'] +df['FG2_miss']

 

    
    df['Season']=season

    #aorb,aortg,fgorb=avg[['ORB%','ORtg','fgoreb%']].iloc[0]
    aorb,aortg=avg[['ORB%','ORtg']].iloc[0]
    
    df['OREB']=df['OffRebounds']

    df['DREB']=df['DefRebounds']



    df['FTM']=df['FtPoints']

    
    #df['DREB']=df['DefRebounds']





    df['avg_ortg']=aortg

   
    df['avg_orb%']=aorb
    #df['avg_forb%']=fgorb
    
    #df['avg_ortg']/=100
    #df['avg_orb%']/=100
    df['FGA']=df['FG2A']-df['FG3A']
    df['FGM']= df['FG2M']-df['FG3M']
    df['FG2_miss']=df['FG2A']-df['FG2M']
    
    df['FG3_miss']=df['FG3A']-df['FG3M']
    df['FG2_points']=df['FG2M']*2
    df['FG3_points']=df['FG3M']*3

    df['2shooting_factor']=df['FG2_points']-(df['FG2M']*df['avg_ortg']) - (1-df['avg_orb%'])*df['FG2_miss']*df['avg_ortg']
    
    df['3shooting_factor']=df['FG3_points']-(df['FG3M']*df['avg_ortg']) - (1-df['avg_orb%'])*df['FG3_miss']*df['avg_ortg']
    
    df['turnover_factor']=-(df['Turnovers']*df['avg_ortg'])
    df['oreb_factor']=((1-df['avg_orb%'])*df['OREB']-df['avg_orb%']*df['opp_DREB'])*df['avg_ortg']
    df['ft_factor']=df['FTM']-.4*df['FTA']*df['avg_ortg'] + .06*(df['FTA']-df['FTM'])*df['avg_ortg']

    columns = ['ft_factor', 'oreb_factor', 'turnover_factor', '2shooting_factor', '3shooting_factor']
    df['spread']=df['ft_factor']+df['oreb_factor']+df['turnover_factor']+df['2shooting_factor']+df['3shooting_factor']
    df.drop(columns=['opp_DREB'],inplace=True)


 
    return df