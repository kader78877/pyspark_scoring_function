from pyspark.sql.functions import *
from pyspark.sql.types import *
from IPython.display import Image
from matplotlib import pyplot as plt
from datetime import datetime  
from datetime import timedelta  
import pandas as pd
import seaborn as sns
from math import ceil,floor
from math import fabs
import time
import datetime
import copy


def scoring(df,params,params_poids_score) :
    def udf_contrat(list_of_sin) :
        score = 0
        copy_l_sin = list_of_sin
        copy_l_sin = []
        crit_contrat = 0
        inf = ''
        if list_of_sin != None :

            
            # reprocess la table
            for ref in list_of_sin :
                new_reference = ref[0]
                new_date = datetime.datetime(int(ref[3]),int(ref[2]),int(ref[1]))
                new_garantie = ref[4]
                new_numconuti = ref[5]
                new_nom_assure = ref[6]
                new_prenom_assure = ref[7]
                new_date_naiss_assure = ref[8]            
                new_ref_sin  = {'reference' : new_reference,\
                               'date' : new_date,\
                               'garantie' : new_garantie,\
                               'numconuti':new_numconuti,\
                               'nom_assure':new_nom_assure,\
                               'prenom_assure':new_prenom_assure,\
                               'date_naiss_assure':new_date_naiss_assure}
                
                copy_l_sin.append(new_ref_sin)
            
            # dossier en cours
            ref_dossier_en_cours = copy_l_sin[0]

            # OK dossiers en double
            while ref_dossier_en_cours in copy_l_sin:
                copy_l_sin.remove(ref_dossier_en_cours)

            # OK analyser les dossiers précédent le sinistre
            to_remove = []
            for ref in copy_l_sin:
                if ref_dossier_en_cours['date'] < ref['date']:
                    to_remove.append(ref)

            # OK remove les dossiers après le sinistre
            for i in to_remove :
                copy_l_sin.remove(i)
                
             
            # Analyser s'il existe plusieurs contrats par assuré :
            to_remove = []
            for ref in copy_l_sin :
                if ref['numconuti'] == ref_dossier_en_cours['numconuti'] and ref['nom_assure'] == ref_dossier_en_cours['nom_assure'] and \
                    ref['prenom_assure'] == ref_dossier_en_cours['prenom_assure'] and ref['date_naiss_assure'] == ref_dossier_en_cours["date_naiss_assure"] :
                    to_remove.append(ref)
            
            # Remove the correct information
            for i in to_remove :
                copy_l_sin.remove(i)
            
            # Identify duplicate numconuti
            duplicate = []
            to_remove = []
            for ref in copy_l_sin :
                if ref['numconuti'] not in duplicate :
                    duplicate.append(ref['numconuti'])
                else:
                    to_remove.append(ref)
                    
            # Remove duplicate contracts
            for i in to_remove :
                copy_l_sin.remove(i)
                
            if len(copy_l_sin) > 0 :
                inf = inf + 'Score + 1 : Plusieurs contrat pour l assuré concerné' + '\n'
            
            for ref in copy_l_sin :
                date_format = ref['date'].strftime('%d-%m-%Y')
                inf = inf + '    Contrat : ' + str(ref['numconuti']) + ' | Rattaché au sinistre : ' +  str(ref['reference']) + ' - Garantie : ' + str(ref['garantie']) + ' - Le : ' + str(date_format) +  '\n'                
            inf = inf +'\n'
            
        return [crit_contrat,inf]
            
    
    def udf_periodicite(list_of_sin) :
        score =0
        copy_l_sin = list_of_sin
        copy_l_sin = []
        crit_period = 0
        crit_period_1_4 = False
        inf = ''
        if list_of_sin != None :
            
            # reprocess la table
            for ref in list_of_sin :
                new_reference = ref[0]
                new_date = datetime.datetime(int(ref[3]),int(ref[2]),int(ref[1]))
                new_garantie = ref[4]
                
                new_ref_sin  = {'reference' : new_reference,\
                               'date' : new_date,\
                               'garantie' : new_garantie}
                
                copy_l_sin.append(new_ref_sin)

            # dossier en cours
            ref_dossier_en_cours = copy_l_sin[0]

            # dossiers en double
            while ref_dossier_en_cours in copy_l_sin:
                copy_l_sin.remove(ref_dossier_en_cours)

            # analyser les dossiers précédent le sinistre
            to_remove = []
            for ref in copy_l_sin:
                if ref_dossier_en_cours['date'] < ref['date']:
                    to_remove.append(ref)

            # remove les dossiers après le sinistre
            for i in to_remove :
                copy_l_sin.remove(i)

            # analyser avec les critères de fraude sur la périodicité
            crit_period_4 = 0
            inf = ''
            l_period_4 = []




            for i,ref in enumerate(copy_l_sin) :

                if (fabs((ref['date'] - ref_dossier_en_cours['date']).days) -1)%365 -1 <= params['delai_periodicite'] : 
                    
                    # d_2 >= d_1
                    if (fabs((ref['date'] - ref_dossier_en_cours['date']).days) -1)%365 -1 <= params['delai_periodicite'] and \
                            (fabs((ref['date'] - ref_dossier_en_cours['date']).days) -1)>=365  :
                        l_period_4.append(ref)
                else :
                    # d_1 > d_2
                    if fabs((fabs((ref['date'] - ref_dossier_en_cours['date']).days) -1)%365 -1 - 365) <= params['delai_periodicite'] and \
                            (fabs((ref['date'] - ref_dossier_en_cours['date']).days) -1)>=365  :
                        l_period_4.append(ref)
                    
                    
            # sort datetime in descending order
            # 1. ascending order
            list_date = [i['date'] for i in l_period_4]
            list_date = sorted(list_date)
            
            # 2. descending order
            list_date = list_date[::-1]
            
            l_period = []
            
            for i in list_date :                
                for j in l_period_4 :
                    if i == j['date'] and j not in l_period : 
                        l_period.append(j)
                        break
                                        
            l_period_4 = l_period
            
            for i,ref in enumerate(l_period_4) :
                date_format = ref['date'].strftime('%d-%m-%Y')
                if i==0 :
                    inf = inf + '\n'
                    crit_period_4 = len(l_period_4)
                    crit_period_1_4 = True
                    inf = inf + 'Score + ' + str(params_poids_score['crit_periodicite_p_4']) + ' : Déclaration sinistre proche' + '\n'
                    inf = inf + '           année antérieure / postérieure'  + '\n'
                    inf = inf + '    Sinistre : ' + str(ref['reference']) + ' | ' +  str(ref['garantie']) + ' | ' + str(date_format) + '\n'
                else :
                    inf = inf + '    Sinistre : ' + str(ref['reference']) + ' | ' +  str(ref['garantie']) + ' | ' + str(date_format) + '\n'
        
                    
            inf = inf + '\n'
            crit_period = crit_period_4 

        return [crit_period,inf,crit_period_1_4]
    
    df_bis = df
    score = 0
    
    detail_general = [lit('Information Générales:'),lit('\n'),lit('\n'),lit('Numéro de sinistre : '),col("numutipre"),lit('\n'),\
                     lit('Numéro de contrat : '),col("numconuti"),lit('\n'),\
                     lit('Date de survenance : '),date_format(col("datsursin"), "dd-MM-YYYY"),lit('\n'),\
                     lit('Type de sinistre : '),col("libgarele"),lit('\n'),\
                     lit('Nom assure : '),col("nom_assure"),lit('\n'),\
                     lit('Prenom assure : '),col("prenom_assure"),lit('\n'),\
                     lit('\n')]
    
    df_bis = df_bis.withColumn('detail_general',concat(*detail_general))
    
    # periodicité
    df_bis = df_bis.withColumn('day_dtsurv',dayofmonth('datsursin'))
    df_bis = df_bis.withColumn('month_dtsurv',month('datsursin'))
    df_bis = df_bis.withColumn('year_dtsurv',year('datsursin'))
    
    df_bis = df_bis.withColumn('numutipre_datsursin',array([df_bis.numutipre,df_bis.day_dtsurv,df_bis.month_dtsurv,df_bis.year_dtsurv,df_bis.codgarele]))
    
    df_periodicite = df_bis.groupBy('nom_assure','prenom_assure','date_naiss_assure').agg(collect_set('numutipre_datsursin').alias('sin_assure_'))
    df_periodicite = df_periodicite.withColumn("nb_sin_assure", size("sin_assure_"))
    df_bis = df_bis.join(df_periodicite,['nom_assure','prenom_assure','date_naiss_assure'],'left')
    
    df_bis = df_bis.withColumn('numutipre_datsursin_init',array([df_bis.numutipre_datsursin]))
    df_bis = df_bis.withColumn("sin_assure", concat(col("numutipre_datsursin_init"), col("sin_assure_")))
    

    periodicite = udf(lambda l: udf_periodicite(l),ArrayType(StringType()))
    df_bis = df_bis.withColumn('crit_period_',periodicite(col('sin_assure')))
    
    df_bis = df_bis.withColumn('crit_period',col('crit_period_')[0])
    df_bis = df_bis.withColumn('crit_period_detail',col('crit_period_')[1])
    df_bis = df_bis.withColumn('crit_period_periodicite',col('crit_period_')[2])
    
    df_bis = df_bis.drop('crit_period_','sin_assure')
                         
    # multiplicité contrat
    df_bis = df_bis.withColumn('numutipre_datsursin_numconuti',\
                               array([df_bis.numutipre,df_bis.day_dtsurv,df_bis.month_dtsurv,df_bis.year_dtsurv,df_bis.codgarele,\
                                      df_bis.numconuti,df_bis.nom_assure,df_bis.prenom_assure,df_bis.date_naiss_assure]))
    
    df_fraud_2 = df_bis.groupBy('nom_assure','prenom_assure','date_naiss_assure').agg(collect_set('numutipre_datsursin_numconuti').alias('fraud_2'))
    df_bis = df_bis.join(df_fraud_2,['nom_assure','prenom_assure','date_naiss_assure'],'left')
    df_bis = df_bis.withColumn('numutipre_datsursin_numconuti_init',array([df_bis.numutipre_datsursin_numconuti]))
    df_bis = df_bis.withColumn("fraud_contrat", concat(col("numutipre_datsursin_numconuti_init"), col("fraud_2")))
    
    # nom de la udf
    fraud_contrat_udf = udf(lambda l: udf_contrat(l),ArrayType(StringType()))
    df_bis = df_bis.withColumn('crit_contrat_',fraud_contrat_udf(col('fraud_contrat')))
    
    df_bis = df_bis.withColumn('crit_contrat',       col('crit_contrat_')[0])
    df_bis = df_bis.withColumn('crit_contrat_detail',col('crit_contrat_')[1])
    
    df_mul_ctr = df_bis.groupBy('nom_assure','prenom_assure','date_naiss_assure').agg(collect_set('numconuti').alias('ctr_assure'))
    df_mul_ctr = df_mul_ctr.withColumn("nb_ctr_assure", size("ctr_assure"))
    
    df_bis = df_bis.join(df_mul_ctr,['nom_assure','prenom_assure','date_naiss_assure'],'left')
    df_bis = df_bis.withColumn('crit_multiplicite_contrat',col('nb_ctr_assure'))
    
    # Délai entre date effet de contrat et datsursin
    
    df_bis = df_bis.withColumn('delai_effcon_sinistre',datediff("date_effet","datsursin"))
    df_bis = df_bis.withColumn('delai_modcon_sinistre',datediff("dtdermod","datsursin"))
    
    
    cond_dteffcon = when(col('delai_effcon_sinistre') < params['delai_eff_con'],lit(params_poids_score['crit_delai_eff_con'])).otherwise(lit(0))
    cond_dtmodcon = when(col('delai_modcon_sinistre') < params['delai_mod_con'],lit(params_poids_score['crit_delai_modcon'])).otherwise(lit(0))
    
    df_bis = df_bis.withColumn('crit_effcon',cond_dteffcon)
    df_bis = df_bis.withColumn('crit_modcon',cond_dtmodcon)
    
    # Nombre d'assure
    
    cond_dteffcon = when(col('nb_assure') < params['nb_assure']\
                     ,lit(params_poids_score['crit_nb_assure']))\
                    .otherwise(lit(0))  
    df_bis = df_bis.withColumn('crit_nb_assure',cond_dteffcon)
    
    # score final
    df_bis = df_bis.withColumn('score',col('crit_multiplicite_contrat') + col('crit_period') + col('crit_contrat'))
    
    df_bis = df_bis.withColumn('score' + '_detail',concat(col('detail_general'),col('crit_contrat'+ '_detail') , col('crit_period'+ '_detail')))
    
    df_bis = df_bis.drop('ctr_assure','day_dtsurv','month_dtsurv','year_dtsurv','numutipre_datsursin','sin_assure_','numutipre_datsursin_init'\
                        'detail_general','crit_multiplicite_contrat'+ '_detail','crit_contrat'+ '_detail','crit_period'+ '_detail',\
                         'numutipre_datsursin_numconuti','numutipre_datsursin_numconuti_init','fraud_contrat','fraud_2','crit_contrat_','nb_ctr_assure',\
                         'numutipre_datsursin_init','nb_sin_assure','crit_contrat','detail_general')
    
    
    return df_bis