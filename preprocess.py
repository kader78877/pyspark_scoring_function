import os
import pyspark.sql.functions as f
import copy
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,lit


def create_df_sinistres_ref(df_source):
    """
    Creation de la master table df sinistre, scope: sinistres de prevoyance collective en gestion interne
    """
    ctr_loxam = [
        "11007096","21007096","61017963","11007098","11007097","21017006","21017007","61017006","21007097",
        "61017005","21007098","21007099","61017007","11007099","21017005"
    ]
    ctr_ues_generali = ["21009383","11001991","203165","21000946","203164","21000945"]
    
    # 1. Creation du peimetre
    cond_statut = (col("codstados")=='CLOS')
    cond_dt_partition = (col("dt_partition")==202012) # Année de survenance du sinistre
    cond_gest = (col("typdoss")!="DEL") # Gestion différente de délégataire
    cond_contrat = (~col("numconuti").isin(ctr_loxam+ctr_ues_generali)) # Exclusion des contrats LOXAM et UES GENERALI FRANCE
    df_source = df_source.where(cond_statut & cond_gest & cond_contrat)
    
    # 2. Preprocessing
    df_source = df_source.withColumn('nbJourCoureg',f.datediff('fincoureg','debcoureg'))
    
    list_grp_by = [
        "numutipre","numconuti","codgarele","typdoss","datsursin",
        'nom_assure','prenom_assure','date_naiss_assure','codris'
    ]
    agg_grp_by = [
        f.min("debcoureg").alias("debcoureg"),
        f.min("fincoureg").alias("fincoureg"),
        f.sum('mntbruind').alias("sumPrest"), # Montant brut indemnisation
        f.sum('nbJourCoureg').alias('nbJourInd'),
        f.sum("mntnetind").alias("mntnetind") # Montant net indemnisation
    ]
    df_sinistres = df_source.groupBy(*list_grp_by).agg(*agg_grp_by).distinct()
    
    # TODO @Abdel - Priorité 1: rajout codegarele incap/inval/ deces
    # rajout arret maladie ou accident travail
    
    # UPDATE (20/07/2021) TODO @Abdel : dans ajout_nat_risque (fonction ci-dessous)
    return df_sinistres


def ajout_nat_risque(df_sinistres,df_ref_garantie) :
    # 1. nat_risque : 'INC' / 'INV' / 'DC'
    # 2. sin_evemt : 'arret_maladie' / 'accident_travail' / 'autre'

    # df_ref_garantie preprocessing
    df_ref_garantie = df_ref_garantie.withColumnRenamed('lb_gstat','nat_risque')
    
    cond_when = when((col('codgarele') == 'ITTA') | (col('codgarele') == 'ITT_AT') | (col('codgarele') == 'ITTA_HPR') | (col('codgarele') == 'ITTA_PRO')\
                     ,lit('accident_travail'))\
                .when((col('codgarele') == 'ITTM') | (col('codgarele') == 'ITTM_HPR') | (col('codgarele') == 'ITTM_PRO')\
                      ,lit('arret_maladie'))\
                .otherwise(lit('autre'))
    
    df_ref_garantie = df_ref_garantie.withColumn('sin_evemt',cond_when)
    df_ref_garantie = df_ref_garantie.select('codgarele','nat_risque','sin_evemt','libgarele')
    
    # df_sinistre preprocessing
    df_sinistres = df_sinistres.join(df_ref_garantie,'codgarele','left')
    return df_sinistres

def ajout_sinistres_prestations(df_sinistres, df_dossier_sin, df_contrat, df_prestations, df_socinfo):
    """
    Ajout des informations de prestation dans la table de référence par dossier sinistre
    """
    df_sin_contrats = df_dossier_sin.join(df_contrat, ['nucontra', 'dt_partition'], 'inner')

    df_sin_contrat_presta = df_sin_contrats.join(
        df_prestations.withColumnRenamed('dtsurv', 'datsurpresta'),
        ['nudossier', 'nucontra', 'cdappli', 'cdcie'], 
        'inner'
    )

    df_sin_contrat_presta_socio = df_sin_contrat_presta.join(df_socinfo, 'cdsociete', 'left')
    df_prestations_join = (
        df_sin_contrat_presta_socio
        .withColumn("delai_effcon_sinistre", f.datediff("dtsurv", "dteffcon"))
        .withColumn("delai_modcon_sinistre", f.datediff("dtsurv", "dtdermod"))
        .withColumnRenamed("nudossieruti", "numutipre")
        .withColumnRenamed("cdmodges", "mode_gestion")
        .withColumnRenamed("dteffcon", "date_effet")
        .withColumnRenamed("dtsurv", "datsursin")
        .withColumnRenamed("nucontra", "numconuti")
        .groupBy(["numconuti", "numutipre", "mode_gestion", "date_effet", "dtdermod", "datsursin", "datsurpresta"])
        .agg(
            f.mean("delai_effcon_sinistre").alias('delai_effcon_sinistre'), 
            f.mean("delai_modcon_sinistre").alias('delai_modcon_sinistre'),
#             f.sum('mtprestation').alias('mtprestation')
        )
    )
    
    # TODO @Abdel - Priorité 2: delai_effcon_sinistre et delai_modcon_sinistre colonnes a integrer dans scoring fraude
    # parametrer seuil pour alerte (1 mois)
    
    # UPDATE (20/07/2021) TODO @Abdel : fait dans crit_effcon et crit_modcon
    
    df_sinistres_presta = df_sinistres.join(df_prestations_join, on=['numconuti', 'numutipre', 'datsursin'], how="left")
    
    return df_sinistres_presta


def ajout_nb_adhesions(df_sinistres, df_adhesionassure):
    """
    Ajout du nombre d'assurés par contrat
    """
    nb_adherants_contrat = (
        df_adhesionassure
        .withColumnRenamed('nucontra', 'numconuti')
        .groupBy('numconuti')
        .agg(f.count('idassureagi').alias('nb_assure'))
    )
    # TODO @Abdel - Priorité 2: nb_assure par contrat pour scoring de fraude
    # parametrer seuil (< 30) 
    
    # UPDATE (20/07/2021) TODO @Abdel : fait dans crit_nb_assure
    
    df_sinistres_adhesion = df_sinistres.join(nb_adherants_contrat, 'numconuti', 'left')
    return df_sinistres_adhesion


def ajout_insee(df_sinistres, df_portefeuille, df_insee):
    """
    ATTENTION: il peut y avoir plusieurs codes siren par contrat (plusieurs filiales): scope different du scope sinistre
    """
    df_sin_portefeuille = df_sinistres_prestation.join(
        df_portefeuille
        .withColumn('siren', col('siret').substr(1,9))
        .withColumnRenamed('num_contrat', 'numconuti')
        .select('numconuti', 'siren', 'cd_postal').distinct(), 
        'numconuti', 
        'left' 
    )
    
    df_sin_insee = df_sin_portefeuille.join(
        df_insee.select('siren', 'apet700', 'dcret').distinct(), 'siren', how='left' # ATTENTION duplicates
    )
    
    # TODO (non prio): groupBy collect_set() en ligne pour avoir une ligne par siren
    
    return df_sin_insee