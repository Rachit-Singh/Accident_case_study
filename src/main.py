from utils import SparkUtilities
from pyspark.sql.functions import col, dense_rank, row_number
from pyspark.sql.window import Window
import os

if __name__ == "__main__" :
    # create the class instance
    spark_utils = SparkUtilities(config_path= os.path.join(os.path.dirname(__file__),"config.json"))

    # read the config file
    CONFIG = spark_utils.get_config()

    # read all the files into dataframes
    charges_use = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['charges_use']))
    damages_use = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['damages_use']))
    endorse_use = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['endorse_use']))
    restrict_use = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['restrict_use']))
    primary_person_use = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['primary_person_use']), do_caching=True)
    units_use_distinct = spark_utils.read_df_from_file(filepath= os.path.join(os.path.dirname(__file__), '..', CONFIG['input_file_paths']['units_use_distinct']), do_caching=True, get_distinct=True)


    ### A1
    a1 = (primary_person_use
          .filter((col("PRSN_INJRY_SEV_ID") == 'KILLED') & (col("PRSN_GNDR_ID") == 'MALE'))
          .groupBy('crash_id')
          .count()
          .filter("count > 2")
          .count()
          )

    ### A2
    a2 = (units_use_distinct
        .filter("VEH_BODY_STYL_ID in ('POLICE MOTORCYCLE','MOTORCYCLE')")
        .select(['crash_id', 'vin'])
        .distinct()
        .count()
        )

    ### A3
    a3 = (primary_person_use.filter((col('DEATH_CNT') > 0) & (col('PRSN_TYPE_ID') == 'DRIVER') & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
    .join(units_use_distinct, ['crash_id', 'unit_nbr'])
    .groupBy('VEH_MAKE_ID')
    .count()
    .orderBy(col('count').desc())
    .limit(5)
    )

    ### A4
    hit_n_run_units = units_use_distinct.filter("VEH_HNR_FL = 'Y'")

    a4 = (primary_person_use.filter("DRVR_LIC_CLS_ID not in ('UNLICENSED', 'UNKNOWN')")
    .join(hit_n_run_units, ['crash_id', 'unit_nbr'])
    .select([primary_person_use['crash_id'], 'VIN'])
    .distinct()
    .count()
    )


    ### A5
    crashes_with_no_females = (primary_person_use.filter(~(col("DRVR_LIC_STATE_ID").isin('NA', 'Unknown')))
    .groupBy("crash_id")   
    .pivot("PRSN_GNDR_ID")  # this creates a dataframe with crash_id as kinda index and gender as columns
    .count()
    .filter("FEMALE is NULL")  # get all the crashes where no females were there
    )

    a5 = (primary_person_use.filter(~(col("DRVR_LIC_STATE_ID").isin('NA', 'Unknown')))   # remove all UNKNOWN and NA states
    .join(crashes_with_no_females, "crash_id")
    .select(["DRVR_LIC_STATE_ID", "CRASH_ID"])
    .distinct()
    .groupby("DRVR_LIC_STATE_ID")
    .count()
    .orderBy(col("count").desc())
    .limit(1)
    .select("DRVR_LIC_STATE_ID")
    )

    ### A6
    Window_Spec  = Window.orderBy(col("sum(inj_death)").desc())

    a6 = (units_use_distinct.filter("VEH_MAKE_ID not in ('UNKNOWN', 'NA')")
    .withColumn("inj_death", col("TOT_INJRY_CNT") + col("DEATH_CNT"))
    .groupBy('VEH_MAKE_ID')
    .sum("inj_death")
    .withColumn("rank",dense_rank().over(Window_Spec))
    .filter("rank in (3,4,5)")
    .withColumnRenamed("sum(inj_death)", "COUNT")
    .select(["VEH_MAKE_ID", "COUNT"])
    .orderBy("rank")
    )


    ### A7
    Window_Spec  = Window.partitionBy(["VEH_BODY_STYL_ID"]).orderBy(col("count").desc())

    a7 = (units_use_distinct.filter("VEH_BODY_STYL_ID not in ('UNKNOWN', 'NA')")
    .join(primary_person_use, ['crash_id', 'unit_nbr'])
    .groupBy(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"])
    .count()
    .withColumn("sno",row_number().over(Window_Spec))
    .filter("sno = 1")
    .select(["VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"])
    )

    ### A8
    a8 = (units_use_distinct.filter(col("VEH_BODY_STYL_ID").isin('SPORT UTILITY VEHICLE', 'PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR', 'POLICE CAR/TRUCK'))  # getting all cars
    .filter((col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_P1_ID").like("%ALCOHOL%")))   # getting all alcohol contributing crashes
    .join(primary_person_use, ['crash_id', 'unit_nbr'])
    .filter("DRVR_ZIP is not NULL")
    .select(units_use_distinct['crash_id'], "DRVR_ZIP")
    .distinct()
    .groupby("DRVR_ZIP")
    .count()
    .orderBy(col("count").desc())
    .limit(5)
    )

    ### A9
    a9 = (units_use_distinct.filter(
        (col("VEH_DMAG_SCL_1_ID").isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")) |    # got damage of more than 4
        (col("VEH_DMAG_SCL_2_ID").isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")) 
        )
        .filter(col("FIN_RESP_TYPE_ID").like("%INSURANCE%"))   # this means he showed some insurance as proof
        .filter(col("VEH_BODY_STYL_ID").isin('SPORT UTILITY VEHICLE', 'PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR', 'POLICE CAR/TRUCK'))  # this means it is a car
        .join(damages_use, on="crash_id", how="leftanti")    # this gets us all the crashes that did not do any damage to property
        .select("CRASH_ID")   
        .distinct()
        .count()  # get all distinct crash_ids
        )


    ### A10
    top_10_used_colors = (units_use_distinct.filter("VIN is not NULL")
                            .select(["VIN", "CRASH_ID", "VEH_COLOR_ID"])
                            .distinct()
                            .groupBy("VEH_COLOR_ID")
                            .count()
                            .orderBy(col("count").desc())
                            .limit(10)
                            )

    top_25_states = (units_use_distinct.filter("VEH_LIC_STATE_ID not in ('NA', 'UN')")
                        .select(["CRASH_ID", "VEH_LIC_STATE_ID"])
                        .distinct()
                        .groupBy("VEH_LIC_STATE_ID")
                        .count()
                        .orderBy(col("count").desc())
                        .limit(25)
                        )

    speeding_offenses = (charges_use
                        .filter(col("CHARGE").like("%SPEED%"))
                        )

    licensed_drivers_with_speeding_offenses = (primary_person_use.filter("DRVR_LIC_CLS_ID not in ('UNLICENSED', 'UNKNOWN')")
                                            .join(speeding_offenses, on=["crash_id", "unit_nbr", "PRSN_NBR"]))

    a10 = (units_use_distinct.filter((col("VEH_BODY_STYL_ID").isin('SPORT UTILITY VEHICLE', 'PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR', 'POLICE CAR/TRUCK')) & ~(col("VEH_MAKE_ID").isin('UNKNOWN', 'NA')))
            .join(licensed_drivers_with_speeding_offenses, on=["crash_id", "unit_nbr"])
            .join(top_10_used_colors, on="VEH_COLOR_ID")
            .join(top_25_states, on="VEH_LIC_STATE_ID")
            .select("VEH_MAKE_ID", "CRASH_ID", "VIN")
            .distinct()
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
            )

    output_lst = [
        {
            "analysis_id": str(idx+1), 
            "analysis_text": ques,
            "analysis_output": output
        }
        for idx, ques, output in zip(range(len(CONFIG['analysis'])), CONFIG['analysis'], [a1,a2,a3,a4,a5,a6,a7,a8,a9,a10])
    ]

    # unpersist dataframes
    units_use_distinct.unpersist()
    primary_person_use.unpersist()

    spark_utils.write_file_from_dfs(output_lst, location=os.path.join(os.path.dirname(__file__), '..', CONFIG['output_file_path']))