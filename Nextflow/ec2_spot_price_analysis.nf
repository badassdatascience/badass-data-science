
params.ec2_django_scripts_directory = 'badassdatascience/django/ec2_spot_price_tracker/scripts'

params.bds_home = '/home/emily/Desktop/projects/test/badassdatascience'


process updateEc2SpotPriceDatabase {
    output:
    path 'x'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/daily_main.py' > x
    """
}

process resample {
    input:
    path x

    output:
    path 'resampled.parquet'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/resample.py'
    """
}

process test_stationarity {
    input:
    path 'resampled.parquet'

    output:
    path 'adfuller_test_result.csv'
    path 'resampled.parquet'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/adfuller_test.py' > adfuller_test_result.csv
    """
}

process find_best_arima {
    input:
    path 'adfuller_test_result.csv'
    path 'resampled.parquet'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/run_notebook.py'
    """
}

workflow {
    updateEc2SpotPriceDatabase | resample | test_stationarity | find_best_arima
}
