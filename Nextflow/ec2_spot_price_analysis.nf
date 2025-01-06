
params.ec2_django_scripts_directory = 'badassdatascience/django/ec2_spot_price_tracker/scripts'

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

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/adfuller_test.py' > adfuller_test_result.csv
    """
}

workflow {
    updateEc2SpotPriceDatabase | resample | test_stationarity
}
