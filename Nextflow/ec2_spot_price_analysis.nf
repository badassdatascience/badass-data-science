
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
    path 'resampled.csv'
    path 'resampled.parquet'
    path 'mean_daily_spot_prices.png'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/resample.py'
    """
}

process test_stationarity {
    input:
    path 'resampled.parquet'

    output:
    path 'adfuller_result.txt'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/adfuller_test.py'
    """
}

workflow {
    updateEc2SpotPriceDatabase | resample
    test_stationarity
}
