
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

workflow {
    updateEc2SpotPriceDatabase | resample
}
