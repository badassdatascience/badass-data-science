
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
    path 'y'

    script:
    """
    python3 '${params.bds_home}/${params.ec2_django_scripts_directory}/resample.py' > y
    """
}

workflow {
    updateEc2SpotPriceDatabase | resample
}
