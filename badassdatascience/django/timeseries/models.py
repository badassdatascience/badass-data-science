from django.db import models

# Create your models here.

from django.db import models

class Interval(models.Model):
    name = models.CharField(max_length=200, db_index=True, unique=True)
    symbol = models.CharField(max_length=200, db_index=True, unique=True)
    pub_date = models.DateTimeField('date published')
    class Meta:
        db_table = 'timeseries_interval'
        
class PriceType(models.Model):
    name = models.CharField(max_length=200, db_index=True, unique=True)
    pub_date = models.DateTimeField('date published')
    class Meta:
        db_table = 'timeseries_pricetype'

class Instrument(models.Model):
    name = models.CharField(max_length=200, db_index=True, unique=True)
    us_margin_requirement = models.FloatField()
    pub_date = models.DateTimeField('date published')
    class Meta:
        db_table = 'timeseries_instrument'

class Timestamp(models.Model):
    timestamp = models.BigIntegerField(db_index=True)
    pub_date = models.DateTimeField('date published')
    class Meta:
        db_table = 'timeseries_timestamp'

class Volume(models.Model):
    volume = models.IntegerField(db_index=True)
    pub_date = models.DateTimeField('date published')
    class Meta:
        db_table = 'timeseries_volume'
    
class Candlestick(models.Model):
    o = models.FloatField()
    l = models.FloatField()
    h = models.FloatField()
    c = models.FloatField()
    volume = models.ForeignKey(Volume, on_delete=models.CASCADE, db_index=True)
    timestamp = models.ForeignKey(Timestamp, on_delete=models.CASCADE, db_index=True)
    instrument = models.ForeignKey(Instrument, on_delete=models.CASCADE, db_index=True)
    price_type = models.ForeignKey(PriceType, on_delete=models.CASCADE, db_index=True)
    interval = models.ForeignKey(Interval, on_delete=models.CASCADE, db_index=True)
    pub_date = models.DateTimeField('date published')

    class Meta:
        db_table = 'timeseries_candlestick'

        indexes = [
            models.Index(
                name = 'candlestick_index',
                fields = ['timestamp', 'instrument', 'price_type', 'interval'],
            )
        ]

        constraints = [
            models.UniqueConstraint(
                name = 'unique_candlestick_parts',
                fields = [
                    'timestamp', 'instrument', 'price_type', 'interval',
                ],
            )
        ]
