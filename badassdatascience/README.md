# Python Modules

This directory contains all the Python modules and many of the scripts. Consider it the "top" level.

### How to Use This

In the shell:
```
export BDS_HOME=/home/emily/Desktop/projects/badassdatascience
```

Then in Python:
```
# load system libraries
import sys
import os

# retrieve the $BDS_HOME environment variable's value
application_root_directory = os.environ['BDS_HOME']

# include $BDS_HOME in the Python path
sys.path.append(application_root_directory + '/badassdatascience/django/infrastructure')
```

See the section below that outlines how to additionally include Django access:

### Accessing the Django Modules:

In the shell:
```
export BDS_HOME=/home/emily/Desktop/projects/badassdatascience
```

Then in Python (if you want access to the Django object models):
```
# load system libraries
import sys
import os

# retrieve the $BDS_HOME environment variable's value
application_root_directory = os.environ['BDS_HOME']

# include $BDS_HOME in the Python path
sys.path.append(application_root_directory + '/badassdatascience/django/infrastructure')

# Set another environment value needed by Django
os.environ['DJANGO_SETTINGS_MODULE'] = 'infrastructure.settings'

# activate Django 
import django
django.setup()
```

These steps are contained in [django/boilerplate.py](django/boilerplate.py) if you simply want to import it.





