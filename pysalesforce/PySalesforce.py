import copy
import datetime
import json
import os
import random



class DBStream:

    def __init__(self, instance_name, client_id):
        self.instance_name = instance_name
        self.instance_type_prefix = ""
        self.ssh_init_port = ""
        self.client_id = client_id
        self.ssh_tunnel = None
        self.dbstream_instance_id = 'df-' + datetime.datetime.now().strftime('%s') + '-' + str(
            random.randint(1000, 9999))

