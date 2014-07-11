import os
import sys
import time
import simplejson as json

import jsonschema
import argparse

from decimal import Decimal

from boto.dynamodb2.fields import *
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
import boto.dynamodb2


## {{{ http://code.activestate.com/recipes/577058/ (r2)
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.
    
    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).
    
    The "answer" return value is one of "yes" or "no".
    """
    valid = {"yes":"yes",    "y":"yes",    "ye":"yes",
             "no":"no",        "n":"no"}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)
    
    while 1:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return default
        elif choice in valid.keys():
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")
## end of http://code.activestate.com/recipes/577058/ }}}

class Meister:
    types = "boto.dynamodb2.types.{0}"
    __location__ = os.path.realpath( os.path.join(os.getcwd(), os.path.dirname(__file__)))
    schema = json.load(open(os.path.join(__location__, 'schema.json')))
    
    def __init__(self, access_key_id=None, secret_access_key=None, region=None):
        self.region = region
        self.connection = boto.dynamodb2.connect_to_region(region,
                                                           aws_access_key_id=access_key_id,
                                                           aws_secret_access_key=secret_access_key)
        
        if 'specification_file' in args and not args['specification_file'] is None:
            self.table_spec = json.load(open(os.path.join(Meister.__location__,
                                                        args['specification_file'])))
    
    # this will take care of its own exceptions (a bit raw, but nothing we can't handle)
    def _validate(self):
        jsonschema.validate(self.table_spec, Meister.schema)
    
    def check(self, args):
        self._validate()
        
        try:
            table = Table(args['origin_table'], connection=self.connection)
            table.describe()
        except Exception as e:
            return "table {0} could not be found in {1}".format(args['origin_table'], self.region)
        
        # first check hash- and rangekey (optional)
        hk_type = self.table_spec['schema']['hashkey']['type']
        hk_type = eval(Meister.types.format(hk_type))
        hk_name = self.table_spec['schema']['hashkey']['name']
        if table.schema[0].name != hk_name or table.schema[0].data_type != hk_type:
            return "{0}'s hashkey should be {1}, and type should be {2}".format(table.table_name,
                                                    self.table_spec['schema']['hashkey']['name'],
                                                    self.table_spec['schema']['hashkey']['type'])
        # beware, rangekey is optional
        if 'rangekey' in self.table_spec['schema']:
            rk_type = self.table_spec['schema']['rangekey']['type']
            rk_type = eval(Meister.types.format(rk_type))
            rk_name = self.table_spec['schema']['rangekey']['name']
            
            if len(table.schema) < 2 or \
                table.schema[1].name != rk_name or \
                table.schema[1].data_type != rk_type:
                return "{0}'s rangekey should be {1}, of type {2}".format(table.table_name,
                                                        self.table_spec['schema']['rangekey']['name'],
                                                        self.table_spec['schema']['rangekey']['type'])
        elif len(table.schema) >= 2:
            return "{0} has a rangekey, specification not".format(table.table_name)

        # indexes
        indexes = {}
        for index in table.indexes:
            indexes[index.name] = index

        if 'indexes' in self.table_spec:
            for index in self.table_spec['indexes']:
                if index['name'] in indexes:
                    i_type = eval(Meister.types.format(index['attribute']['type']))
                    i_name = index['attribute']['name']
                    if i_name != indexes[index['name']].parts[1].name or \
                            i_type != indexes[index['name']].parts[1].data_type:
                        return "index {0} in table {1} has type or name mismatch".format(index['name'],
                                                                                    table.table_name)
                    del(indexes[index['name']])
                else:
                    return "index {0} in table {1} not found in specification".format(index['name'],
                                                                                table.table_name)
            if len(indexes) > 0:
                return "table {0} has indexes not found in specification".format(table.table_name)

        # and last, indexes
        global_indexes = {}
        for index in table.global_indexes:
            global_indexes[index.name] = index

        if 'global_indexes' in self.table_spec:
            for index in self.table_spec['global_indexes']:
                if index['name'] in global_indexes:
                    i_type = eval(Meister.types.format(index['attribute']['type']))
                    i_name = index['attribute']['name']
                    if i_name != global_indexes[index['name']].parts[1].name or \
                            i_type != global_indexes[index['name']].parts[1].data_type:
                        return "index {0} in table {1} has type or name mismatch".format(index['name'],
                                                                                    table.table_name)
                    del(global_indexes[index['name']])
                else:
                    return "index {0} in table {1} not found in specification".format(index['name'],
                                                                                table.table_name)
            if len(global_indexes) > 0:
                return "table {0} has indexes not found in specification".format(table.table_name)
            
        return "no mismatch or errors found"
    
    def create(self, args):
        self._validate()
        
        if 'destination_table' not in args:
            return "you must specify a destination table for this command"
        else:
            if args['destination_table'] in self.connection.list_tables()['TableNames']:
                return "table {0} already exists".format(args['destination_table'])
        
        schema = []
        hk_name = self.table_spec['schema']['hashkey']['name']
        hk_type = eval(Meister.types.format(self.table_spec['schema']['hashkey']['type']))
        hashkey = HashKey(hk_name, hk_type)
        schema.append(hashkey)
        if 'rangekey' in self.table_spec['schema']:
            rk_name = self.table_spec['schema']['rangekey']['name']
            rk_type = eval(Meister.types.format(self.table_spec['schema']['rangekey']['type']))
            schema.append(RangeKey(rk_name, rk_type))
        
        throughput = self.table_spec['throughput'] if 'throughput' in self.table_spec else None
        indexes = []
        if 'indexes' in self.table_spec:
            for index in self.table_spec['indexes']:
                rk_name = index['attribute']['name']
                rk_type = eval(Meister.types.format(index['attribute']['type']))
                rangekey = RangeKey(rk_name, rk_type)
                if 'fields' not in index:
                    indexes.append(AllIndex(index['name'], parts=[hashkey, rangekey]))
                elif len(index['fields']) <= 0:
                    indexes.append(KeysOnlyIndex(index['name'], parts=[hashkey, rangekey]))
                else:
                    indexes.append(IncludeIndex(index['name'], parts=[hashkey, rangekey],
                                                includes=[field for field in index['fields']]))
        global_indexes = []
        if 'global_indexes' in self.table_spec:
            for index in self.table_spec['global_indexes']:
                parts = []
                parts.append(HashKey(index['hashkey']))
                if 'rangekey' in index:
                    parts.append(RangeKey(index['rangekey']))
                
                index_throughput = index['throughput'] if 'throughput' in index else None
                if 'fields' not in index:
                    global_indexes.append(GlobalAllIndex(index['name'],
                                                         parts=parts,
                                                         throughput=index_throughput))
                elif len(index['fields']) <= 0:
                    global_indexes.append(GlobalKeysOnlyIndex(index['name'],
                                                              parts=parts,
                                                              throughput=index_throughput))
                else:
                    includes = [field for field in index['fields']]
                    global_indexes.append(GlobalIncludeIndex(index['name'],
                                                             parts=parts,
                                                             includes=includes,
                                                             throughput=index_throughput))
        table = Table.create(args['destination_table'],
                             schema=schema,
                             throughput=throughput,
                             indexes=indexes,
                             global_indexes=global_indexes,
                             connection=self.connection)
        
        return table.describe()['Table']['TableStatus']
    
    def copy(self, args):
        # lets get the origin table
        try:
            origin = Table(args['origin_table'], connection=self.connection)
            origin.describe()
        except Exception as e:
            return "table {0} could not be found in {1}".format(args['origin_table'], self.region)
        
        # now, get the destination_table
        destination = Table(args['destination_table'], connection=self.connection)
        
        print "copying items from {0} to {1}".format(origin.table_name, destination.table_name)
        for item in origin.scan():
            destination.put_item(dict(item))
    
    def _mold(self, item):
        item = dict(item)
        
        for key in item.keys():
            if type(item[key]) is list:
                item[key] = set(item[key])

        if 'indexes' in self.table_spec:
            for index in self.table_spec['indexes']:
                attribute = index['attribute']
                if attribute['type'] == "NUMBER":
                    if 'translation' in attribute:
                        if item[attribute['name']] in attribute['translation']:
                            item[attribute['name']] = Decimal(attribute['translation'][item[attribute['name']]])
                        elif 'default' in attribute:
                            item[attribute['name']] = Decimal(attribute['default'])
                    else:
                        try:
                            item[attribute['name']] = Decimal(item[attribute['name']])
                        except:
                            if 'default' in attribute:
                                item[attribute['name']] = Decimal(item[attribute['default']])
                elif attribute['type'] == "STRING":
                    if 'translation' in attribute:
                        if item[attribute['name']] in attribute['translation']:
                            item[attribute['name']] = "{0}".format(attribute['translation'][item[attribute['name']]])
                        elif 'default' in attribute:
                            item[attribute['name']] = "{0}".format(attribute['default'])
                    else:
                        try:
                            item[attribute['name']] = "{0}".format(item[attribute['name']])
                        except:
                            if 'default' in attribute:
                                item[attribute['name']] = "{0}".format(attribute['default'])
        if 'global_indexes' in self.table_spec:
            for index in self.table_spec['global_indexes']:
                attribute = index['attribute']
                if attribute['type'] == "NUMBER":
                    if 'translation' in attribute:
                        if item[attribute['name']] in attribute['translation']:
                            item[attribute['name']] = Decimal(attribute['translation'][item[attribute['name']]])
                        elif 'default' in attribute:
                            item[attribute['name']] = Decimal(attribute['default'])
                    else:
                        try:
                            item[attribute['name']] = Decimal(item[attribute['name']])
                        except:
                            if 'default' in attribute:
                                item[attribute['name']] = Decimal(item[attribute['default']])
                elif attribute['type'] == "STRING":
                    if 'translation' in attribute:
                        if item[attribute['name']] in attribute['translation']:
                            item[attribute['name']] = "{0}".format(attribute['translation'][item[attribute['name']]])
                        elif 'default' in attribute:
                            item[attribute['name']] = "{0}".format(attribute['default'])
                    else:
                        try:
                            item[attribute['name']] = "{0}".format(item[attribute['name']])
                        except:
                            if 'default' in attribute:
                                item[attribute['name']] = "{0}".format(attribute['default'])
        if 'transformations' in self.table_spec:
            for index in self.table_spec['transformations']:
                if index['type'] == "NUMBER":
                    item[index['name']] = Decimal(item[index['name']])
                elif index['type'] == "NUMBER_SET":
                    fields = set()
                    try:
                        for field in json.loads(item[index['name']]):
                            fields.add(Decimal(field))
                    except TypeError:
                        for field in item[index['name']]:
                            fields.add(Decimal(field))
                    
                    item[index['name']] = fields
                elif index['type'] == "STRING":
                    item[index['name']] = "{0}".format(item[index['name']])
                elif index['type'] == "STRING_SET":
                    fields = set()
                    try:
                        for field in json.loads(item[index['name']]):
                            fields.add("{0}".format(field))
                    except TypeError:
                        for field in item[index['name']]:
                            fields.add("{0}".format(field))
                    item[index['name']] = fields
                elif index['type'] == "OBSOLETE":
                    if index['name'] in item:
                        del item[index['name']]
        
        return item
    
    def migrate(self, args):
        # lets get the origin table
        try:
            origin = Table(args['origin_table'], connection=self.connection)
            origin.describe()
        except Exception as e:
            return "table {0} could not be found in {1}".format(args['origin_table'], self.region)
        
        # now, create the destination_table (using create)
        destination = Table(args['destination_table'], connection=self.connection)
        print "creating table {0}".format(destination.table_name)
        if self.create(args) != 'CREATING':
            print "    table {0} exists".format(destination.table_name)
        else:
            while destination.describe()['Table']['TableStatus'] != 'ACTIVE':
                print "        ..."
                time.sleep(5)
            print "    table {0} created".format(destination.table_name)
        
        print "copying items from {0} to {1}".format(origin.table_name, destination.table_name)
        for item in origin.scan():
            # be sure to mold the fields into their proper shapes
            item = self._mold(item)
            destination.put_item(item, overwrite=True)
    
    def archive(self, args):
        # lets get the origin table
        try:
            origin = Table(args['origin_table'], connection=self.connection)
            origin.describe()
        except Exception as e:
            return "table {0} could not be found in {1}".format(args['origin_table'], self.region)
        
        for item in origin.scan():
            item = dict(item)
            for key in item.keys():
                if type(item[key]) is set:
                    item[key] = list(item[key])
            
            print json.dumps(item, use_decimal=True)
    
    def restore(self, args):
        # create the destination_table (using create)
        destination = Table(args['destination_table'], connection=self.connection)
        print "creating table {0}".format(destination.table_name)
        if self.create(args) != 'CREATING':
            print "    table {0} exists".format(destination.table_name)
        else:
            while destination.describe()['Table']['TableStatus'] != 'ACTIVE':
                print "        ..."
                time.sleep(5)
            print "    table {0} created".format(destination.table_name)
        
        print "reading items from stdin to {0}".format(destination.table_name)
        for line in sys.stdin:
            # be sure to mold the fields into their proper shapes
            item = self._mold(json.loads(line))
            destination.put_item(item, overwrite=True)

parser = argparse.ArgumentParser()
parser.add_argument('command',
                    choices=['check', 'create', 'copy', 'archive', 'restore', 'migrate'],
                    default='check',
                    help='Action to perform')
parser.add_argument('-o', '--origin-table', help='Name of the table')
parser.add_argument('-d', '--destination-table', required=False,
                    help='Name of the destination table (migrate)')
parser.add_argument('-f', '--specification-file',
                    help='File with the json table specification')
parser.add_argument('-a', '--access-key-id', help='AWS Access Key ID')
parser.add_argument('-s', '--secret-access-key', help='AWS Secret Access Key')
parser.add_argument('-r', '--region', default='us-east-1', help='AWS Region')

args = vars(parser.parse_args())

meister = Meister(access_key_id=args['access_key_id'],
                secret_access_key=args['secret_access_key'],
                region=args['region'])
print getattr(meister, args['command'])(args)
