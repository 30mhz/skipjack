Skipjack
========
_As any decent Skipjack, we also need to migrate. Even in the land of the DynamoDBs..._

Working with DynamoDB is not very straightforward. It is said to be schema-less, but in fact a better description would be schema-inferred. The hashkey, rangekey and local secondary keys all impose a type on their fields. Especially early in development this can be confusing, and annoying to work with: meet Skipjack.

So, we need a way to specify our DynamoDB tables. As there is no official specification (like SQL) we came up with a basic, table specification language (json). The TSL's syntax has been defined with http://json-schema.org/. When working with table specifications they are validated with https://github.com/Julian/jsonschema.

An example TSL

	{
	    "name": "mytable",
	    "schema": {
	        "hashkey": {
	            "name": "myhashkey",
	            "type": "STRING"
	        }
	    }
	}

This is a specification for a very simple DynamoDB table. Look at the examples, and the syntax definition for inspiration. You should be able to express any DynamoDB table structure possible. In addition we can also specify transformations, or in other words, we can enforce types. (Casting simple types, and transforming from json to _SET types.)

Use
---

	$ python skipjack.py -h
	usage: skipjack.py [-h] [-o ORIGIN_TABLE] [-d DESTINATION_TABLE]
	                   [-f SPECIFICATION_FILE] [-a ACCESS_KEY_ID]
	                   [-s SECRET_ACCESS_KEY] [-r REGION]
	                   {check,create,copy,archive,restore,migrate}

	positional arguments:
	  {check,create,copy,archive,restore,migrate}
	                        Action to perform

	optional arguments:
	  -h, --help            show this help message and exit
	  -o ORIGIN_TABLE, --origin-table ORIGIN_TABLE
	                        Name of the table
	  -d DESTINATION_TABLE, --destination-table DESTINATION_TABLE
	                        Name of the destination table (migrate)
	  -f SPECIFICATION_FILE, --specification-file SPECIFICATION_FILE
	                        File with the json table specification
	  -a ACCESS_KEY_ID, --access-key-id ACCESS_KEY_ID
	                        AWS Access Key ID
	  -s SECRET_ACCESS_KEY, --secret-access-key SECRET_ACCESS_KEY
	                        AWS Secret Access Key
	  -r REGION, --region REGION
	                        AWS Region

The rest is up to your imagination, inquisitiveness and perseverance...

Checking
--------

One cool thing we can do with our 'type system' is checking if an existing table complies. Given a .json table specification, and an existing table we can do the following

	$ python skipjack.py check -o jurg.checks.30mhz.com \
			-f checks.30mhz.json \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1

This checks if the table `jurg.checks.30mhz.com` in `eu-west-1` follows the specification from the file `checks.30mhz.json`.

Creating
--------

In the same fashion we can create an empty table from a specification

	$ python skipjack.py create -d jurg.checks.30mhz.com \
			-f checks.30mhz.json \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1

Copy
----

Sometimes (for testing?) a verbatim copy of a table is what you want

	$ python skipjack.py copy -o production.checks.30mhz.com \
			-d jurg.checks.30mhz.com \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1

Migrate
-------

But, in other cases, the table needs to changed (other indexes, field transformations) and the data migrated

	$ python skipjack.py migrate -o production.checks.30mhz.com \
			-d jurg.checks.30mhz.com \
			-f checks.30mhz.json \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1

Archive & Restore
-----------------

We can also migrate using the archive/restore functionality

	$ python skipjack.py archive -o production.checks.30mhz.com \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1 > archive.json

	$ python skipjack.py restore -d jurg.checks.30mhz.com \
			-f checks.30mhz.json \
			-a your-access-key \
			-s your-secret-key \
			-r eu-west-1 < archive.json
