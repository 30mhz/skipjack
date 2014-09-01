{
    "name": "mytable",
    "schema": {
        "hashkey": {
            "name": "email_address",
            "type": "STRING"
        },
        "hashkey": {
            "name": "name",
            "type": "STRING"
        }
    },
    "global_indexes": [
		{
	        "name": "name-index",
	        "hashkey": "name",
	        "throughput": {
	            "read": 1,
	            "write": 1
	        }
    	}
	],
    "throughput": {
        "read": 1,
        "write": 1
    }
}
