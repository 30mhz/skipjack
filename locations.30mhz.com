{
    "name": "mytable",
    "schema": {
        "hashkey": {
            "name": "name",
            "type": "STRING"
        }
    },
    "global_indexes": [
		{
	        "name": "owner-index",
	        "hashkey": "owner",
	        "throughput": {
	            "read": 1,
	            "write": 1
	        }
    	},
		{
	        "name": "access-index",
	        "hashkey": "access",
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
