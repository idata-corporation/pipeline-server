echo GET /version
curl --location --request GET 'http://pipeline.poc.idatadev.cloud/version' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' | json_pp

echo POST /dataset - stock_price_snowflake
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
   "destination" : {
      "database" : {
         "dbName" : "DEMO_DB",
         "schema" : "PUBLIC",
         "snowflake" : {
            "warehouse" : "COMPUTE_WH"
         },
         "table" : "stock_price"
      }
   },
   "name" : "stock_price_snowflake",
   "source" : {
      "fileAttributes" : {
         "csvAttributes" : {
            "delimiter" : ",",
            "encoding" : "UTF-8",
            "header" : true
         }
      },
      "schemaProperties" : {
         "dbName" : "testdb",
         "fields" : [
            {
               "name" : "symbol",
               "type" : "string"
            },
            {
               "name" : "date",
               "type" : "string"
            },
            {
               "name" : "open",
               "type" : "double"
            },
            {
               "name" : "high",
               "type" : "double"
            },
            {
               "name" : "low",
               "type" : "double"
            },
            {
               "name" : "close",
               "type" : "double"
            },
            {
               "name" : "volume",
               "type" : "int"
            },
            {
               "name" : "adj_close",
               "type" : "double"
            }
         ]
      }
   }
}
'

echo POST /dataset - stock_price_snowflake_filter_col
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_snowflake_filter_col",
	"destination": {
		"database": {
			"dbName": "DEMO_DB",
			"schema": "PUBLIC",
			"snowflake": {
				"warehouse": "COMPUTE_WH"
			},
			"table": "stock_price_filter_col"
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_snowflake_filter_col_merge
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_snowflake_filter_col_merge",
	"dataQuality": {
		"validateFileHeader": false
	},
	"transformation": {
	    "deduplicate": true
	},
	"destination": {
		"database": {
			"dbName": "DEMO_DB",
			"deleteBeforeWrite": false,
			"schema": "PUBLIC",
			"snowflake": {
				"keyFields": [
					"symbol",
					"date"
				],
				"warehouse": "COMPUTE_WH"
			},
			"table": "stock_price_filter_col_merge"
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_redshift
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_redshift",
	"destination": {
		"database": {
			"dbName": "dev",
            "schema": "public",
			"redshift": {
				"useJsonOptions": false
			},
			"table": "stock_price"
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
        "schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_redshift_filter_col
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_redshift_filter_col",
	"destination": {
		"database": {
			"dbName": "dev",
            "schema": "public",
			"redshift": {
				"useJsonOptions": false
			},
			"table": "stock_price_filter_col"
		},
         "schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
        "schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_redshift_filter_col_merge
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_redshift_filter_col_merge",
	"transformation": {
	    "deduplicate": true
	},
	"destination": {
		"database": {
			"dbName": "dev",
            "schema": "public",
			"redshift": {
                "keyFields": ["symbol", "date"],
				"useJsonOptions": false
			},
			"table": "stock_price_filter_col_merge"
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_object_store
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_object_store",
	"destination": {
		"objectStore": {
			"deleteBeforeWrite": true,
			"partitionBy": [
				"date"
			],
			"prefixKey": "yahoo/finance",
			"writeToTemporaryLocation": false
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_object_store_dq
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_object_store_dq",
	"dataQuality": {
		"columnRules": [{
				"columnName": "symbol",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^[a-zA-Z]+$"
			},
			{
				"columnName": "open",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^(?:0|[1-9][0-9]*)\\.[0-9]+$"
			},
			{
				"columnName": "high",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^(?:0|[1-9][0-9]*)\\.[0-9]+$"
			},
			{
				"columnName": "low",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^(?:0|[1-9][0-9]*)\\.[0-9]+$"
			},
			{
				"columnName": "close",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^(?:0|[1-9][0-9]*)\\.[0-9]+$"
			},
			{
				"columnName": "volume",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^[0-9]+$"
			},
			{
				"columnName": "adj_close",
				"function": "regex",
				"onFailureIsError": true,
				"parameter": "^(?:0|[1-9][0-9]*)\\.[0-9]+$"
			}
		],
		"rowRules": [{
			"function": "javascript",
			"onFailureIsError": false,
			"parameters": [
				"stock_price_data_quality.js"
			]
		}],
		"validateFileHeader": true
	},
	"destination": {
		"objectStore": {
			"deleteBeforeWrite": true,
			"fileFormat": "parquet",
			"manageGlueTableManually": false,
			"partitionBy": [
				"date"
			],
			"prefixKey": "yahoo/finance",
			"useIceberg": false,
			"writeToTemporaryLocation": false
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_object_store_iceberg
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_object_store_iceberg",
	"dataQuality": {
		"validateFileHeader": false
	},
	"destination": {
		"objectStore": {
			"deleteBeforeWrite": false,
			"fileFormat": "parquet",
			"manageGlueTableManually": false,
			"partitionBy": [
				"date"
			],
			"prefixKey": "yahoo/finance",
			"useIceberg": true,
			"writeToTemporaryLocation": true
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - stock_price_object_store_iceberg_keys
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
  "name": "stock_price_object_store_iceberg_keys",
  "dataQuality": {
   "validateFileHeader": true
  },
  "transformation": {
    "deduplicate": true
  },
  "destination": {
   "objectStore": {
    "deleteBeforeWrite": false,
    "fileFormat": "parquet",
    "keyFields": [
     "symbol",
     "date"
    ],
    "manageGlueTableManually": false,
    "partitionBy": [
     "date"
    ],
    "prefixKey": "yahoo/finance",
    "useIceberg": true,
    "writeToTemporaryLocation": true
   }
  },
  "source": {
   "fileAttributes": {
    "csvAttributes": {
     "delimiter": ",",
     "encoding": "UTF-8",
     "header": true
    }
   },
   "schemaProperties": {
    "dbName": "testdb",
    "fields": [
     {
      "name": "symbol",
      "type": "string"
     },
     {
      "name": "date",
      "type": "string"
     },
     {
      "name": "open",
      "type": "double"
     },
     {
      "name": "high",
      "type": "double"
     },
     {
      "name": "low",
      "type": "double"
     },
     {
      "name": "close",
      "type": "double"
     },
     {
      "name": "volume",
      "type": "int"
     },
     {
      "name": "adj_close",
      "type": "double"
     }
    ]
   }
  }
 }'


echo POST /dataset - stock_price_object_store_transform
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "stock_price_object_store_transform",
	"transformation": {
        "trimColumnWhitespace": true,
        "deduplicate": true,
		"rowFunctions": [{
			"function": "javascript",
			"parameters": [
				"stock_price_transformation.js"
			]
		}]
	},
	"destination": {
		"objectStore": {
			"deleteBeforeWrite": true,
			"fileFormat": "parquet",
			"manageGlueTableManually": false,
			"partitionBy": [
				"date"
			],
			"prefixKey": "yahoo/finance",
			"useIceberg": false,
			"writeToTemporaryLocation": true
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				},
				{
					"name": "year",
					"type": "string"
				},
				{
					"name": "mynewcolumn",
					"type": "double"
				}
			]
		}
	},
	"source": {
		"fileAttributes": {
			"csvAttributes": {
				"delimiter": ",",
				"encoding": "UTF-8",
				"header": true
			}
		},
		"schemaProperties": {
			"dbName": "testdb",
			"fields": [{
					"name": "symbol",
					"type": "string"
				},
				{
					"name": "date",
					"type": "string"
				},
				{
					"name": "open",
					"type": "double"
				},
				{
					"name": "high",
					"type": "double"
				},
				{
					"name": "low",
					"type": "double"
				},
				{
					"name": "close",
					"type": "double"
				},
				{
					"name": "volume",
					"type": "int"
				},
				{
					"name": "adj_close",
					"type": "double"
				}
			]
		}
	}
}'

echo POST /dataset - parkinglotimage
curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--header 'Content-Type: application/json' \
--data-raw '{
	"name": "parkinglotimage",
	"destination": {
		"objectStore": {
			"deleteBeforeWrite": true,
			"manageGlueTableManually": false,
			"prefixKey": "satellite/images",
			"useIceberg": false,
			"writeToTemporaryLocation": true
		}
	},
	"source": {
		"fileAttributes": {
			"unstructuredAttributes": {
				"fileExtension": "jpeg",
				"preserveFilename": true
			}
		}
	}
}'

echo GET /dataset/status
curl --location --request GET 'http://pipeline.poc.idatadev.cloud/dataset/status' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' | json_pp

echo GET /datasets
curl --location --request GET 'http://pipeline.poc.idatadev.cloud/datasets' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' | json_pp

echo GET /dataset?dataset=stock_price_snowflake
curl --location --request GET 'http://pipeline.poc.idatadev.cloud/dataset?dataset=stock_price_snowflake' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' | json_pp