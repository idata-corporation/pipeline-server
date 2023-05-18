curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_object_store' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_object_store_dq' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_object_store_iceberg' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_object_store_iceberg_keys' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_object_store_transform' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_redshift' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_redshift_filter_col' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_redshift_filter_col_merge' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_snowflake' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_snowflake_filter_col' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=stock_price_snowflake_filter_col_merge' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/stock_price.20170102.dataset.csv"'

curl --location --request POST 'http://pipeline.poc.idatadev.cloud/dataset/upload?dataset=parkinglotimage' \
--header 'x-api-key: 1847626a-5b46-4d43-827c-25f323d9201b' \
--form 'file=@"./files/parkinglotimage.dataset.jpeg"'