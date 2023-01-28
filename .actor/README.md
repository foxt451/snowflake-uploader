# What does Snowflake Uploader do?

Snowflake Uploader lets you upload any dataset to a DB provider [Snowflake](https://www.snowflake.com/en/). Use it for integration between Apify and Snowflake

# How to use Snowflake Uploader?

1. Go to [Snowflake Uploader](https://apify.com/svpetrenko/snowflake-uploader) on Apify
2. Click **Try for free** button
3. Enter scraper Input (see below): the dataset info, connection and transformation options.
4. Click the Start button
5. If the run is finished successfully, you should see data in your Snowflake table.

The power of this scraper will come if you set up webhooks on Apify scrapers that will call this uploader after completion.

# Input

The most important options for this scraper are connection options and datasetId. Other options allow you to manipualte your table schema and transform acquired json rows from the dataset. If you decide to set up a webhook for some scraper, it will by default post the default dataset id, so in this case you may omit it.

It might be convenient to set up a separate task with connection options. Then you'll be able to use a default webhook without modifying the payload at all.
