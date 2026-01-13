provider "google" {
  project = "loyal-weaver-471905-p9"
  region  = "US"
}

resource "google_bigquery_dataset" "raw_layer" {
  dataset_id  = "crypto_raw"
  location    = "US"
  description = "Landing zone for CoinGecko Data"
}

resource "google_bigquery_dataset" "dbt_prod" {
  dataset_id = "dbt_prod"
  location   = "US"
}