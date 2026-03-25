terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "project-8406299b-edd5-44e0-92b" 
  region  = "us-central1"
}


resource "google_storage_bucket" "taxi_data_lake" {
  name          = "dezoomcamp_hw4_dor" 
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "ny_taxi_dataset" {
  dataset_id = "nytaxi"
  location   = "US"
}