# Linode Object Storage — S3-compatible data lake storage
# Used for bronze/silver/gold zones via Apache Iceberg + Nessie catalog.
# No separate MinIO or self-hosted S3 needed — Linode Object Storage is
# natively S3-compatible (endpoint: https://{region}-1.linodeobjects.com).

resource "linode_object_storage_bucket" "datalake" {
  region = var.region   # same region as LKE cluster (LGPD data residency)
  label  = "fraud-datalake"
  acl    = "private"
}

resource "linode_object_storage_key" "datalake" {
  label = "fraud-datalake-rw"

  bucket_access {
    bucket_name = linode_object_storage_bucket.datalake.label
    region      = linode_object_storage_bucket.datalake.region
    permissions = "read_write"
  }
}
