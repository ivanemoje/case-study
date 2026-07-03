# ─────────────────────────────────────────────────────────────
# PROCESSED FILES LEDGER
#
# Tracks every zip file processed by SHA256 checksum, not filename.
# Same content under a different name is still recognised as a
# duplicate. A different file using the same name (e.g. provider
# re-sends "sales.zip" with new data) is correctly NOT treated as
# a duplicate, because the checksum differs.
#
# Primary key: checksum_sha256 (string)
# On-demand billing — this table sees one write per file ingested,
# nowhere near enough volume to justify provisioned capacity.
# ─────────────────────────────────────────────────────────────

resource "aws_dynamodb_table" "processed_files" {
  name         = "${var.project}-processed-files"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "checksum_sha256"

  attribute {
    name = "checksum_sha256"
    type = "S"
  }

  tags = { Project = var.project }
}
