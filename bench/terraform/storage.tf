# Configuration for data storage bucket.

variable "s3_data_bucket" {
  description = "Bucket which holds test and result data"
}



### Bootstrap Resources ###

variable "solanum_version" {
  description = "Version of Solanum daemon to install"
  default = "3.2.0"
}

resource "aws_s3_bucket_object" "install_solanum" {
  bucket = "${var.s3_data_bucket}"
  key    = "bootstrap/install-solanum"
  source = "../bootstrap/install-solanum"
  etag   = "${md5(file("../bootstrap/install-solanum"))}"
}

resource "aws_s3_bucket_object" "install_profiler" {
  bucket = "${var.s3_data_bucket}"
  key    = "bootstrap/install-profiler"
  source = "../bootstrap/install-profiler"
  etag   = "${md5(file("../bootstrap/install-profiler"))}"
}

resource "aws_s3_bucket_object" "riemann_profiler_jar" {
  bucket = "${var.s3_data_bucket}"
  key    = "bootstrap/riemann-jvm-profiler.jar"
  source = "../bootstrap/riemann-jvm-profiler.jar"
  etag   = "${md5(file("../bootstrap/riemann-jvm-profiler.jar"))}"
}
